import type { AnyMessageContent, proto, WAMessage } from "@whiskeysockets/baileys";
import {
  DisconnectReason,
  decryptPollVote,
  getAggregateVotesInPollMessage,
  isJidGroup,
  jidNormalizedUser,
  normalizeMessageContent,
} from "@whiskeysockets/baileys";
import { createInboundDebouncer } from "../../../../src/auto-reply/inbound-debounce.js";
import { formatLocationText } from "../../../../src/channels/location.js";
import { logVerbose, shouldLogVerbose } from "../../../../src/globals.js";
import { recordChannelActivity } from "../../../../src/infra/channel-activity.js";
import { getChildLogger } from "../../../../src/logging/logger.js";
import { createSubsystemLogger } from "../../../../src/logging/subsystem.js";
import { saveMediaBuffer } from "../../../../src/media/store.js";
import { jidToE164, resolveJidToE164 } from "../../../../src/utils.js";
import { createWaSocket, getStatusCode, waitForWaConnection } from "../session.js";
import { checkInboundAccessControl } from "./access-control.js";
import { isRecentInboundMessage } from "./dedupe.js";
import {
  describeReplyContext,
  extractLocationData,
  extractMediaPlaceholder,
  extractMentionedJids,
  extractText,
} from "./extract.js";
import { downloadInboundMedia } from "./media.js";
import { isPollCreationMessage, PollStore } from "./poll-store.js";
import { createWebSendApi } from "./send-api.js";
import type { WebInboundMessage, WebListenerCloseReason } from "./types.js";

export async function monitorWebInbox(options: {
  verbose: boolean;
  accountId: string;
  authDir: string;
  onMessage: (msg: WebInboundMessage) => Promise<void>;
  mediaMaxMb?: number;
  /** Send read receipts for incoming messages (default true). */
  sendReadReceipts?: boolean;
  /** Debounce window (ms) for batching rapid consecutive messages from the same sender (0 to disable). */
  debounceMs?: number;
  /** Optional debounce gating predicate. */
  shouldDebounce?: (msg: WebInboundMessage) => boolean;
}) {
  const inboundLogger = getChildLogger({ module: "web-inbound" });
  const inboundConsoleLog = createSubsystemLogger("gateway/channels/whatsapp").child("inbound");
  const sock = await createWaSocket(false, options.verbose, {
    authDir: options.authDir,
  });
  await waitForWaConnection(sock);
  const connectedAtMs = Date.now();

  let onCloseResolve: ((reason: WebListenerCloseReason) => void) | null = null;
  const onClose = new Promise<WebListenerCloseReason>((resolve) => {
    onCloseResolve = resolve;
  });
  const resolveClose = (reason: WebListenerCloseReason) => {
    if (!onCloseResolve) {
      return;
    }
    const resolver = onCloseResolve;
    onCloseResolve = null;
    resolver(reason);
  };

  try {
    await sock.sendPresenceUpdate("available");
    if (shouldLogVerbose()) {
      logVerbose("Sent global 'available' presence on connect");
    }
  } catch (err) {
    logVerbose(`Failed to send 'available' presence on connect: ${String(err)}`);
  }

  const selfJid = sock.user?.id;
  const selfLid = sock.user?.lid;
  const selfE164 = selfJid ? jidToE164(selfJid) : null;
  const debouncer = createInboundDebouncer<WebInboundMessage>({
    debounceMs: options.debounceMs ?? 0,
    buildKey: (msg) => {
      const senderKey =
        msg.chatType === "group"
          ? (msg.senderJid ?? msg.senderE164 ?? msg.senderName ?? msg.from)
          : msg.from;
      if (!senderKey) {
        return null;
      }
      const conversationKey = msg.chatType === "group" ? msg.chatId : msg.from;
      return `${msg.accountId}:${conversationKey}:${senderKey}`;
    },
    shouldDebounce: options.shouldDebounce,
    onFlush: async (entries) => {
      const last = entries.at(-1);
      if (!last) {
        return;
      }
      if (entries.length === 1) {
        await options.onMessage(last);
        return;
      }
      const mentioned = new Set<string>();
      for (const entry of entries) {
        for (const jid of entry.mentionedJids ?? []) {
          mentioned.add(jid);
        }
      }
      const combinedBody = entries
        .map((entry) => entry.body)
        .filter(Boolean)
        .join("\n");
      const combinedMessage: WebInboundMessage = {
        ...last,
        body: combinedBody,
        mentionedJids: mentioned.size > 0 ? Array.from(mentioned) : undefined,
      };
      await options.onMessage(combinedMessage);
    },
    onError: (err) => {
      inboundLogger.error({ error: String(err) }, "failed handling inbound web message");
      inboundConsoleLog.error(`Failed handling inbound web message: ${String(err)}`);
    },
  });
  const groupMetaCache = new Map<
    string,
    { subject?: string; participants?: string[]; expires: number }
  >();
  const GROUP_META_TTL_MS = 5 * 60 * 1000; // 5 minutes
  const lidLookup = sock.signalRepository?.lidMapping;
  const pollStorePath = `${options.authDir}/poll-store.json`;
  const pollStore = new PollStore(pollStorePath);

  const resolveInboundJid = async (jid: string | null | undefined): Promise<string | null> =>
    resolveJidToE164(jid, { authDir: options.authDir, lidLookup });

  const getGroupMeta = async (jid: string) => {
    const cached = groupMetaCache.get(jid);
    if (cached && cached.expires > Date.now()) {
      return cached;
    }
    try {
      const meta = await sock.groupMetadata(jid);
      const participants =
        (
          await Promise.all(
            meta.participants?.map(async (p) => {
              const mapped = await resolveInboundJid(p.id);
              return mapped ?? p.id;
            }) ?? [],
          )
        ).filter(Boolean) ?? [];
      const entry = {
        subject: meta.subject,
        participants,
        expires: Date.now() + GROUP_META_TTL_MS,
      };
      groupMetaCache.set(jid, entry);
      return entry;
    } catch (err) {
      logVerbose(`Failed to fetch group metadata for ${jid}: ${String(err)}`);
      return { expires: Date.now() + GROUP_META_TTL_MS };
    }
  };

  type NormalizedInboundMessage = {
    id?: string;
    remoteJid: string;
    group: boolean;
    participantJid?: string;
    from: string;
    senderE164: string | null;
    groupSubject?: string;
    groupParticipants?: string[];
    messageTimestampMs?: number;
    access: Awaited<ReturnType<typeof checkInboundAccessControl>>;
  };

  const normalizeInboundMessage = async (
    msg: WAMessage,
  ): Promise<NormalizedInboundMessage | null> => {
    const id = msg.key?.id ?? undefined;
    const remoteJid = msg.key?.remoteJid;
    if (!remoteJid) {
      return null;
    }
    if (remoteJid.endsWith("@status") || remoteJid.endsWith("@broadcast")) {
      return null;
    }

    const group = isJidGroup(remoteJid) === true;
    if (id) {
      const dedupeKey = `${options.accountId}:${remoteJid}:${id}`;
      if (isRecentInboundMessage(dedupeKey)) {
        return null;
      }
    }
    const participantJid = msg.key?.participant ?? undefined;
    const from = group ? remoteJid : await resolveInboundJid(remoteJid);
    if (!from) {
      return null;
    }
    const senderE164 = group
      ? participantJid
        ? await resolveInboundJid(participantJid)
        : null
      : from;

    let groupSubject: string | undefined;
    let groupParticipants: string[] | undefined;
    if (group) {
      const meta = await getGroupMeta(remoteJid);
      groupSubject = meta.subject;
      groupParticipants = meta.participants;
    }
    const messageTimestampMs = msg.messageTimestamp
      ? Number(msg.messageTimestamp) * 1000
      : undefined;

    const access = await checkInboundAccessControl({
      accountId: options.accountId,
      from,
      selfE164,
      senderE164,
      group,
      pushName: msg.pushName ?? undefined,
      isFromMe: Boolean(msg.key?.fromMe),
      messageTimestampMs,
      connectedAtMs,
      sock: { sendMessage: (jid, content) => sock.sendMessage(jid, content) },
      remoteJid,
    });
    if (!access.allowed) {
      return null;
    }

    return {
      id,
      remoteJid,
      group,
      participantJid,
      from,
      senderE164,
      groupSubject,
      groupParticipants,
      messageTimestampMs,
      access,
    };
  };

  const maybeMarkInboundAsRead = async (inbound: NormalizedInboundMessage) => {
    const { id, remoteJid, participantJid, access } = inbound;
    if (id && !access.isSelfChat && options.sendReadReceipts !== false) {
      try {
        await sock.readMessages([{ remoteJid, id, participant: participantJid, fromMe: false }]);
        if (shouldLogVerbose()) {
          const suffix = participantJid ? ` (participant ${participantJid})` : "";
          logVerbose(`Marked message ${id} as read for ${remoteJid}${suffix}`);
        }
      } catch (err) {
        logVerbose(`Failed to mark message ${id} read: ${String(err)}`);
      }
    } else if (id && access.isSelfChat && shouldLogVerbose()) {
      // Self-chat mode: never auto-send read receipts (blue ticks) on behalf of the owner.
      logVerbose(`Self-chat mode: skipping read receipt for ${id}`);
    }
  };

  type EnrichedInboundMessage = {
    body: string;
    location?: ReturnType<typeof extractLocationData>;
    replyContext?: ReturnType<typeof describeReplyContext>;
    mediaPath?: string;
    mediaType?: string;
    mediaFileName?: string;
  };

  const enrichInboundMessage = async (msg: WAMessage): Promise<EnrichedInboundMessage | null> => {
    const location = extractLocationData(msg.message ?? undefined);
    const locationText = location ? formatLocationText(location) : undefined;
    let body = extractText(msg.message ?? undefined);
    if (locationText) {
      body = [body, locationText].filter(Boolean).join("\n").trim();
    }
    if (!body) {
      body = extractMediaPlaceholder(msg.message ?? undefined);
      if (!body) {
        return null;
      }
    }
    const replyContext = describeReplyContext(msg.message as proto.IMessage | undefined);

    let mediaPath: string | undefined;
    let mediaType: string | undefined;
    let mediaFileName: string | undefined;
    try {
      const inboundMedia = await downloadInboundMedia(msg as proto.IWebMessageInfo, sock);
      if (inboundMedia) {
        const maxMb =
          typeof options.mediaMaxMb === "number" && options.mediaMaxMb > 0
            ? options.mediaMaxMb
            : 50;
        const maxBytes = maxMb * 1024 * 1024;
        const saved = await saveMediaBuffer(
          inboundMedia.buffer,
          inboundMedia.mimetype,
          "inbound",
          maxBytes,
          inboundMedia.fileName,
        );
        mediaPath = saved.path;
        mediaType = inboundMedia.mimetype;
        mediaFileName = inboundMedia.fileName;
      }
    } catch (err) {
      logVerbose(`Inbound media download failed: ${String(err)}`);
    }

    return {
      body,
      location: location ?? undefined,
      replyContext,
      mediaPath,
      mediaType,
      mediaFileName,
    };
  };

  const enqueueInboundMessage = async (
    msg: WAMessage,
    inbound: NormalizedInboundMessage,
    enriched: EnrichedInboundMessage,
  ) => {
    const chatJid = inbound.remoteJid;
    const sendComposing = async () => {
      try {
        await sock.sendPresenceUpdate("composing", chatJid);
      } catch (err) {
        logVerbose(`Presence update failed: ${String(err)}`);
      }
    };
    const reply = async (text: string) => {
      await sock.sendMessage(chatJid, { text });
    };
    const sendMedia = async (payload: AnyMessageContent) => {
      await sock.sendMessage(chatJid, payload);
    };
    const timestamp = inbound.messageTimestampMs;
    const mentionedJids = extractMentionedJids(msg.message as proto.IMessage | undefined);
    const senderName = msg.pushName ?? undefined;

    inboundLogger.info(
      {
        from: inbound.from,
        to: selfE164 ?? "me",
        body: enriched.body,
        mediaPath: enriched.mediaPath,
        mediaType: enriched.mediaType,
        mediaFileName: enriched.mediaFileName,
        timestamp,
      },
      "inbound message",
    );
    const inboundMessage: WebInboundMessage = {
      id: inbound.id,
      from: inbound.from,
      conversationId: inbound.from,
      to: selfE164 ?? "me",
      accountId: inbound.access.resolvedAccountId,
      body: enriched.body,
      pushName: senderName,
      timestamp,
      chatType: inbound.group ? "group" : "direct",
      chatId: inbound.remoteJid,
      senderJid: inbound.participantJid,
      senderE164: inbound.senderE164 ?? undefined,
      senderName,
      replyToId: enriched.replyContext?.id,
      replyToBody: enriched.replyContext?.body,
      replyToSender: enriched.replyContext?.sender,
      replyToSenderJid: enriched.replyContext?.senderJid,
      replyToSenderE164: enriched.replyContext?.senderE164,
      groupSubject: inbound.groupSubject,
      groupParticipants: inbound.groupParticipants,
      mentionedJids: mentionedJids ?? undefined,
      selfJid,
      selfLid,
      selfE164,
      fromMe: Boolean(msg.key?.fromMe),
      location: enriched.location ?? undefined,
      sendComposing,
      reply,
      sendMedia,
      mediaPath: enriched.mediaPath,
      mediaType: enriched.mediaType,
      mediaFileName: enriched.mediaFileName,
    };
    try {
      const task = Promise.resolve(debouncer.enqueue(inboundMessage));
      void task.catch((err) => {
        inboundLogger.error({ error: String(err) }, "failed handling inbound web message");
        inboundConsoleLog.error(`Failed handling inbound web message: ${String(err)}`);
      });
    } catch (err) {
      inboundLogger.error({ error: String(err) }, "failed handling inbound web message");
      inboundConsoleLog.error(`Failed handling inbound web message: ${String(err)}`);
    }
  };

  const handleMessagesUpsert = async (upsert: { type?: string; messages?: Array<WAMessage> }) => {
    if (upsert.type !== "notify" && upsert.type !== "append") {
      return;
    }
    for (const msg of upsert.messages ?? []) {
      // Track poll creation messages for later vote decryption.
      if (msg.key?.id && isPollCreationMessage(msg.message ?? undefined)) {
        pollStore.trackPoll(msg.key.id, msg);
      }
      recordChannelActivity({
        channel: "whatsapp",
        accountId: options.accountId,
        direction: "inbound",
      });
      const inbound = await normalizeInboundMessage(msg);
      if (!inbound) {
        continue;
      }

      await maybeMarkInboundAsRead(inbound);

      // If this is history/offline catch-up, mark read above but skip auto-reply.
      if (upsert.type === "append") {
        const APPEND_RECENT_GRACE_MS = 60_000;
        const msgTsRaw = msg.messageTimestamp;
        const msgTsNum = msgTsRaw != null ? Number(msgTsRaw) : NaN;
        const msgTsMs = Number.isFinite(msgTsNum) ? msgTsNum * 1000 : 0;
        if (msgTsMs < connectedAtMs - APPEND_RECENT_GRACE_MS) {
          continue;
        }
      }

      const enriched = await enrichInboundMessage(msg);
      if (!enriched) {
        continue;
      }

      // If this is a poll vote (pollUpdateMessage), try to enrich the body with
      // actual vote selections by decrypting via the PollStore.
      // Use normalizeMessageContent to unwrap ephemeral/viewOnce wrappers.
      const normalizedMsg = normalizeMessageContent(msg.message ?? undefined);
      const pollUpdateMsg = normalizedMsg?.pollUpdateMessage;
      if (pollUpdateMsg && enriched.body === "<media:poll-vote>") {
        const pollCreationKey = pollUpdateMsg.pollCreationMessageKey;
        const pollMsgId = pollCreationKey?.id;
        inboundLogger.info(
          {
            pollMsgId,
            hasPollCreationKey: Boolean(pollCreationKey),
            hasVote: Boolean(pollUpdateMsg.vote),
            msgKeys: Object.keys(normalizedMsg ?? {}),
            storedPollExists: pollMsgId ? Boolean(pollStore.getPoll(pollMsgId)) : false,
          },
          "poll vote upsert: attempting decryption",
        );
        if (pollMsgId) {
          const storedPoll = pollStore.getPoll(pollMsgId);
          if (storedPoll?.message) {
            try {
              // The vote in messages.upsert is encrypted (IPollEncValue).
              // Use decryptPollVote to decrypt it, then match SHA-256 hashes
              // of option names to determine the selected options.
              const storedMessage = storedPoll.message.message;
              const pollEncKey = (storedMessage as Record<string, unknown>)?.messageContextInfo as
                | { messageSecret?: Uint8Array }
                | undefined;
              // After JSON round-trip (PollStore disk persistence), messageSecret
              // is a base64 string instead of Uint8Array. Convert it back.
              const rawSecret = pollEncKey?.messageSecret;
              const messageSecret: Uint8Array | undefined =
                typeof rawSecret === "string"
                  ? Buffer.from(rawSecret, "base64")
                  : rawSecret instanceof Uint8Array
                    ? rawSecret
                    : rawSecret
                      ? Buffer.from(Object.values(rawSecret as Record<string, number>))
                      : undefined;

              const voterJid = msg.key?.participant || msg.key?.remoteJid;
              const voterE164 = voterJid ? await resolveInboundJid(voterJid) : null;
              const voterLabel = voterE164 ?? voterJid ?? "unknown";

              // Extract poll question and options.
              const creation = (storedMessage?.pollCreationMessage ??
                (storedMessage as Record<string, unknown> | undefined)?.pollCreationMessageV2 ??
                (storedMessage as Record<string, unknown> | undefined)?.pollCreationMessageV3) as
                | { name?: string; options?: Array<{ optionName?: string }> }
                | undefined;
              const pollQuestion = (creation?.name ?? "Poll").trim() || "Poll";
              const options = creation?.options ?? [];

              if (messageSecret && pollUpdateMsg.vote) {
                // Use Baileys' getKeyAuthor logic:
                // pollCreatorJid = fromMe ? meId : (participant || remoteJid)
                // voterJid = fromMe ? meId : (participant || remoteJid) for the vote msg
                // Note: WhatsApp LID privacy may require using selfLid instead of selfJid.
                // Try both — first with LID, fallback to JID.
                const meJid = jidNormalizedUser(selfJid ?? "");
                const meLid = selfLid ? jidNormalizedUser(selfLid) : null;

                const getCreatorJid = (meId: string) =>
                  storedPoll.message.key?.fromMe
                    ? meId
                    : jidNormalizedUser(
                        storedPoll.message.key?.participant ||
                          storedPoll.message.key?.remoteJid ||
                          "",
                      );
                const getVoterJid = (meId: string) =>
                  msg.key?.fromMe
                    ? meId
                    : jidNormalizedUser(msg.key?.participant || msg.key?.remoteJid || "");

                // Try LID first (newer WhatsApp privacy), then JID.
                const attempts = meLid ? [meLid, meJid] : [meJid];
                let decryptedVote: { selectedOptions?: Uint8Array[] } | null = null;
                let usedCreatorJid = "";
                let usedVoterJid = "";

                for (const meId of attempts) {
                  usedCreatorJid = getCreatorJid(meId);
                  usedVoterJid = getVoterJid(meId);
                  try {
                    decryptedVote = decryptPollVote(pollUpdateMsg.vote, {
                      pollEncKey: messageSecret as Buffer,
                      pollCreatorJid: usedCreatorJid,
                      pollMsgId,
                      voterJid: usedVoterJid,
                    });
                    break; // Success — stop trying.
                  } catch {
                    // Try next identity format.
                    decryptedVote = null;
                  }
                }

                if (decryptedVote) {
                  // decryptedVote.selectedOptions are SHA-256 hashes (Uint8Array[]).
                  // Match them against SHA-256 hashes of option names.
                  const { createHash } = await import("node:crypto");
                  const selectedOptionHashes = new Set(
                    (decryptedVote.selectedOptions ?? []).map((opt) =>
                      Buffer.from(opt).toString("hex"),
                    ),
                  );

                  const selected: string[] = [];
                  for (const opt of options) {
                    const optName = opt.optionName ?? "";
                    const hash = createHash("sha256").update(Buffer.from(optName)).digest("hex");
                    if (selectedOptionHashes.has(hash)) {
                      selected.push(optName);
                    }
                  }

                  inboundLogger.info(
                    {
                      selected,
                      voterLabel,
                      pollQuestion,
                      optionCount: options.length,
                      usedCreatorJid,
                      usedVoterJid,
                    },
                    "poll vote decrypted successfully",
                  );

                  if (selected.length > 0) {
                    const selectedStr = selected.join(", ");
                    enriched.body = `<media:poll-vote voter="${voterLabel}" selected="${selectedStr}" poll="${pollQuestion}">`;
                  } else {
                    enriched.body = `<media:poll-vote voter="${voterLabel}" poll="${pollQuestion}">`;
                  }
                } else {
                  inboundLogger.info(
                    {
                      meJid,
                      meLid,
                      pollMsgId,
                      pollCreatorFromMe: storedPoll.message.key?.fromMe,
                      voterFromMe: msg.key?.fromMe,
                      voterRemoteJid: msg.key?.remoteJid,
                      voterParticipant: msg.key?.participant,
                    },
                    "poll vote decryption FAILED with all identity variants",
                  );
                  enriched.body = `<media:poll-vote voter="${voterLabel}" poll="${pollQuestion}">`;
                }
              } else {
                enriched.body = `<media:poll-vote voter="${voterLabel}" poll="${pollQuestion}">`;
                inboundLogger.info(
                  {
                    hasMessageSecret: Boolean(messageSecret),
                    hasVote: Boolean(pollUpdateMsg.vote),
                  },
                  "poll vote: missing encryption data for decryption",
                );
              }
            } catch (err) {
              inboundLogger.info(
                { error: String(err), pollMsgId },
                "poll vote decryption FAILED (outer)",
              );
            }
          }
        }
      }

      await enqueueInboundMessage(msg, inbound, enriched);
    }
  };
  sock.ev.on("messages.upsert", handleMessagesUpsert);

  // Poll vote updates arrive via messages.update with pollUpdates array.
  // These are separate from messages.upsert and must be subscribed to explicitly.
  // We decrypt votes using getAggregateVotesInPollMessage (requires the original
  // poll creation message from PollStore), deduplicate cumulative vote events,
  // and construct synthetic WAMessages so votes flow through the full pipeline.
  const handleMessagesUpdate = async (
    updates: Array<{ key: proto.IMessageKey; update: Partial<WAMessage> }>,
  ) => {
    inboundLogger.info(
      { updateCount: updates.length, keys: updates.map((u) => u.key?.id).slice(0, 5) },
      "messages.update event received",
    );
    for (const { key, update } of updates) {
      const hasPollUpdates = Boolean(update.pollUpdates && update.pollUpdates.length > 0);
      if (hasPollUpdates) {
        inboundLogger.info(
          { messageId: key.id, chatId: key.remoteJid, pollUpdateCount: update.pollUpdates?.length },
          "poll vote update found in messages.update",
        );
      }
      if (!update.pollUpdates || update.pollUpdates.length === 0) {
        continue;
      }
      const chatJid = key.remoteJid;
      const pollMessageId = key.id;
      if (!chatJid || !pollMessageId) {
        continue;
      }

      // Look up the original poll creation message for vote decryption.
      const storedPoll = pollStore.getPoll(pollMessageId);
      let voteAggregation: Array<{ name: string; voters: string[] }> = [];
      let pollQuestion = "Poll";

      if (storedPoll?.message) {
        try {
          voteAggregation = getAggregateVotesInPollMessage(
            {
              message: storedPoll.message.message ?? undefined,
              pollUpdates: update.pollUpdates,
            } as Pick<WAMessage, "pollUpdates" | "message">,
            selfJid ?? undefined,
          );
          // Extract the poll question for context.
          const creation =
            storedPoll.message.message?.pollCreationMessage ??
            (storedPoll.message.message as Record<string, unknown> | undefined)
              ?.pollCreationMessageV2 ??
            (storedPoll.message.message as Record<string, unknown> | undefined)
              ?.pollCreationMessageV3;
          if (creation && typeof creation === "object" && "name" in creation) {
            pollQuestion = ((creation as { name?: string }).name ?? "Poll").trim() || "Poll";
          }
        } catch (err) {
          logVerbose(`Failed to decrypt poll votes for ${pollMessageId}: ${String(err)}`);
        }
      } else {
        logVerbose(
          `Poll creation message not found in store for ${pollMessageId}; vote data unavailable`,
        );
      }

      inboundLogger.info(
        {
          chatId: chatJid,
          messageId: pollMessageId,
          voteCount: update.pollUpdates.length,
          aggregation: voteAggregation,
        },
        "poll vote update received",
      );

      // Build a map of voter → selected options from the aggregation.
      const voterSelections = new Map<string, string[]>();
      for (const option of voteAggregation) {
        for (const voter of option.voters) {
          const existing = voterSelections.get(voter) ?? [];
          existing.push(option.name);
          voterSelections.set(voter, existing);
        }
      }

      // Process each pollUpdate entry individually for access control.
      // Deduplicate so cumulative Baileys events don't re-notify the same voter.
      for (const pollUpdate of update.pollUpdates) {
        const voterKey = pollUpdate.pollUpdateMessageKey;
        const voterParticipant = voterKey?.participant ?? key.participant;
        if (!voterParticipant) {
          continue;
        }

        // Skip if this voter was already reported for this poll.
        if (pollStore.isVoteReported(pollMessageId, voterParticipant)) {
          continue;
        }
        pollStore.markVoteReported(pollMessageId, voterParticipant);

        // Build the vote body with actual selection data.
        const selections = voterSelections.get(voterParticipant);
        const voterE164 = await resolveInboundJid(voterParticipant);
        const voterLabel = voterE164 ?? voterParticipant;
        let voteBody: string;
        if (selections && selections.length > 0) {
          const selectedStr = selections.join(", ");
          voteBody = `<media:poll-vote voter="${voterLabel}" selected="${selectedStr}" poll="${pollQuestion}">`;
        } else {
          voteBody = `<media:poll-vote voter="${voterLabel}" poll="${pollQuestion}">`;
        }

        const syntheticMsg: WAMessage = {
          key: {
            remoteJid: chatJid,
            id: `poll-vote-${pollMessageId}-${voterParticipant}-${Date.now()}`,
            participant: voterParticipant,
            fromMe: Boolean(voterKey?.fromMe),
          },
          message: { pollUpdateMessage: {} } as proto.IMessage,
          messageTimestamp: Math.floor(Date.now() / 1000),
        };
        recordChannelActivity({
          channel: "whatsapp",
          accountId: options.accountId,
          direction: "inbound",
        });
        const inbound = await normalizeInboundMessage(syntheticMsg);
        if (!inbound) {
          continue;
        }
        // Override the enriched body with our decoded vote data instead of
        // relying on extractMediaPlaceholder (which only returns "<media:poll-vote>").
        const enriched = await enrichInboundMessage(syntheticMsg);
        if (!enriched) {
          continue;
        }
        enriched.body = voteBody;
        await enqueueInboundMessage(syntheticMsg, inbound, enriched);
      }
    }
  };
  sock.ev.on("messages.update", handleMessagesUpdate);

  // Emoji reactions arrive via messages.reaction as a dedicated event.
  // We construct a synthetic WAMessage so the reaction flows through the full pipeline.
  const handleMessagesReaction = async (
    reactions: Array<{ key: proto.IMessageKey; reaction: proto.IReaction }>,
  ) => {
    for (const { key, reaction } of reactions) {
      const emoji = reaction.text ?? "";
      const chatJid = key.remoteJid;
      if (!chatJid) {
        continue;
      }
      inboundLogger.info(
        {
          chatId: chatJid,
          messageId: key.id,
          emoji,
          participant: key.participant,
        },
        "reaction received",
      );
      // Construct a synthetic WAMessage with reactionMessage so it flows through extract.ts.
      // Use the reactor's participant (from the reaction event itself, not reaction.key which
      // refers to the message being reacted to) to preserve correct sender identity.
      const reactorParticipant = key.participant ?? reaction.key?.participant;
      const syntheticMsg: WAMessage = {
        key: {
          remoteJid: chatJid,
          id: `reaction-${key.id}-${reactorParticipant ?? "unknown"}-${Date.now()}`,
          participant: reactorParticipant,
          fromMe: Boolean(key.fromMe),
        },
        message: {
          reactionMessage: {
            key: reaction.key ?? key,
            text: emoji || null,
          },
        } as proto.IMessage,
        messageTimestamp: Math.floor(Date.now() / 1000),
        pushName: undefined,
      };
      recordChannelActivity({
        channel: "whatsapp",
        accountId: options.accountId,
        direction: "inbound",
      });
      const inbound = await normalizeInboundMessage(syntheticMsg);
      if (!inbound) {
        continue;
      }
      const enriched = await enrichInboundMessage(syntheticMsg);
      if (!enriched) {
        continue;
      }
      await enqueueInboundMessage(syntheticMsg, inbound, enriched);
    }
  };
  sock.ev.on("messages.reaction", handleMessagesReaction);

  const handleConnectionUpdate = (
    update: Partial<import("@whiskeysockets/baileys").ConnectionState>,
  ) => {
    try {
      if (update.connection === "close") {
        const status = getStatusCode(update.lastDisconnect?.error);
        resolveClose({
          status,
          isLoggedOut: status === DisconnectReason.loggedOut,
          error: update.lastDisconnect?.error,
        });
      }
    } catch (err) {
      inboundLogger.error({ error: String(err) }, "connection.update handler error");
      resolveClose({ status: undefined, isLoggedOut: false, error: err });
    }
  };
  sock.ev.on("connection.update", handleConnectionUpdate);

  const sendApi = createWebSendApi({
    sock: {
      sendMessage: (jid: string, content: AnyMessageContent) => sock.sendMessage(jid, content),
      sendPresenceUpdate: (presence, jid?: string) => sock.sendPresenceUpdate(presence, jid),
    },
    defaultAccountId: options.accountId,
  });

  return {
    close: async () => {
      try {
        const ev = sock.ev as unknown as {
          off?: (event: string, listener: (...args: unknown[]) => void) => void;
          removeListener?: (event: string, listener: (...args: unknown[]) => void) => void;
        };
        const messagesUpsertHandler = handleMessagesUpsert as unknown as (
          ...args: unknown[]
        ) => void;
        const connectionUpdateHandler = handleConnectionUpdate as unknown as (
          ...args: unknown[]
        ) => void;
        const messagesUpdateHandler = handleMessagesUpdate as unknown as (
          ...args: unknown[]
        ) => void;
        const messagesReactionHandler = handleMessagesReaction as unknown as (
          ...args: unknown[]
        ) => void;
        if (typeof ev.off === "function") {
          ev.off("messages.upsert", messagesUpsertHandler);
          ev.off("messages.update", messagesUpdateHandler);
          ev.off("messages.reaction", messagesReactionHandler);
          ev.off("connection.update", connectionUpdateHandler);
        } else if (typeof ev.removeListener === "function") {
          ev.removeListener("messages.upsert", messagesUpsertHandler);
          ev.removeListener("messages.update", messagesUpdateHandler);
          ev.removeListener("messages.reaction", messagesReactionHandler);
          ev.removeListener("connection.update", connectionUpdateHandler);
        }
        sock.ws?.close();
      } catch (err) {
        logVerbose(`Socket close failed: ${String(err)}`);
      }
    },
    onClose,
    signalClose: (reason?: WebListenerCloseReason) => {
      resolveClose(reason ?? { status: undefined, isLoggedOut: false, error: "closed" });
    },
    // IPC surface (sendMessage/sendPoll/sendReaction/sendComposingTo)
    // Wrap sendPoll to track outbound polls in the PollStore for vote decryption.
    ...sendApi,
    sendPoll: async (
      to: string,
      poll: { question: string; options: string[]; maxSelections?: number },
    ) => {
      const result = await sendApi.sendPoll(to, poll);
      // Store the ACTUAL WAMessage returned by Baileys (contains messageSecret
      // needed for poll vote decryption) rather than a synthetic one.
      if (result.messageId && result.messageId !== "unknown") {
        inboundLogger.info(
          { messageId: result.messageId, question: poll.question },
          "tracking outbound poll in PollStore",
        );
        const rawMsg = result.rawResult as WAMessage | undefined;
        if (rawMsg?.message) {
          pollStore.trackPoll(result.messageId, rawMsg);
        } else {
          // Fallback: synthetic message (won't decrypt but preserves poll name).
          const syntheticCreation: WAMessage = {
            key: { remoteJid: to, id: result.messageId, fromMe: true },
            message: {
              pollCreationMessage: {
                name: poll.question,
                options: poll.options.map((name) => ({ optionName: name })),
                selectableOptionsCount: poll.maxSelections ?? 1,
              },
            } as proto.IMessage,
          };
          pollStore.trackPoll(result.messageId, syntheticCreation);
        }
      }
      return result;
    },
  } as const;
}
