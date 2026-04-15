/**
 * Poll store for tracking poll creation messages, enabling vote decryption.
 * Persists to disk so polls survive gateway restarts.
 *
 * `decryptPollVote` from Baileys requires the original poll creation message
 * (specifically its `messageSecret`) to decrypt votes. This store captures
 * polls when sent or received, and provides deduplication so cumulative vote
 * events don't produce duplicate notifications.
 */

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { proto, WAMessage } from "@whiskeysockets/baileys";

export interface StoredPoll {
  /** The full WAMessage of the poll creation (needed for decryption). */
  message: WAMessage;
  /** Set of voter JIDs that have already been reported for this poll. */
  reportedVoters: Set<string>;
  /** Timestamp when this entry was created. */
  createdAt: number;
}

interface SerializedPoll {
  message: WAMessage;
  reportedVoters: string[];
  createdAt: number;
}

const POLL_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const MAX_POLLS = 200;

export class PollStore {
  private polls = new Map<string, StoredPoll>();
  private persistPath: string | null;
  private saveTimeout: ReturnType<typeof setTimeout> | null = null;

  /**
   * @param persistPath Path to JSON file for persistence. Pass null for in-memory only.
   */
  constructor(persistPath?: string | null) {
    this.persistPath = persistPath ?? null;
    if (this.persistPath) {
      this.loadFromDisk();
    }
  }

  /** Track a poll creation message (outbound or inbound). */
  trackPoll(messageId: string, message: WAMessage): void {
    this.prune();
    this.polls.set(messageId, {
      message,
      reportedVoters: new Set(),
      createdAt: Date.now(),
    });
    this.scheduleSave();
  }

  /** Retrieve the stored poll creation message for vote decryption. */
  getPoll(messageId: string): StoredPoll | undefined {
    return this.polls.get(messageId);
  }

  /** Check if a voter has already been reported for a given poll. */
  isVoteReported(pollMessageId: string, voterJid: string): boolean {
    const poll = this.polls.get(pollMessageId);
    return poll?.reportedVoters.has(voterJid) ?? false;
  }

  /** Mark a voter as reported for a given poll. */
  markVoteReported(pollMessageId: string, voterJid: string): void {
    const poll = this.polls.get(pollMessageId);
    if (poll) {
      poll.reportedVoters.add(voterJid);
      this.scheduleSave();
    }
  }

  /** Remove expired entries and enforce size cap. */
  private prune(): void {
    const now = Date.now();
    for (const [id, entry] of this.polls) {
      if (now - entry.createdAt > POLL_TTL_MS) {
        this.polls.delete(id);
      }
    }
    // If still over limit, drop oldest entries.
    if (this.polls.size >= MAX_POLLS) {
      const sorted = [...this.polls.entries()].toSorted((a, b) => a[1].createdAt - b[1].createdAt);
      const toRemove = sorted.slice(0, sorted.length - MAX_POLLS + 1);
      for (const [id] of toRemove) {
        this.polls.delete(id);
      }
    }
  }

  /** Debounced save — batches rapid writes into a single disk write. */
  private scheduleSave(): void {
    if (!this.persistPath) {
      return;
    }
    if (this.saveTimeout) {
      clearTimeout(this.saveTimeout);
    }
    this.saveTimeout = setTimeout(() => {
      this.saveToDisk();
      this.saveTimeout = null;
    }, 1000);
  }

  /** Serialize and write to disk. */
  private saveToDisk(): void {
    if (!this.persistPath) {
      return;
    }
    try {
      const data: Record<string, SerializedPoll> = {};
      for (const [id, poll] of this.polls) {
        data[id] = {
          message: poll.message,
          reportedVoters: [...poll.reportedVoters],
          createdAt: poll.createdAt,
        };
      }
      mkdirSync(dirname(this.persistPath), { recursive: true });
      writeFileSync(this.persistPath, JSON.stringify(data, null, 2));
    } catch {
      // Silently ignore write errors — in-memory store still works.
    }
  }

  /** Load from disk on startup. */
  private loadFromDisk(): void {
    if (!this.persistPath) {
      return;
    }
    try {
      const raw = readFileSync(this.persistPath, "utf-8");
      const data = JSON.parse(raw) as Record<string, SerializedPoll>;
      const now = Date.now();
      for (const [id, entry] of Object.entries(data)) {
        // Skip expired entries on load.
        if (now - entry.createdAt > POLL_TTL_MS) {
          continue;
        }
        this.polls.set(id, {
          message: entry.message,
          reportedVoters: new Set(entry.reportedVoters),
          createdAt: entry.createdAt,
        });
      }
    } catch {
      // File doesn't exist or is corrupted — start fresh.
    }
  }
}

/**
 * Check if a WAMessage contains a poll creation message (any version).
 */
export function isPollCreationMessage(message: proto.IMessage | undefined | null): boolean {
  if (!message) {
    return false;
  }
  return Boolean(
    message.pollCreationMessage ??
    (message as Record<string, unknown>).pollCreationMessageV2 ??
    (message as Record<string, unknown>).pollCreationMessageV3,
  );
}
