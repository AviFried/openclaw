import { describe, expect, it } from "vitest";
import { extractLocationData, extractMediaPlaceholder, extractText } from "./inbound.js";

describe("web inbound helpers", () => {
  it("prefers the main conversation body", () => {
    const body = extractText({
      conversation: " hello ",
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("hello");
  });

  it("falls back to captions when conversation text is missing", () => {
    const body = extractText({
      imageMessage: { caption: " caption " },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("caption");
  });

  it("handles document captions", () => {
    const body = extractText({
      documentMessage: { caption: " doc " },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("doc");
  });

  it("extracts WhatsApp contact cards", () => {
    const body = extractText({
      contactMessage: {
        displayName: "Ada Lovelace",
        vcard: [
          "BEGIN:VCARD",
          "VERSION:3.0",
          "FN:Ada Lovelace",
          "TEL;TYPE=CELL:+15555550123",
          "END:VCARD",
        ].join("\n"),
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contact: Ada Lovelace, +15555550123>");
  });

  it("prefers FN over N in WhatsApp vcards", () => {
    const body = extractText({
      contactMessage: {
        vcard: [
          "BEGIN:VCARD",
          "VERSION:3.0",
          "N:Lovelace;Ada;;;",
          "FN:Ada Lovelace",
          "TEL;TYPE=CELL:+15555550123",
          "END:VCARD",
        ].join("\n"),
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contact: Ada Lovelace, +15555550123>");
  });

  it("normalizes tel: prefixes in WhatsApp vcards", () => {
    const body = extractText({
      contactMessage: {
        vcard: [
          "BEGIN:VCARD",
          "VERSION:3.0",
          "FN:Ada Lovelace",
          "TEL;TYPE=CELL:tel:+15555550123",
          "END:VCARD",
        ].join("\n"),
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contact: Ada Lovelace, +15555550123>");
  });

  it("trims and skips empty WhatsApp vcard phones", () => {
    const body = extractText({
      contactMessage: {
        vcard: [
          "BEGIN:VCARD",
          "VERSION:3.0",
          "FN:Ada Lovelace",
          "TEL;TYPE=CELL:  +15555550123  ",
          "TEL;TYPE=HOME:   ",
          "TEL;TYPE=WORK:+15555550124",
          "END:VCARD",
        ].join("\n"),
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contact: Ada Lovelace, +15555550123 (+1 more)>");
  });

  it("extracts multiple WhatsApp contact cards", () => {
    const body = extractText({
      contactsArrayMessage: {
        contacts: [
          {
            displayName: "Alice",
            vcard: [
              "BEGIN:VCARD",
              "VERSION:3.0",
              "FN:Alice",
              "TEL;TYPE=CELL:+15555550101",
              "END:VCARD",
            ].join("\n"),
          },
          {
            displayName: "Bob",
            vcard: [
              "BEGIN:VCARD",
              "VERSION:3.0",
              "FN:Bob",
              "TEL;TYPE=CELL:+15555550102",
              "END:VCARD",
            ].join("\n"),
          },
          {
            displayName: "Charlie",
            vcard: [
              "BEGIN:VCARD",
              "VERSION:3.0",
              "FN:Charlie",
              "TEL;TYPE=CELL:+15555550103",
              "TEL;TYPE=HOME:+15555550104",
              "END:VCARD",
            ].join("\n"),
          },
          {
            displayName: "Dana",
            vcard: [
              "BEGIN:VCARD",
              "VERSION:3.0",
              "FN:Dana",
              "TEL;TYPE=CELL:+15555550105",
              "END:VCARD",
            ].join("\n"),
          },
        ],
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe(
      "<contacts: Alice, +15555550101, Bob, +15555550102, Charlie, +15555550103 (+1 more), Dana, +15555550105>",
    );
  });

  it("counts empty WhatsApp contact cards in array summaries", () => {
    const body = extractText({
      contactsArrayMessage: {
        contacts: [
          {
            displayName: "Alice",
            vcard: [
              "BEGIN:VCARD",
              "VERSION:3.0",
              "FN:Alice",
              "TEL;TYPE=CELL:+15555550101",
              "END:VCARD",
            ].join("\n"),
          },
          {},
          {},
        ],
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contacts: Alice, +15555550101 +2 more>");
  });

  it("summarizes empty WhatsApp contact cards with a count", () => {
    const body = extractText({
      contactsArrayMessage: {
        contacts: [{}, {}],
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("<contacts: 2 contacts>");
  });

  it("unwraps view-once v2 extension messages", () => {
    const body = extractText({
      viewOnceMessageV2Extension: {
        message: { conversation: " hello " },
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(body).toBe("hello");
  });

  it("returns placeholders for media-only payloads", () => {
    expect(
      extractMediaPlaceholder({
        imageMessage: {},
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:image>");
    expect(
      extractMediaPlaceholder({
        audioMessage: {},
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:audio>");
  });

  it("returns poll creation placeholder with question and options", () => {
    expect(
      extractMediaPlaceholder({
        pollCreationMessage: {
          name: "Mincha 2:30?",
          options: [{ optionName: "yes" }, { optionName: "no" }, { optionName: "+1" }],
        },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:poll>\nMincha 2:30?\n1. yes\n2. no\n3. +1");
  });

  it("returns poll creation V2 placeholder", () => {
    expect(
      extractMediaPlaceholder({
        pollCreationMessageV2: {
          name: "Lunch?",
          options: [{ optionName: "Pizza" }, { optionName: "Sushi" }],
        },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:poll>\nLunch?\n1. Pizza\n2. Sushi");
  });

  it("returns poll-vote placeholder for poll update messages", () => {
    expect(
      extractMediaPlaceholder({
        pollUpdateMessage: {},
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:poll-vote>");
  });

  it("returns reaction placeholder with emoji", () => {
    expect(
      extractMediaPlaceholder({
        reactionMessage: { text: "\ud83d\ude02" },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<reaction:\ud83d\ude02>");
  });

  it("returns reaction:removed for empty reaction", () => {
    expect(
      extractMediaPlaceholder({
        reactionMessage: { text: "" },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<reaction:removed>");
  });

  it("returns event placeholder with name and description", () => {
    expect(
      extractMediaPlaceholder({
        eventMessage: { name: "Team Meeting", description: "Weekly sync", isCanceled: false },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:event>\nTeam Meeting\nWeekly sync");
  });

  it("returns event placeholder with CANCELED flag", () => {
    expect(
      extractMediaPlaceholder({
        eventMessage: { name: "Team Meeting", isCanceled: true },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:event> [CANCELED]\nTeam Meeting");
  });

  it("returns event-rsvp placeholder for event responses", () => {
    expect(
      extractMediaPlaceholder({
        eventResponseMessage: { response: 1 },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<event-rsvp:going>");
    expect(
      extractMediaPlaceholder({
        eventResponseMessage: { response: 2 },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<event-rsvp:not-going>");
    expect(
      extractMediaPlaceholder({
        eventResponseMessage: { response: 1, extraGuestCount: 3 },
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<event-rsvp:going +3>");
  });

  it("returns encrypted-update placeholder for secret messages", () => {
    expect(
      extractMediaPlaceholder({
        secretEncryptedMessage: {},
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBe("<media:encrypted-update>");
  });

  it("returns undefined for unrecognized message types", () => {
    expect(
      extractMediaPlaceholder({
        someUnknownMessage: {},
      } as unknown as import("@whiskeysockets/baileys").proto.IMessage),
    ).toBeUndefined();
  });

  it("extracts WhatsApp location messages", () => {
    const location = extractLocationData({
      locationMessage: {
        degreesLatitude: 48.858844,
        degreesLongitude: 2.294351,
        name: "Eiffel Tower",
        address: "Champ de Mars, Paris",
        accuracyInMeters: 12,
        comment: "Meet here",
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(location).toEqual({
      latitude: 48.858844,
      longitude: 2.294351,
      accuracy: 12,
      name: "Eiffel Tower",
      address: "Champ de Mars, Paris",
      caption: "Meet here",
      source: "place",
      isLive: false,
    });
  });

  it("extracts WhatsApp live location messages", () => {
    const location = extractLocationData({
      liveLocationMessage: {
        degreesLatitude: 37.819929,
        degreesLongitude: -122.478255,
        accuracyInMeters: 20,
        caption: "On the move",
      },
    } as unknown as import("@whiskeysockets/baileys").proto.IMessage);
    expect(location).toEqual({
      latitude: 37.819929,
      longitude: -122.478255,
      accuracy: 20,
      caption: "On the move",
      source: "live",
      isLive: true,
    });
  });
});
