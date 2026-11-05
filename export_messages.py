"""
Downloads the signal group's message history so the parser can be tested on
every real signal and follow-up message.

Run:   python export_messages.py            (last 3000 messages)
       python export_messages.py 10000      (more)

Creates two files in this folder:
  group_messages.json   - every text message: id, date, text, reply-to id, sender id
  parser_report.txt     - what the bot understood from each message, and a list of
                          messages that mention BUY/SELL/SL/TP but were NOT understood

Send group_messages.json (or parser_report.txt) back for the parser to be tuned.
Only message text is exported: no names, phone numbers or photos.
"""
import asyncio
import json
import os
import re
import sys

import yaml
from telethon import TelegramClient

from signal_parser import parse_message

HERE = os.path.dirname(os.path.abspath(__file__))
TRADING_WORDS = re.compile(r"\b(BUY|SELL|SL|TP\d?|STOP\s*LOSS|TAKE\s*PROFIT|ENTRY|BE|CLOSE|LIMIT)\b", re.I)


async def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    with open(os.path.join(HERE, "config.yaml"), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    tg = cfg["telegram"]
    client = TelegramClient(os.path.join(HERE, tg.get("session_name", "signal_bot")),
                            int(tg["api_id"]), tg["api_hash"])
    await client.start()
    await client.get_dialogs()
    chat = tg["chat"]
    if isinstance(chat, str) and chat.lstrip("-").isdigit():
        chat = int(chat)
    entity = await client.get_entity(chat)

    msgs = []
    print(f"Downloading up to {limit} messages ...")
    async for m in client.iter_messages(entity, limit=limit):
        if m.raw_text:
            msgs.append({"id": m.id, "date": m.date.isoformat(), "text": m.raw_text,
                         "reply_to": m.reply_to_msg_id, "sender_id": m.sender_id,
                         "edited": bool(m.edit_date), "has_photo": bool(m.photo)})
    await client.disconnect()
    msgs.reverse()  # oldest first

    with open(os.path.join(HERE, "group_messages.json"), "w", encoding="utf-8") as f:
        json.dump(msgs, f, ensure_ascii=False, indent=1)

    n_sig = n_cmd = 0
    missed, lines = [], []
    for m in msgs:
        r = parse_message(m["text"], cfg.get("trading", {}).get("require_symbol_word", False))
        one_line = m["text"].replace("\n", " | ")[:160]
        if r.signal:
            n_sig += 1
            s = r.signal
            entry = "market" if s.entry_low is None else (
                s.entry_low if s.entry_low == s.entry_high else f"{s.entry_low} & {s.entry_high}")
            lines.append(f"[{m['id']}] SIGNAL {s.direction} entry {entry} SL {s.sl} TPs {s.tps}\n    {one_line}")
        elif r.commands:
            n_cmd += 1
            lines.append(f"[{m['id']}] COMMAND {[c.action + (f' {c.value}' if c.value else '') for c in r.commands]}"
                         f"{' (reply to ' + str(m['reply_to']) + ')' if m['reply_to'] else ''}\n    {one_line}")
        elif TRADING_WORDS.search(m["text"]):
            missed.append(f"[{m['id']}] NOT UNDERSTOOD ({r.reason})\n    {one_line}")

    with open(os.path.join(HERE, "parser_report.txt"), "w", encoding="utf-8") as f:
        f.write(f"{len(msgs)} text messages | {n_sig} signals | {n_cmd} commands | "
                f"{len(missed)} trading-looking messages not understood\n\n")
        f.write("=== NOT UNDERSTOOD (check these) ===\n" + "\n".join(missed) + "\n\n")
        f.write("=== UNDERSTOOD ===\n" + "\n".join(lines) + "\n")

    print(f"Saved {len(msgs)} messages to group_messages.json")
    print(f"Signals read: {n_sig} | commands read: {n_cmd} | not understood: {len(missed)}")
    print("Details in parser_report.txt")


asyncio.run(main())
