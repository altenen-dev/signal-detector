"""
Gold signal bot - reads signals from a Telegram group and trades them on MT5.

Run:   python bot.py            (uses config.yaml in this folder)
       python bot.py my.yaml    (other config file)

First run asks for your phone number and the login code Telegram sends you.
After that the login is saved in <session_name>.session (keep that file private).
"""
import asyncio
import logging
import os
import sys

import yaml
from telethon import TelegramClient, events

from brokers import make_broker
from engine import Engine

HERE = os.path.dirname(os.path.abspath(__file__))


def setup_logging():
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in (logging.StreamHandler(sys.stdout),
              logging.FileHandler(os.path.join(HERE, "bot.log"), encoding="utf-8")):
        h.setFormatter(fmt)
        root.addHandler(h)
    logging.getLogger("telethon").setLevel(logging.WARNING)


def load_config(path):
    if not os.path.exists(path):
        sys.exit(f"Config file not found: {path}\nCopy config.example.yaml to config.yaml and fill it in.")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


async def resolve_chat(client, chat):
    await client.get_dialogs()          # fills Telethon's cache so numeric IDs resolve
    if isinstance(chat, str) and chat.lstrip("-").isdigit():
        chat = int(chat)
    return await client.get_entity(chat)


async def main():
    setup_logging()
    log = logging.getLogger("bot")
    cfg = load_config(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "config.yaml"))
    tg, trading = cfg["telegram"], cfg.get("trading", {})

    broker = make_broker(cfg["broker"], int(trading.get("magic", 20261104)))
    log.info("Connecting to broker (%s)...", cfg["broker"].get("backend", "mt5"))
    await broker.connect()

    engine = Engine(trading, broker,
                    state_path=os.path.join(HERE, "state.json"),
                    journal_path=os.path.join(HERE, "trades_journal.csv"))
    await engine.start()

    client = TelegramClient(os.path.join(HERE, tg.get("session_name", "signal_bot")),
                            int(tg["api_id"]), tg["api_hash"])
    await client.start()
    chat = await resolve_chat(client, tg["chat"])
    title = getattr(chat, "title", None) or getattr(chat, "username", chat.id)
    log.info("Listening to Telegram chat: %s", title)

    @client.on(events.NewMessage(chats=chat))
    async def on_new(ev):
        await engine.on_message(ev.message.id, ev.raw_text, ev.message.reply_to_msg_id, ev.sender_id)

    @client.on(events.MessageEdited(chats=chat))
    async def on_edit(ev):
        await engine.on_message(ev.message.id, ev.raw_text, ev.message.reply_to_msg_id, ev.sender_id,
                                edited=True)

    monitor = asyncio.create_task(engine.monitor_loop())
    log.info("Bot is running. Press Ctrl+C to stop.")
    try:
        await client.run_until_disconnected()
    finally:
        monitor.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
