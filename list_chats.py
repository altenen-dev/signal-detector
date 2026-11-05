"""
Prints every Telegram group/channel you are in with its numeric ID,
so you can copy the right ID into config.yaml -> telegram.chat.

Run:  python list_chats.py
"""
import asyncio
import os

import yaml
from telethon import TelegramClient

HERE = os.path.dirname(os.path.abspath(__file__))


async def main():
    with open(os.path.join(HERE, "config.yaml"), encoding="utf-8") as f:
        tg = yaml.safe_load(f)["telegram"]
    client = TelegramClient(os.path.join(HERE, tg.get("session_name", "signal_bot")),
                            int(tg["api_id"]), tg["api_hash"])
    await client.start()
    print(f"{'ID':>16}  NAME")
    async for d in client.iter_dialogs():
        if d.is_group or d.is_channel:
            print(f"{d.id:>16}  {d.name}")
    await client.disconnect()


asyncio.run(main())
