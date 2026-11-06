# Gold Signal Bot (Telegram → MT5)

Reads BUY/SELL gold (XAUUSD) signals from your Telegram group and places them on
your MetaTrader 5 account with the entry, stop loss and take profit, then
protects the trade automatically.

## What it does

| Situation | What the bot does |
|---|---|
| Signal still being typed ("GOLD SELL LIMIT 4121", "GOLD BUY  Entry: 4052 & 4049  SL: TP:") | Placed straight away with a **temporary SL** ($6) and no TP. When the admin edits in the SL and TPs, waiting orders are replaced and open trades get the real SL/TP (lot reduced if the real SL is wider). Not finished within 5 min → waiting orders cancelled. |
| Signal with one entry ("BUY NOW 3906.50") | One trade. **TP = the signal's last TP.** SL = the signal's SL. |
| Signal with two prices ("SELL 3909 - 3911.5", "Buy 3898 & 3895", "sell limit 4020 and 4024") | **Two trades, one at each price**, same SL and last TP. |
| Lot size | Calculated so hitting the SL loses **`risk_percent` %** of your balance — shared between the two entries by default (0.5% + 0.5%). |
| Price at an entry price (± tolerance) or better | That entry is taken at market. |
| Price not there yet | A limit order waits at that price. |
| Price reaches **TP1** | **Each trade's SL moves to its own entry** (+ small offset). An entry that hasn't filled yet is cancelled. |
| Each TP reached (TP1, TP2, TP3…) | **An equal part of the trade is closed** (e.g. 0.04 lots, 4 TPs → 0.01 at each TP). The last part runs to the last TP. |
| Trade is only 0.01 lot | Can't be split, so its TP is set to **TP1** and it closes fully there. |
| Price reaches TP2, TP3… | SL of what's left moves up to TP1, then TP2… (ladder, can be turned off). |
| Signal with only one TP | SL moves to entry at 50% of the way to TP. |
| Admin posts "SL to BE", "risk free", **نامن / التامين اجباري** | SL moved to entry now. |
| **امن و احذف التعزيز**, **حذف التعزيز** | Deletes the unfilled second entry (and moves SL to entry). |
| **نصك الدخول الاول على الدخول** | Closes the first entry at its entry price (now if in profit, otherwise when price returns). |
| **نلغي الأمر / إلغاء الأمر / نمسح الأمر** | Cancels unfilled orders. |
| **نرفع ستوبنا الى 4469** | SL moved to 4469. |
| **هدف الذهب الأخير 4067** | Last TP changed to 4067. |
| **نرجع ندخل بنفس المعطيات / بنكرر** | Places the same signal again (only if its trades are finished). |
| **اغلاق كامل / اغلاق جميع الصفقات** | Closes the trades. |
| Results summaries, "ضرب تامين", "TP1✅", "ممنوع التامين", "اللي حاب…", "اذا وصل…" | Ignored on purpose (reports, warnings, optional or conditional advice). |
| "Move SL to 2655" | SL moved to 2655. |
| "Close half" / "close all now" / "cancel" | Partial close / full close / cancel unfilled order. |
| Admin edits the signal message | New SL/TP applied (protection is never undone). |
| Replies | A command sent as a reply goes to that signal; otherwise to the newest trade. |

Safety built in: **dry-run mode** (on by default), **refuses real-money accounts**
until you set `allow_live: true`, rejects prices far from the market (typos),
skips stale signals already past TP1, max spread, max open trades, pending
orders expire, and every action is written to `trades_journal.csv`.

## Which setup?

| Your machine | `broker.backend` | Needs |
|---|---|---|
| Windows PC or Windows VPS | `mt5` | MT5 desktop terminal (free) |
| Mac, Linux, or anywhere | `metaapi` | A [metaapi.cloud](https://metaapi.cloud) account linked to your MT5 login (third-party service — check their current pricing) |

The bot only trades while it's running. For 24/5 trading, run it on a Windows VPS
(many brokers offer one free) with MT5 open.

## Setup

**1. Install Python 3.10+** from python.org (on Windows tick "Add Python to PATH").

**2. Get Telegram API keys.** Go to https://my.telegram.org → *API development tools*
→ create an app → copy **api_id** and **api_hash**. The bot reads the group through
your own Telegram account, so it works even though you're not the group admin.

**3. Prepare MT5** (backend `mt5`):
- Install MT5 from your broker, log in to a **demo account** first.
- Turn on **Algo Trading** (toolbar button must be green).
- In *Market Watch*, find the exact gold symbol name (e.g. `XAUUSD`, `XAUUSDm`, `GOLD`).

   For backend `metaapi`: add your MT5 account at metaapi.cloud, copy the API token
   and account ID, then also run `pip install metaapi-cloud-sdk`.

**4. Install the bot.** Open a terminal in this folder:
```
pip install -r requirements.txt
```

**5. Configure.** Copy `config.example.yaml` to `config.yaml` and fill in:
`api_id`, `api_hash`, `backend`, `symbol`, `risk_percent`.

**6. Find the group ID:**
```
python list_chats.py
```
First time, Telegram asks for your phone number and sends you a login code.
Copy the group's number (like `-1001234567890`) into `telegram.chat`.

**7. Check everything works offline:**
```
python selftest.py
```

**8. Run in dry-run mode** (default) and watch it read the group's signals:
```
python bot.py
```
Look at the console / `bot.log`: every signal shows what it *would* have done.

**9. Trade on demo:** set `dry_run: false`. Run for 1–2 weeks and review
`trades_journal.csv`.

**10. Real money** (only when you're happy): log MT5 into the live account and
set `allow_live: true`.

## Files

| File | Purpose |
|---|---|
| `start_bot.bat` | **Start here on Windows.** Runs `run.py`, which keeps the bot running and restarts it after `git pull` or a settings change |
| `run.py` | Restarts the bot if it stops, and when a `.py` file or `config.yaml` changes |
| `bot.py` | The bot itself (`python bot.py` runs it without auto-restart) |
| `config.yaml` | Your settings (keep private) |
| `signal_parser.py` | Reads signal text — tune here if your group writes differently |
| `engine.py` | Sizing, entry, break-even, commands, safety |
| `brokers.py` | MT5 and MetaApi connections |
| `selftest.py` | Offline test with a simulated broker |
| `list_chats.py` | Shows your groups' IDs |
| `export_messages.py` | Downloads the group's message history and reports which messages the parser understood |
| `bot.log`, `trades_journal.csv`, `state.json` | Logs, trade history, open-trade memory (created when running) |
| `signal_bot.session` | Your Telegram login — **never share this file** |

## Good to know

- Signals posted as **images** can't be read — only text.
- Stopping and restarting is safe: `state.json` remembers trades and keeps protecting them.
- Run only **one copy** of the bot per account.
- If the group also posts chat that mentions "close" or "BE", set
  `allowed_sender_ids` to the admin IDs so only they can control trades.
- Times the bot can't move the SL (price too close / broker limits) are retried
  automatically and logged.

## Troubleshooting

| Message | Fix |
|---|---|
| `MT5 initialize failed` | Open MT5, log in, enable Algo Trading. Set `mt5.path` if MT5 is installed in an unusual folder. |
| `Symbol 'XAUUSD' not found` | Use the exact symbol name from Market Watch. |
| `retcode 10027` | Algo Trading is off in MT5. |
| `retcode 10016 Invalid stops` | Broker's minimum stop distance — the bot logs it and retries. |
| `LIVE account and allow_live is false` | Working as designed; see step 10. |
| Signal "ignored" in the log | Paste the message text to whoever maintains the parser so the format can be added. |

## Updating

1. Leave `start_bot.bat` running.
2. In the bot folder run `git pull`.
3. That's it: within a few seconds the bot restarts on the new code (the window shows
   `code or settings changed - restarting the bot`). Saving `config.yaml` restarts it the same way.

Open trades carry on after the restart. Messages posted during the few seconds of the
restart are still received; anything older than 2 minutes (`max_message_age_seconds`)
is ignored, so a long stop never makes the bot act on old signals.
