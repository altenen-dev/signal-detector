"""
Keeps the bot running. Start this instead of bot.py (start_bot.bat does).

  * Starts bot.py and restarts it 10 s after it stops or crashes.
  * Restarts the bot when a .py file or config.yaml in this folder changes -
    for example after you run `git pull`, copy in a new file, or edit the settings.

Open trades are picked up again after each restart from state.json, and
messages that arrive during the few seconds of a restart are delivered by
Telegram once the bot is back.
"""
import glob
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
RESTART_DELAY = 10
WIN = os.name == "nt"


def say(msg):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  RUN     {msg}", flush=True)


def fingerprint():
    files = sorted(glob.glob(os.path.join(HERE, "*.py")) + [os.path.join(HERE, "config.yaml")])
    return tuple((f, os.path.getmtime(f)) for f in files if os.path.exists(f))


def start_bot():
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if WIN else 0
    env = dict(os.environ, PYTHONIOENCODING="utf-8:replace", PYTHONUTF8="1")
    return subprocess.Popen([sys.executable, "bot.py", *sys.argv[1:]], cwd=HERE, creationflags=flags, env=env)


def stop_bot(p):
    if p.poll() is not None:
        return
    try:
        p.send_signal(signal.CTRL_BREAK_EVENT if WIN else signal.SIGTERM)
        p.wait(timeout=20)
    except Exception:
        pass
    if p.poll() is None:
        p.kill()
        p.wait()


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    say("starting the bot - it restarts by itself after `git pull` or when you change a file here")
    bot, prints = start_bot(), fingerprint()
    try:
        while True:
            time.sleep(2)
            if bot.poll() is not None:
                say(f"bot stopped (code {bot.returncode}) - restarting in {RESTART_DELAY} s")
                time.sleep(RESTART_DELAY)
                bot, prints = start_bot(), fingerprint()
                continue
            if fingerprint() != prints:
                time.sleep(3)              # let git pull / the copy finish writing every file
                say("code or settings changed - restarting the bot")
                stop_bot(bot)
                bot, prints = start_bot(), fingerprint()
    except KeyboardInterrupt:
        say("stopping")
        stop_bot(bot)


if __name__ == "__main__":
    main()
