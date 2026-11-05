@echo off
cd /d %~dp0
:loop
python bot.py
echo Bot stopped - restarting in 10 seconds (close this window to stop)
timeout /t 10
goto loop
