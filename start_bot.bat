@echo off
cd /d %~dp0
rem Starts the bot through run.py: it restarts the bot if it stops, and after git pull.
:loop
python run.py
echo run.py stopped - starting again in 10 seconds (close this window to stop)
timeout /t 10
goto loop
