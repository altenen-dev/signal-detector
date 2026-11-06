@echo off
cd /d %~dp0
rem Starts the bot through run.py: it restarts the bot if it stops, and after git pull.
:loop
python run.py
if %errorlevel%==3 goto already
echo run.py stopped - starting again in 10 seconds (close this window to stop)
timeout /t 10
goto loop
:already
echo.
echo The bot is already running in another window - this window will not start a second copy.
pause
