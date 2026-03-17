@echo off
cd /d "%~dp0"
title Urethane WIP Tool - Setup

echo =======================================
echo Urethane WIP Tool setup starts now.
echo Please wait until installation ends.
echo =======================================
echo.

python -m pip install --upgrade pip
if errorlevel 1 goto error

python -m pip install -r requirements.txt
if errorlevel 1 goto error

echo.
echo =======================================
echo Setup finished successfully.
echo Now double-click 2_run_app.bat
echo =======================================
pause
exit /b 0

:error
echo.
echo =======================================
echo Setup failed.
echo Please send me a screenshot of this window.
echo =======================================
pause
exit /b 1
