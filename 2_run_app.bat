@echo off
cd /d "%~dp0"
title Urethane WIP Tool - Run

echo =======================================
echo Urethane WIP Tool is starting.
echo Wait for the browser window to open.
echo To stop, press Ctrl + C in this window.
echo =======================================
echo.

python -m streamlit run app.py

echo.
echo App stopped.
pause
