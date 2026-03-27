@echo off
title M8BF Live Pipeline
cd /d "C:\Users\rakhm\brave-trading\scraper"
set PYTHONIOENCODING=utf-8
echo Starting M8BF pipeline...
echo Log in to Discord in the Chrome window that opens.
echo.
python live_updater.py
pause
