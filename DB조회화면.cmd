@echo off
chcp 65001 >nul
cd /d "%~dp0"
wsl -u root --cd "%~dp0" -- bash scripts/local/dbui.sh
if errorlevel 1 (pause & exit /b 1)
start "" "http://127.0.0.1:8081"
start "" "http://127.0.0.1:8082"
