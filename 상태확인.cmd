@echo off
chcp 65001 >nul
cd /d "%~dp0"
wsl -u root --cd "%~dp0" -- bash scripts/local/status.sh
pause
