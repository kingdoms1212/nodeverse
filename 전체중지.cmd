@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo NODEVERSE 로컬 전체 중지 (데이터는 지우지 않습니다)
wsl -u root --cd "%~dp0" -- bash scripts/local/down.sh
pause
