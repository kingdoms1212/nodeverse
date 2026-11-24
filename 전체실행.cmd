@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
echo ============================================================
echo   NODEVERSE 로컬 전체 실행 (파이프라인 + MySQL + Django)
echo ============================================================
wsl -u root --cd "%~dp0" -- bash scripts/local/up.sh "%APPDATA%"
if errorlevel 1 (
  echo.
  echo [오류] 실행 중 문제가 생겼습니다. 위 메시지를 확인하세요.
  pause
  exit /b 1
)
echo.
start "" "http://127.0.0.1:8010/staff/login/"
choice /c YN /t 15 /d N /m "게임도 실행할까요? (15초 후 자동으로 N)"
if errorlevel 2 goto :end
call "%~dp0game\play_game.bat"
:end
endlocal
