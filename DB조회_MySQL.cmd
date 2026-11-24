@echo off
chcp 65001 >nul
rem MySQL 명령줄 조회 (nodeverse). 예: SHOW TABLES;  /  SELECT * FROM game_members LIMIT 10;  /  exit
cd /d "%~dp0"
wsl -u root --cd "%~dp0" -- bash -c "P=$(grep -m1 '^MYSQL_PASSWORD=' server/.env | cut -d= -f2- | tr -d '\r'); docker exec -it -e MYSQL_PWD=\"$P\" nodeverse-mysql mysql -unodeverse_app nodeverse --default-character-set=utf8mb4"
