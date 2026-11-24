@echo off
chcp 65001 >nul
rem MongoDB 명령줄 조회 (nodeverse_logs). 예: db.rt_summary.findOne()  /  show collections  /  exit
wsl -u root -- docker exec -it nodeverse-mongo-1 mongosh nodeverse_logs
