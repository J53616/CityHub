@echo off
REM ============================================================
REM  CityHub 一键停止脚本
REM  停止顺序: 应用 -> RocketMQ -> Redis -> MySQL
REM  内部用 PowerShell 按端口查 PID，可靠且不误杀其他进程
REM ============================================================
echo ============================================
echo   CityHub - 停止所有服务
echo ============================================

echo.
echo [1/4] 停止 Spring Boot 应用 (8081) ...
powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }"
timeout /t 1 /nobreak >nul
netstat -ano | findstr ":8081" | findstr "LISTENING" >nul
if errorlevel 1 ( echo       应用已停止 ) else ( echo       应用仍在运行, 请手动关闭 )

echo.
echo [2/4] 停止 RocketMQ (9876 / 10911) ...
powershell -NoProfile -Command "foreach ($p in 9876,10911) { Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue } }"
timeout /t 1 /nobreak >nul
for %%p in (9876 10911) do (
    netstat -ano | findstr ":%%p" | findstr "LISTENING" >nul
    if errorlevel 1 ( echo       %%p 已停止 ) else ( echo       %%p 仍在运行, 请手动关闭 )
)

echo.
echo [3/4] 停止 Redis (Docker) ...
docker ps --filter "name=cityhub-redis" --format "{{.Names}}" | findstr "cityhub-redis" >nul
if errorlevel 1 ( echo       Redis 未在运行 ) else (
    docker stop cityhub-redis >nul 2>&1
    echo       Redis 已停止
)

echo.
echo [4/4] 停止 MySQL ...
taskkill /F /IM mysqld.exe >nul 2>&1
timeout /t 1 /nobreak >nul
netstat -ano | findstr ":3306" | findstr "LISTENING" >nul
if errorlevel 1 ( echo       MySQL 已停止 ) else ( echo       MySQL 仍在运行, 请手动关闭 )

echo.
echo ============================================
echo   停止完成!
echo ============================================
timeout /t 2 /nobreak >nul
