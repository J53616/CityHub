@echo off
REM ============================================================
REM  CityHub 一键启动脚本
REM  启动顺序: MySQL -> Redis -> RocketMQ -> Spring Boot 应用
REM  停止请运行 stop-cityhub.bat
REM ============================================================
setlocal enabledelayedexpansion

echo ============================================
echo   CityHub 本地生活服务平台 - 一键启动
echo ============================================

REM ---------- 路径配置（按需修改） ----------
set "MYSQL_BASE=D:\dev\mysql\mysql-8.0.28-winx64"
set "MYSQL_DATA=D:\dev\mysql\cityhub-data"
set "MYSQL_PWD=290390"
set "REDIS_PWD=9b09b97b22181283"
set "ROCKETMQ_HOME=D:\rocketmq\rocketmq-all-5.1.4-bin-release"
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.6.7-hotspot"

REM ---------- 1. 启动 MySQL ----------
echo.
echo [1/4] 启动 MySQL ...
tasklist /FI "IMAGENAME eq mysqld.exe" 2>nul | find /i "mysqld.exe" >nul
if !errorlevel!==0 (
    echo       MySQL 已在运行，跳过
) else (
    start "CityHub-MySQL" "%MYSQL_BASE%\bin\mysqld.exe" --console --basedir="%MYSQL_BASE%" --datadir="%MYSQL_DATA%"
    echo       等待 MySQL 就绪 ...
    timeout /t 8 /nobreak >nul
    netstat -ano | findstr ":3306" | findstr "LISTENING" >nul
    if !errorlevel!==0 ( echo       MySQL 启动成功 ) else ( echo       MySQL 启动失败! 请检查端口占用 )
)

REM ---------- 2. 启动 Redis (Docker) ----------
echo.
echo [2/4] 启动 Redis (Docker) ...
docker ps --filter "name=cityhub-redis" --format "{{.Names}}" | findstr "cityhub-redis" >nul
if !errorlevel!==0 (
    echo       Redis 容器已在运行
) else (
    docker start cityhub-redis >nul 2>&1
    if !errorlevel!==0 (
        echo       容器不存在，创建新容器 ...
        docker run -d --name cityhub-redis -p 6379:6379 redis:7 redis-server --requirepass %REDIS_PWD% >nul 2>&1
    )
    timeout /t 3 /nobreak >nul
    docker ps --filter "name=cityhub-redis" --format "{{.Status}}" | findstr "Up" >nul
    if !errorlevel!==0 ( echo       Redis 启动成功 ) else ( echo       Redis 启动失败! 请检查 Docker )
)

REM ---------- 3. 启动 RocketMQ ----------
echo.
echo [3/4] 启动 RocketMQ ...
REM 先设置环境变量（开两个独立窗口，避免互相影响）
set "ROCKETMQ_HOME=%ROCKETMQ_HOME%"
start "CityHub-NameServer" cmd /k "set ROCKETMQ_HOME=%ROCKETMQ_HOME% && cd /d %ROCKETMQ_HOME%\bin && mqnamesrv.cmd"
timeout /t 6 /nobreak >nul
start "CityHub-Broker" cmd /k "set ROCKETMQ_HOME=%ROCKETMQ_HOME% && cd /d %ROCKETMQ_HOME%\bin && mqbroker.cmd -n 127.0.0.1:9876"
timeout /t 6 /nobreak >nul
netstat -ano | findstr ":9876" | findstr "LISTENING" >nul
if %errorlevel%==0 ( echo       NameServer 已启动 (9876) ) else ( echo       NameServer 可能未就绪, 请查看窗口日志 )
netstat -ano | findstr ":10911" | findstr "LISTENING" >nul
if %errorlevel%==0 ( echo       Broker 已启动 (10911) ) else ( echo       Broker 可能未就绪, 请查看窗口日志 )

REM ---------- 4. 启动 Spring Boot 应用 ----------
echo.
echo [4/4] 启动 Spring Boot 应用 ...
REM 优先用系统 Maven（PATH 里的），否则退回 IDEA 内置 Maven
where mvn >nul 2>&1
if %errorlevel%==0 (
    set "MAVEN_CMD=mvn"
) else (
    set "MAVEN_CMD=C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2024.3.5\plugins\maven\lib\maven3\bin\mvn.cmd"
)
REM 用项目内干净的 settings（本机 settings 有 blocked 镜像会拦编译）
cd /d "%~dp0"
start "CityHub-App" cmd /k "cd /d ""%~dp0"" && set JAVA_HOME=%JAVA_HOME% && call ""%MAVEN_CMD%"" -s ""%~dp0maven-settings.xml"" spring-boot:run"
echo       应用启动中, 请稍候访问 http://localhost:8081 (启动需 5-10 秒)

echo.
echo ============================================
echo   全部组件已启动!
echo   访问地址: http://localhost:8081
echo   停止服务: 运行 stop-cityhub.bat
echo ============================================
timeout /t 3 >nul
endlocal
