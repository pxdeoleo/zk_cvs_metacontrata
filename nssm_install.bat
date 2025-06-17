@echo off
setlocal

REM === CONFIGURATION ===
set SERVICE_NAME=MetaCVSync
set SERVICE_DISPLAY_NAME=MetaContrata-CVSecurity Sync Service
set EXECUTABLE_PATH=%~dp0dist\service.exe
set STARTUP_DIR=%~dp0
set NSSM_EXE=%~dp0nssm.exe
set LOG_DIR=%~dp0logs

REM === VALIDATION ===
if not exist "%NSSM_EXE%" (
    echo ❌ NSSM executable not found: %NSSM_EXE%
    pause
    exit /b 1
)

if not exist "%EXECUTABLE_PATH%" (
    echo ❌ Compiled service.exe not found: %EXECUTABLE_PATH%
    pause
    exit /b 1
)

if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

REM === REMOVE OLD SERVICE IF EXISTS ===
echo Removing existing service (if it exists)...
"%NSSM_EXE%" stop "%SERVICE_NAME%" >nul 2>&1
"%NSSM_EXE%" remove "%SERVICE_NAME%" confirm >nul 2>&1

REM === INSTALL SERVICE ===
echo ✅ Installing service "%SERVICE_NAME%"...
"%NSSM_EXE%" install "%SERVICE_NAME%" "%EXECUTABLE_PATH%"
"%NSSM_EXE%" set "%SERVICE_NAME%" DisplayName "%SERVICE_DISPLAY_NAME%"
"%NSSM_EXE%" set "%SERVICE_NAME%" AppDirectory "%STARTUP_DIR%"
"%NSSM_EXE%" set "%SERVICE_NAME%" Start SERVICE_AUTO_START
"%NSSM_EXE%" set "%SERVICE_NAME%" AppStdout "%LOG_DIR%\stdout.log"
"%NSSM_EXE%" set "%SERVICE_NAME%" AppStderr "%LOG_DIR%\stderr.log"
"%NSSM_EXE%" set "%SERVICE_NAME%" AppRotateFiles 1

REM === START SERVICE ===
echo Starting service...
net start "%SERVICE_NAME%"

echo Service "%SERVICE_NAME%" installed and started successfully.
pause
endlocal
