@echo off
setlocal EnableExtensions EnableDelayedExpansion

set PG_HOST=127.0.0.1
set PG_PORT=5432
set PYTHON_EXE=c:\tkis_stockwise\.venv\Scripts\python.exe
set WAIT_STEP_SECONDS=5
set MAX_WAIT_SECONDS=180
set /a MAX_ATTEMPTS=%MAX_WAIT_SECONDS%/%WAIT_STEP_SECONDS%
set /a ATTEMPT=0
set LOG_FILE=c:\tkis_stockwise\logs\start_services.log

if not exist "%PYTHON_EXE%" (
  echo Python not found at %PYTHON_EXE%
  >> "%LOG_FILE%" echo [%date% %time%] ERROR Python not found at %PYTHON_EXE%
  exit /b 1
)

if not exist c:\tkis_stockwise\logs (
  mkdir c:\tkis_stockwise\logs
)

>> "%LOG_FILE%" echo [%date% %time%] INFO Starting services. PG=%PG_HOST%:%PG_PORT%

:WAIT_PG
set /a ATTEMPT+=1
wsl -d Ubuntu-22.04 --exec /bin/bash -lc "echo > /dev/tcp/%PG_HOST%/%PG_PORT%" >nul 2>&1
if not "%ERRORLEVEL%"=="0" (
  if !ATTEMPT! GEQ !MAX_ATTEMPTS! (
    >> "%LOG_FILE%" echo [%date% %time%] ERROR Postgres not reachable after !MAX_WAIT_SECONDS!s. Exiting.
    exit /b 1
  )
  >> "%LOG_FILE%" echo [%date% %time%] WARN Waiting for Postgres... attempt !ATTEMPT!/!MAX_ATTEMPTS!
  timeout /t %WAIT_STEP_SECONDS% /nobreak >nul
  goto WAIT_PG
)

>> "%LOG_FILE%" echo [%date% %time%] INFO Postgres reachable. Launching backend and ETL.
start "" /b /d c:\tkis_stockwise\backend "%PYTHON_EXE%" -m uvicorn main:app --host 127.0.0.1 --port 8000 > c:\tkis_stockwise\logs\uvicorn.log 2>&1
start "" /b /d c:\tkis_stockwise\etl "%PYTHON_EXE%" raw_sync.py > c:\tkis_stockwise\logs\raw_sync.log 2>&1

endlocal
