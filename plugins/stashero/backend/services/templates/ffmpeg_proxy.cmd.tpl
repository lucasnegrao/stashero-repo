@echo off
setlocal
set "LOG_FILE={{LOG_FILE}}"
set "WATCHDOG_LOG_FILE={{WATCHDOG_LOG_FILE}}"
set "PYTHON_EXE={{PYTHON_EXE}}"
set "WATCHDOG_SCRIPT={{WATCHDOG_SCRIPT}}"
set "WATCHDOG_SERVER={{WATCHDOG_SERVER}}"
set "WATCHDOG_API_KEY={{WATCHDOG_API_KEY}}"
set "TRIGGER_ARGS={{TRIGGER_ARGS}}"
set "CURRENT_ARGS=%*"
echo %date% %time% %*>> "%LOG_FILE%"
if "%CURRENT_ARGS%"=="%TRIGGER_ARGS%" (
  echo %date% %time% [watchdog-trigger] detected sentinel ffmpeg args>> "%LOG_FILE%"
  if exist "%PYTHON_EXE%" if exist "%WATCHDOG_SCRIPT%" (
    if not "%WATCHDOG_API_KEY%"=="" (
      start "" /B "%PYTHON_EXE%" "%WATCHDOG_SCRIPT%" "%WATCHDOG_SERVER%" --api-key "%WATCHDOG_API_KEY%" --wait-seconds 2 --retries 10 >> "%WATCHDOG_LOG_FILE%" 2>&1
    ) else (
      start "" /B "%PYTHON_EXE%" "%WATCHDOG_SCRIPT%" "%WATCHDOG_SERVER%" --wait-seconds 2 --retries 10 >> "%WATCHDOG_LOG_FILE%" 2>&1
    )
  ) else (
    echo %date% %time% [watchdog-trigger] missing python or startup script>> "%LOG_FILE%"
  )
)
"{{ORIGINAL_FFMPEG_PATH}}" %*
exit /b %errorlevel%
