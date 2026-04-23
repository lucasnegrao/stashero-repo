#!/bin/sh
LOG_FILE="{{LOG_FILE}}"
WATCHDOG_LOG_FILE="{{WATCHDOG_LOG_FILE}}"
PYTHON_EXE="{{PYTHON_EXE}}"
WATCHDOG_SCRIPT="{{WATCHDOG_SCRIPT}}"
WATCHDOG_SERVER="{{WATCHDOG_SERVER}}"
WATCHDOG_API_KEY="{{WATCHDOG_API_KEY}}"
TRIGGER_ARGS="{{TRIGGER_ARGS}}"
CURRENT_ARGS="$*"
printf "%s %s\n" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >> "$LOG_FILE"
if [ "$CURRENT_ARGS" = "$TRIGGER_ARGS" ]; then
  printf "%s %s\n" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "[watchdog-trigger] detected sentinel ffmpeg args" >> "$LOG_FILE"
  if [ -x "$PYTHON_EXE" ] && [ -f "$WATCHDOG_SCRIPT" ]; then
    if [ -n "$WATCHDOG_API_KEY" ]; then
      "$PYTHON_EXE" "$WATCHDOG_SCRIPT" "$WATCHDOG_SERVER" --api-key "$WATCHDOG_API_KEY" --wait-seconds 2 --retries 10 >> "$WATCHDOG_LOG_FILE" 2>&1 &
    else
      "$PYTHON_EXE" "$WATCHDOG_SCRIPT" "$WATCHDOG_SERVER" --wait-seconds 2 --retries 10 >> "$WATCHDOG_LOG_FILE" 2>&1 &
    fi
  else
    printf "%s %s\n" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "[watchdog-trigger] missing python executable or startup script" >> "$LOG_FILE"
  fi
fi
exec "{{ORIGINAL_FFMPEG_PATH}}" "$@"
