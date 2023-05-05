#!/bin/bash

# Find the process ID of Servx
PID=$(ps aux | grep concrr | grep -v grep | awk '{print $2}')

# Kill the process if it's running
if [ -n "$PID" ]; then
  echo "Killing concrr (PID $PID)"
  kill $PID
else
  echo "concrr is not running"
fi

