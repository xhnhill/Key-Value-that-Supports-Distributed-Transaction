#!/bin/bash

# Find the process ID of Servx
PID=$(ps aux | grep Servx | grep -v grep | awk '{print $2}')

# Kill the process if it's running
if [ -n "$PID" ]; then
  echo "Killing Servx (PID $PID)"
  kill $PID
else
  echo "Servx is not running"
fi