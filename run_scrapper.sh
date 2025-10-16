#!/bin/bash

# Configuration
SCRIPT_DIR="/home/user1/startups/news-scrapper"
VENV_PATH="$SCRIPT_DIR/env"
PYTHON_SCRIPT="main.py"
LOG_DIR="$SCRIPT_DIR/log"
LOG_FILE="$LOG_DIR/main.log"
PIDFILE="$SCRIPT_DIR/news_scrapper.pid"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Clean up large log files (>100MB)
find "$LOG_DIR" -name "*.log" -size +100M -delete

# Function to kill previous instance of this specific script
kill_previous_instance() {
    if [ -f "$PIDFILE" ]; then
        local old_pid=$(cat "$PIDFILE")
        if ps -p "$old_pid" > /dev/null 2>&1; then
            # Check if it's actually our Python script
            if ps -p "$old_pid" -o cmd= | grep -q "$PYTHON_SCRIPT"; then
                echo "$(date): Killing previous instance (PID: $old_pid)" >> "$LOG_FILE"
                kill "$old_pid"
                sleep 2
                # Force kill if still running
                if ps -p "$old_pid" > /dev/null 2>&1; then
                    kill -9 "$old_pid"
                fi
            fi
        fi
        rm -f "$PIDFILE"
    fi
}

# Kill any previous instance
kill_previous_instance

# Change to script directory
cd "$SCRIPT_DIR" || {
    echo "$(date): ERROR - Cannot change to directory $SCRIPT_DIR" >> "$LOG_FILE"
    exit 1
}

# Check if virtual environment exists
if [ ! -f "$VENV_PATH/bin/python" ]; then
    echo "$(date): ERROR - Virtual environment not found at $VENV_PATH" >> "$LOG_FILE"
    exit 1
fi

# Check if Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "$(date): ERROR - Python script $PYTHON_SCRIPT not found" >> "$LOG_FILE"
    exit 1
fi

# Log start time
echo "$(date): Starting news scrapper from $(pwd)" >> "$LOG_FILE"

# Run the Python script in background and save PID
"$VENV_PATH/bin/python" "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1 &
SCRIPT_PID=$!

# Save PID to file
echo "$SCRIPT_PID" > "$PIDFILE"

# Log the PID
echo "$(date): Started with PID: $SCRIPT_PID" >> "$LOG_FILE"

# Wait for the process to complete and clean up
wait "$SCRIPT_PID"
EXIT_CODE=$?

# Clean up PID file
rm -f "$PIDFILE"

# Log completion
echo "$(date): Script completed with exit code: $EXIT_CODE" >> "$LOG_FILE"

exit $EXIT_CODE