#!/bin/bash

# Root directory (where this script resides)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Use Python from env (e.g., ./env/bin/python)
PYTHON="$ROOT_DIR/env/bin/python"

# Activate virtual environment if needed
if [ ! -x "$PYTHON" ]; then
    echo "Python environment not found at: $PYTHON"
    exit 1
fi

# Create log directory if not exists
mkdir -p "$ROOT_DIR/log"

# Run link_scrapper
cd "$ROOT_DIR/link_scrapper"
pwd
$PYTHON main.py >> "$ROOT_DIR/log/link_scrapper.log" 2>&1

# Run news_scrapper
cd "$ROOT_DIR/news_scrapper"
pwd
$PYTHON main.py >> "$ROOT_DIR/log/news_scrapper.log" 2>&1
