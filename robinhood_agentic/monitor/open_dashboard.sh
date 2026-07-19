#!/bin/bash
# Open auto-refreshing live dashboard in default browser.
DIR="$(cd "$(dirname "$0")" && pwd)"
HTML="$DIR/dashboard.html"
if [[ ! -f "$HTML" ]]; then
  echo "Run session_monitor.py first to generate dashboard.html"
  exit 1
fi
open "$HTML" 2>/dev/null || xdg-open "$HTML" 2>/dev/null || echo "Open file://$HTML"
