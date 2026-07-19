#!/bin/bash
# Inject live trading monitor snapshot when a new Agent session starts.
set -euo pipefail
LIVE="robinhood_agentic/monitor/LIVE_STATUS.md"
if [[ ! -f "$LIVE" ]]; then
  exit 0
fi
# sessionStart: pass context to the agent (fail open if format unsupported)
CONTENT=$(head -40 "$LIVE" | sed 's/"/\\"/g' | tr '\n' ' ')
if command -v jq >/dev/null 2>&1; then
  jq -n --arg ctx "Trading monitor active. Latest snapshot:\n\n$(cat "$LIVE")" \
    '{ "additional_context": $ctx }'
else
  echo "{\"additional_context\": \"Trading monitor: see robinhood_agentic/monitor/LIVE_STATUS.md\"}"
fi
exit 0
