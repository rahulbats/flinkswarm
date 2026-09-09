#!/usr/bin/env bash
# Regenerate agentic-ai-workflows.pdf from deck.html (one slide per page, 16:9).
#   diagrams/make-pdf.sh
set -euo pipefail
cd "$(dirname "$0")"

CHROME="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
[ -x "$CHROME" ] || CHROME="$(command -v google-chrome || command -v chromium || true)"
[ -n "$CHROME" ] || { echo "Chrome not found — set CHROME=/path/to/chrome"; exit 1; }

SRC="deck.html"
OUT="agentic-ai-workflows.pdf"
TMP="$(mktemp -t deckpdf).html"
trap 'rm -f "$TMP"' EXIT

# deck.html is an artifact fragment (no <html>/<head>/<body>) — wrap it so a
# plain browser renders it the same way the hosted artifact does.
{
  printf '%s' '<!doctype html><html><head><meta charset="utf-8">'
  printf '%s' '<meta name="viewport" content="width=device-width,initial-scale=1">'
  printf '%s' '<style>body{margin:0}img{max-width:100%}[hidden]{display:none!important}</style></head><body>'
  cat "$SRC"
  printf '%s' '</body></html>'
} > "$TMP"

"$CHROME" --headless=new --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="$OUT" --print-to-pdf-no-header "file://$TMP" 2>/dev/null

sleep 1
echo "wrote $(pwd)/$OUT"
