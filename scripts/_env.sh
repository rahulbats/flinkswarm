# sourced by the other scripts — resolve project root, python, and a banner
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"
[ -x "$PY" ] || PY="python3"

banner() {  # banner "TITLE" "subtitle"
  printf '\n\033[1;36m┌─ %s\033[0m\n\033[2m│  %s\033[0m\n\033[1;36m└─────────────────────────────\033[0m\n' "$1" "$2"
}
