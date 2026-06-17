#!/bin/sh
set -e

OPTIONS=/data/options.json
CONFIG=/data/config.json

# Apply HA options.json if present (written by HA supervisor)
if [ -f "${OPTIONS}" ]; then
    echo "[node-tester] Merging HA options into config..."
    python3 - <<'PYEOF'
import json, os, sys

options_path = os.environ.get("OPTIONS_PATH", "/data/options.json")
config_path  = os.environ.get("CONFIG_PATH",  "/data/config.json")

try:
    with open(options_path) as f:
        opts = json.load(f)
except Exception as e:
    print(f"[run.sh] Could not read options.json: {e}", file=sys.stderr)
    opts = {}

existing = {}
if os.path.exists(config_path):
    try:
        with open(config_path) as f:
            existing = json.load(f)
    except Exception:
        pass

# HA options override existing config keys
merged = {**existing, **opts}
os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
with open(config_path, "w") as f:
    json.dump(merged, f, indent=2)
print(f"[run.sh] Config saved to {config_path}")
PYEOF
fi

echo "[node-tester] Starting on 0.0.0.0:8080"
exec python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8080
