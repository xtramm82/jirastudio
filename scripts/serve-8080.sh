#!/usr/bin/env bash
set -euo pipefail
cd /home/xtramm/.openclaw/workspace/public
exec /usr/bin/python3 -m http.server 8080 --bind 0.0.0.0
