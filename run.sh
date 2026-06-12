#!/usr/bin/env bash
# Khởi động AIOS Hub (server + dashboard). Demo chạy ngay ở MOCK mode.
set -e
cd "$(dirname "$0")"

# venv
if [ ! -d ".venv" ]; then
  echo "→ Tạo virtualenv .venv ..."
  python3 -m venv .venv
fi
source .venv/bin/activate

# deps core
echo "→ Cài dependencies core ..."
pip install -q -r requirements.txt

# env mặc định cho demo nếu chưa có .env
[ -f .env ] || { echo "→ Chưa có .env, tạo từ .env.example"; cp .env.example .env; }

PORT="${AIOS_PORT:-8800}"
echo "→ AIOS Hub: http://localhost:${PORT}/"
exec uvicorn aios_server:app --host 0.0.0.0 --port "${PORT}"
