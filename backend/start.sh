#!/bin/bash
set -e
echo "🔄 Ejecutando pipeline..."
python etl/pipeline.py

echo "✅ Pipeline terminado, levantando FastAPI..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

echo "✅ FastAPI levantado en background, iniciando Panel..."
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*" \
    --prefix "" \
    --show-tracebacks
