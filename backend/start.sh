#!/bin/bash
echo "📂 Carpeta actual:"
pwd
echo "📂 Listando archivos y carpetas:"
ls -R

echo "🔄 Ejecutando pipeline..."
python etl/pipeline.py

echo "🚀 Levantando FastAPI..."
uvicorn main:app --host 0.0.0.0 --port 8000 &

echo "📊 Levantando Dashboard Panel..."
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*"