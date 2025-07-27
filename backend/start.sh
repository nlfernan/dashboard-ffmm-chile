#!/bin/bash
# 🔄 Ejecuta el pipeline
echo "🔄 Ejecutando pipeline para asegurar que fondos_mutuos existe..."
python etl/pipeline.py

# 🚀 Levanta FastAPI desde /app/app/main.py en segundo plano
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

# 📊 Levanta Panel desde /app/dashboard/app.py como servicio principal
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*"