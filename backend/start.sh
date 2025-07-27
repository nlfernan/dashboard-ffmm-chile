#!/bin/bash
# 🔄 Ejecuta el pipeline antes de arrancar FastAPI y Panel

echo "🔄 Ejecutando pipeline para asegurar que fondos_mutuos existe..."
python etl/pipeline.py

# 🚀 Levanta FastAPI en segundo plano en el puerto 8000
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

# 📊 Levanta Panel como servicio principal en el puerto asignado por Railway
panel serve dashboard/app_dashboard.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*"