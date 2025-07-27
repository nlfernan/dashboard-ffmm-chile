#!/bin/bash
# 🔄 Ejecuta el pipeline dentro de backend/
echo "🔄 Ejecutando pipeline para asegurar que fondos_mutuos existe..."
python etl/pipeline.py

# 🚀 Levanta FastAPI desde main.py dentro de backend/
uvicorn main:app --host 0.0.0.0 --port 8000 &

# 📊 Levanta Panel desde dashboard/app.py dentro de backend/
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*"