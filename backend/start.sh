#!/bin/bash
# Ejecuta el pipeline antes de arrancar FastAPI y Panel

echo "🔄 Ejecutando pipeline para asegurar que fondos_mutuos existe..."
python etl/pipeline.py

# Levanta FastAPI en segundo plano
uvicorn main:app --host 0.0.0.0 --port 8000 &

# Levanta Panel como servicio principal
panel serve dashboard/app.py --address 0.0.0.0 --port $PORT --allow-websocket-origin="*"
