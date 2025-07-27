#!/bin/bash
# Arranca FastAPI y Panel en paralelo

# Levanta FastAPI en segundo plano
uvicorn backend.main:app --host 0.0.0.0 --port 8000 &

# Levanta Panel como servicio principal
panel serve backend/dashboard/app.py --address 0.0.0.0 --port $PORT --allow-websocket-origin="*"
