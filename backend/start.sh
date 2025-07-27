#!/bin/bash
set -e

echo "📂 Ruta actual:"
pwd

echo "📂 Archivos en contenedor:"
ls -R

echo "🔄 Ejecutando pipeline en background..."
python etl/pipeline.py &
PIPELINE_PID=$!
echo "✅ Pipeline corriendo en background con PID $PIPELINE_PID"

echo "📊 Verificando existencia de dashboard/app.py..."
if [ -f dashboard/app.py ]; then
  echo "✅ Encontrado dashboard/app.py"
else
  echo "❌ No existe dashboard/app.py"
fi

echo "✅ Levantando FastAPI en background..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 &
FASTAPI_PID=$!
echo "✅ FastAPI corriendo en background con PID $FASTAPI_PID"

echo "✅ Iniciando Panel..."
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*" \
    --prefix "" \
    --show-tracebacks \
    --autoreload
