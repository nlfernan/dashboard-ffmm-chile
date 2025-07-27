#!/bin/bash
set -e

echo "📂 Ruta actual:"
pwd

echo "📂 Archivos en contenedor:"
ls -R

echo "🔄 Ejecutando pipeline..."
python etl/pipeline.py

echo "✅ Pipeline terminado, levantando FastAPI..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

echo "📊 Verificando existencia de dashboard/app.py..."
if [ -f dashboard/app.py ]; then
  echo "✅ Encontrado dashboard/app.py"
else
  echo "❌ No existe dashboard/app.py"
fi

echo "✅ FastAPI levantado en background, iniciando Panel..."
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*" \
    --prefix "" \
    --show-tracebacks