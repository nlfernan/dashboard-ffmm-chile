
	
	#!/bin/bash
set -e

echo "📂 Ruta actual:"
pwd
echo "📂 Archivos en contenedor:"
ls -R

echo "🔍 Verificando instalación de Panel y Bokeh..."
python -c "import panel as pn; import bokeh; print('✅ Panel', pn.__version__, '✅ Bokeh', bokeh.__version__)"

echo "🔄 Ejecutando pipeline en background..."
python etl/pipeline.py &
PIPELINE_PID=$!
echo "✅ Pipeline corriendo en background con PID $PIPELINE_PID"

echo "✅ Levantando FastAPI en background..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 &

echo "✅ Iniciando Panel..."
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*" \
    --prefix "" 
