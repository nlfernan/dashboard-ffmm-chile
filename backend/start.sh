#!/bin/bash
echo "🚀 Iniciando Dashboard de Fondos Mutuos con Streamlit..."

# 👇 Si querés ejecutar el pipeline manualmente, descomentá la línea siguiente
python etl/pipeline.py

# Levantar Streamlit
streamlit run dashboard/app.py --server.port $PORT --server.address 0.0.0.0
