import panel as pn
import pandas as pd
from sqlalchemy import create_engine
import os

pn.extension()

# Conexión a PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Consulta mínima para probar conexión
try:
    df = pd.read_sql("SELECT COUNT(*) as total_filas FROM fondos_mutuos;", engine)
    mensaje = f"✅ Conexión OK. Total de filas en fondos_mutuos: {df['total_filas'].iloc[0]}"
except Exception as e:
    mensaje = f"❌ Error conectando a la base de datos: {e}"

# Layout básico
dashboard = pn.Column(
    "# 🚀 Dashboard FFMM Chile",
    pn.pane.Markdown(mensaje)
)

dashboard.servable()
