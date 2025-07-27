# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
from datetime import date, timedelta
from sqlalchemy import create_engine
from openai import OpenAI

# -------------------------------
# 🔑 Test de conexión OpenAI
# -------------------------------
st.sidebar.header("🔑 Test de OpenAI")
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

if st.sidebar.button("Probar conexión IA"):
    with st.spinner("Consultando OpenAI..."):
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Escribe: Hola, funciona la API"}]
        )
    st.sidebar.success(resp.choices[0].message.content)

# -------------------------------
# Conexión a PostgreSQL
# -------------------------------
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    st.error("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")
    st.stop()

engine = create_engine(DB_URL)

# -------------------------------
# Función cacheada para cargar datos desde la DB
# -------------------------------
@st.cache_data
def cargar_datos_db():
    query = """
    SELECT 
        fecha_inf_date, run_fm, nombre_corto, nom_adm, serie,
        patrimonio_neto_mm, venta_neta_mm, tipo_fm, categoria
    FROM fondos_mutuos;
    """
    return pd.read_sql(query, engine)

@st.cache_data
def obtener_rango_fechas(df):
    años = sorted(df["fecha_inf_date"].dt.year.unique())
    meses = list(calendar.month_name)[1:]
    return años, meses

try:
    df = cargar_datos_db()
except Exception as e:
    st.error(f"❌ Error al leer la base de datos: {e}")
    st.stop()

# -------------------------------
# Preprocesamiento
# -------------------------------
df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# -------------------------------
# Título con logo
# -------------------------------
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; padding-top: 10px;'>
    <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Owl_in_the_Moonlight.jpg/640px-Owl_in_the_Moonlight.jpg'
         width='60' style='border-radius: 50%; box-shadow: 0 2px 6px rgba(0,0,0,0.2);'/>
    <h1 style='margin: 0; font-size: 2.2em;'>Dashboard Fondos Mutuos</h1>
</div>
""", unsafe_allow_html=True)

# -------------------------------
# (El resto de tu código del dashboard sigue igual)
# -------------------------------
