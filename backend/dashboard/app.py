# app.py
import streamlit as st
import pandas as pd
import os
import unicodedata
import calendar
from datetime import date, timedelta

# -------------------------------
# Carga de datos
# -------------------------------
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

@st.cache_data
def cargar_datos():
    if not os.path.exists(PARQUET_PATH):
        st.error(f"❌ No se encontró el archivo Parquet en {PARQUET_PATH}")
        st.stop()
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    return df

df = cargar_datos()

# -------------------------------
# Filtros globales
# -------------------------------
st.title("Dashboard Fondos Mutuos")
st.markdown("Selecciona el rango de fechas y filtros globales. Los resultados se aplicarán a todas las páginas.")

fechas_unicas = sorted(pd.to_datetime(df["fecha_inf_date"]).dt.date.unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted(pd.to_datetime(df["fecha_inf_date"]).dt.year.unique())
meses_disponibles = list(calendar.month_name)[1:]

col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

año_inicio = col1.selectbox("Año inicio", años_disponibles, index=0)
mes_inicio = col2.selectbox("Mes inicio", meses_disponibles, index=0)
año_fin = col3.selectbox("Año fin", años_disponibles, index=len(años_disponibles)-1)
mes_fin = col4.selectbox("Mes fin", meses_disponibles, index=len(meses_disponibles)-1)

fecha_inicio = date(año_inicio, meses_disponibles.index(mes_inicio)+1, 1)
ultimo_dia_mes_fin = calendar.monthrange(año_fin, meses_disponibles.index(mes_fin)+1)[1]
fecha_fin = date(año_fin, meses_disponibles.index(mes_fin)+1, ultimo_dia_mes_fin)

df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
df_filtrado = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) & (df["fecha_inf_date"].dt.date <= fecha_fin)]

# Guardar en session_state para heredar entre páginas
st.session_state["df_filtrado"] = df_filtrado

st.success(f"Datos cargados: {df_filtrado.shape[0]} filas entre {fecha_inicio} y {fecha_fin}")
