
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os, unicodedata, calendar
from datetime import date, timedelta

# 🔐 Login
USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")
if "logueado" not in st.session_state:
    st.session_state.logueado = False
if "requiere_login" not in st.session_state:
    import random
    st.session_state.requiere_login = random.randint(1, 3) == 1

if st.session_state.requiere_login and not st.session_state.logueado:
    st.title("🔐 Acceso al Dashboard")
    usuario = st.text_input("Usuario")
    clave = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if usuario == USER and clave == PASS:
            st.session_state.logueado = True
            st.success("✅ Acceso concedido. Cargando dashboard...")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")
    st.stop()

# 📂 Cargar Parquet
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"
def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

@st.cache_data
def cargar_datos():
    if not os.path.exists(PARQUET_PATH):
        st.error(f"❌ No se encontró el archivo {PARQUET_PATH}")
        st.stop()
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    return df

df = cargar_datos()
df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])

st.title("Dashboard Fondos Mutuos")

# 📅 Rango de Fechas
fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
años_disponibles = sorted(df["fecha_inf_date"].dt.year.unique())
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

df = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) & (df["fecha_inf_date"].dt.date <= fecha_fin)]

# Guardar en session_state
st.session_state["df_filtrado"] = df

st.success(f"Datos filtrados: {df.shape[0]} filas entre {fecha_inicio} y {fecha_fin}")
