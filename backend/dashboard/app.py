
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import random
import unicodedata
from openai import OpenAI

# -------------------------------
# 🔐 Login
# -------------------------------
USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")

if "logueado" not in st.session_state:
    st.session_state.logueado = False
if "requiere_login" not in st.session_state:
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

# -------------------------------
# 🔑 Conexión a OpenAI
# -------------------------------
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    st.error("❌ No se encontró la variable OPENAI_API_KEY.")
    st.stop()
client = OpenAI(api_key=OPENAI_KEY)

# -------------------------------
# 📂 Leer Parquet original y normalizar columnas
# -------------------------------
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

def hacer_unicas(cols):
    seen = {}
    nuevas = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            nuevas.append(c)
        else:
            seen[c] += 1
            nuevas.append(f"{c}_{seen[c]}")
    return nuevas

@st.cache_data
def cargar_datos():
    if not os.path.exists(PARQUET_PATH):
        st.error(f"❌ No se encontró el archivo Parquet en {PARQUET_PATH}")
        st.stop()
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    df.columns = hacer_unicas(df.columns)
    columnas = [c for c in [
        "fecha_inf_date", "run_fm", "nombre_corto", "nom_adm",
        "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
        "tipo_fm", "categoria", "serie"
    ] if c in df.columns]
    return df[columnas]

if "df" not in st.session_state:
    df = cargar_datos()
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
    df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)
    st.session_state.df = df

st.markdown("# 📊 Dashboard Fondos Mutuos")
st.write("Usá el menú lateral para navegar entre las páginas.")
