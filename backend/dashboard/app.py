# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import unicodedata
import calendar
import time
from datetime import date, timedelta
import numpy as np

# ===============================
# 📂 Ruta y columnas necesarias
# ===============================
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

COLUMNAS_NECESARIAS = [
    "fecha_inf_date", "fecha_inf", "run_fm", "nombre_corto", "run_fm_nombrecorto",
    "nom_adm", "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
    "tipo_fm", "categoria", "categoria_agrupada", "serie"
]

SINDATO = "(Sin dato)"
TODO = "(Seleccionar todo)"

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

def _to_num(s):
    return pd.to_numeric(s, errors="coerce")

def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ===============================
# 📊 Carga con barra de progreso
# ===============================
@st.cache_data
def cargar_datos():
    placeholder = st.empty()
    placeholder.info("⏳ Cargando datos, por favor espera...")

    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")

    progress = st.progress(0)
    for i in range(0, 101, 10):
        time.sleep(0.03)
        progress.progress(i)
    placeholder.empty()
    progress.empty()

    # Normalizo nombres y fechas
    df.columns = [limpiar_nombre(c) for c in df.columns]
    if "fecha_inf_date" not in df.columns and "fecha_inf" in df.columns:
        df = df.rename(columns={"fecha_inf": "fecha_inf_date"})
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"], errors="coerce")
    df["fecha_dia"] = df["fecha_inf_date"].dt.date

    # ------- Aliases mínimos -------
    if "run_fm_nombrecorto" not in df.columns and {"run_fm", "nombre_corto"}.issubset(df.columns):
        df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

    if "nombre_corto" not in df.columns:
        if "run_fm_nombrecorto" in df.columns:
            parts = df["run_fm_nombrecorto"].astype(str).str.split(" - ", n=1, expand=True)
            if parts.shape[1] == 2:
                df["nombre_corto"] = parts[1]
                if "run_fm" not in df.columns:
                    df["run_fm"] = parts[0]
            else:
                for cand in ["nombre_fondo", "nombre", "fondo"]:
                    if cand in df.columns:
                        df["nombre_corto"] = df[cand].astype(str)
                        break
        elif "nombre_fondo" in df.columns:
            df["nombre_corto"] = df["nombre_fondo"].astype(str)
        else:
            df["nombre_corto"] = ""

    if "run_fm" not in df.columns:
        if "run_fm_nombrecorto" in df.columns:
            df["run_fm"] = df["run_fm_nombrecorto"].astype(str).str.split(" - ", n=1, expand=True)[0]
        else:
            for cand in ["run", "rut_fm", "rut_fondo", "id_fondo"]:
                if cand in df.columns:
                    df["run_fm"] = df[cand].astype(str)
                    break
        if "run_fm" not in df.columns:
            df["run_fm"] = ""

    for c in ["patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm"]:
        if c in df.columns:
            df[c] = _to_num(df[c])

    col_tipo_src = _pick_col(df, [
        "tipo_de_fondo", "tipo_fm", "tipo", "tipo_de_fondo_cmf",
        "tipofm", "tipo_fondo", "tipo_de_fondos"
    ])
    if col_tipo_src is None:
        if "categoria_agrupada" in df.columns:
            df["tipo_de_fondo"] = df["categoria_agrupada"]
        elif "categoria" in df.columns:
            df["tipo_de_fondo"] = df["categoria"]
        else:
            df["tipo_de_fondo"] = SINDATO
    else:
        if col_tipo_src != "tipo_de_fondo":
            df["tipo_de_fondo"] = df[col_tipo_src]

    for c in ["categoria", "categoria_agrupada", "nom_adm", "tipo_de_fondo", "serie", "run_fm_nombrecorto"]:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype("string")
                .str.strip()
                .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            )
            df[c] = df[c].astype("category")

    return df

# ===============================
# 🚦 Carga inicial
# ===============================
if "df" not in st.session_state:
    st.session_state.datos_cargados = False
    st.session_state.df = cargar_datos()
    st.session_state.datos_cargados = True

df = st.session_state.df

# ===============================
# 🦉 Logo y título
# ===============================
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; padding-top: 10px;'>
    <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Owl_in_the_Moonlight.jpg/640px-Owl_in_the_Moonlight.jpg'
         width='60' style='border-radius: 50%; box-shadow: 0 2px 6px rgba(0,0,0,0.2);'/>
    <h1 style='margin: 0; font-size: 2.2em;'>Dashboard Fondos Mutuos</h1>
</div>
""", unsafe_allow_html=True)

# 🔗 Lanzadera AFPs
tab_afps, = st.tabs(["AFPs"])
with tab_afps:
    st.info("Para no mezclar cargas, la vista de AFPs se abre en otra pestaña.")
    st.page_link(
        "pages/08_AFPs.py",
        label="Abrir vista AFPs en otra pestaña",
        icon="📈",
        new_tab=True,
    )

st.write("Configura los filtros y presiona **Aplicar filtros** para actualizar los datos.")

# ===============================
# 📅 Filtros de fecha
# ===============================
# (aquí seguiría todo el resto de tu código tal cual lo pasaste)
