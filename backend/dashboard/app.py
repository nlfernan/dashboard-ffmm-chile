# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import unicodedata
import calendar
from datetime import date, timedelta
from openai import OpenAI

# ===============================
# 📂 Cargar parquet y normalizar
# ===============================
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
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    df.columns = hacer_unicas(df.columns)
    columnas = [c for c in [
        "fecha_inf_date", "run_fm", "nombre_corto", "run_fm_nombrecorto",
        "nom_adm", "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
        "tipo_fm", "categoria", "serie"
    ] if c in df.columns]
    df = df[columnas]

    # Convertir fechas y agregar columna optimizada de día
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
    df["fecha_dia"] = df["fecha_inf_date"].dt.date

    # Generar run_fm_nombrecorto si no existe
    if "run_fm_nombrecorto" not in df.columns:
        df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

    # Optimizar columnas categóricas
    for col in ["categoria", "nom_adm", "tipo_fm", "serie", "run_fm_nombrecorto"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    return df

if "df" not in st.session_state:
    st.session_state.df = cargar_datos()

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

st.write("Configura los filtros y navega por las pestañas para ver los resultados.")

# ===============================
# 📅 Filtros de fecha
# ===============================
fechas_unicas = sorted(df["fecha_dia"].unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted(pd.Series(fechas_unicas).dt.year.unique())
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

df = df[(df["fecha_dia"] >= fecha_inicio) & (df["fecha_dia"] <= fecha_fin)]

# ===============================
# 🏷️ Multiselect con "Seleccionar todo"
# ===============================
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    return list(opciones) if "(Seleccionar todo)" in seleccion or not seleccion else seleccion

# ===============================
# 📌 Cache de opciones únicas
# ===============================
@st.cache_data
def opciones_unicas(df, columna):
    return sorted(df[columna].dropna().unique())

categorias = multiselect_con_todo("Categoría", opciones_unicas(df, "categoria"))
administradoras = multiselect_con_todo("Administradora(s)", opciones_unicas(df[df["categoria"].isin(categorias)], "nom_adm"))
fondos = multiselect_con_todo("Fondo(s)", opciones_unicas(df[df["nom_adm"].isin(administradoras)], "run_fm_nombrecorto"))

with st.expander("Filtros adicionales"):
    tipos = multiselect_con_todo("Tipo de Fondo", opciones_unicas(df, "tipo_fm"))
    series = multiselect_con_todo("Serie(s)", opciones_unicas(df[df["run_fm_nombrecorto"].isin(fondos)], "serie"))

    st.markdown("#### Ajuste fino de fechas")
    if "rango_fechas" not in st.session_state:
        st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    hoy = fecha_max_real
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=30), fecha_min_real), hoy)
    if col_b.button("3M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=90), fecha_min_real), hoy)
    if col_c.button("6M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=180), fecha_min_real), hoy)
    if col_d.button("MTD"): st.session_state["rango_fechas"] = (date(hoy.year, hoy.month, 1), hoy)
    if col_e.button("YTD"): st.session_state["rango_fechas"] = (date(hoy.year, 1, 1), hoy)

rango = st.session_state["rango_fechas"]

# ===============================
# ✅ Botón aplicar filtros
# ===============================
if st.button("Aplicar filtros"):
    df_filtrado = df[
        (df["categoria"].isin(categorias)) &
        (df["nom_adm"].isin(administradoras)) &
        (df["run_fm_nombrecorto"].isin(fondos)) &
        (df["tipo_fm"].isin(tipos)) &
        (df["serie"].isin(series)) &
        (df["fecha_dia"] >= rango[0]) &
        (df["fecha_dia"] <= rango[1])
    ]
    st.session_state.df_filtrado = df_filtrado
    st.success(f"✅ Datos filtrados: {df_filtrado.shape[0]:,} filas disponibles")
elif "df_filtrado" in st.session_state:
    st.info(f"ℹ️ Usando datos filtrados previamente: {st.session_state.df_filtrado.shape[0]:,} filas")
else:
    st.warning("🔎 Configura los filtros y presiona **Aplicar filtros** para ver datos")
