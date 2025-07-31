# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import calendar
from datetime import date, timedelta

# ===============================
# ⚙️ Cargar solo columnas necesarias
# ===============================
columnas = [
    "fecha_inf_date", "run_fm", "nombre_corto", "run_fm_nombrecorto",
    "nom_adm", "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
    "tipo_fm", "categoria", "serie"
]
df = st.session_state.df[columnas].copy()

# Si no existe run_fm_nombrecorto, crearla una vez
if "run_fm_nombrecorto" not in df.columns:
    df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# ===============================
# 📅 Fechas únicas cacheadas
# ===============================
if "fechas_unicas" not in st.session_state:
    st.session_state.fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
    st.session_state.años_disponibles = sorted(df["fecha_inf_date"].dt.year.unique())
    st.session_state.meses_disponibles = list(calendar.month_name)[1:]

fechas_unicas = st.session_state.fechas_unicas
años_disponibles = st.session_state.años_disponibles
meses_disponibles = st.session_state.meses_disponibles

fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

# ===============================
# 📌 Selectores de fechas (rápidos)
# ===============================
col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

año_inicio = col1.selectbox("Año inicio", años_disponibles, index=0)
mes_inicio = col2.selectbox("Mes inicio", meses_disponibles, index=0)
año_fin = col3.selectbox("Año fin", años_disponibles, index=len(años_disponibles)-1)
mes_fin = col4.selectbox("Mes fin", meses_disponibles, index=len(meses_disponibles)-1)

fecha_inicio = date(año_inicio, meses_disponibles.index(mes_inicio)+1, 1)
ultimo_dia_mes_fin = calendar.monthrange(año_fin, meses_disponibles.index(mes_fin)+1)[1]
fecha_fin = date(año_fin, meses_disponibles.index(mes_fin)+1, ultimo_dia_mes_fin)

# Filtrar rápido solo por fechas para reducir filas
df = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) & (df["fecha_inf_date"].dt.date <= fecha_fin)]

# ===============================
# 🏷️ Multiselect con "Seleccionar todo"
# ===============================
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    return list(opciones) if "(Seleccionar todo)" in seleccion or not seleccion else seleccion

# ===============================
# 📌 Opciones cacheadas por columna
# ===============================
@st.cache_data
def opciones_unicas(df, columna, filtro=None):
    data = df if filtro is None else df[filtro]
    return sorted(data[columna].dropna().unique())

# ===============================
# 📌 Filtros principales
# ===============================
categoria_opciones = opciones_unicas(df, "categoria")
categorias = multiselect_con_todo("Categoría", categoria_opciones)

adm_opciones = opciones_unicas(df, "nom_adm", df["categoria"].isin(categorias))
administradoras = multiselect_con_todo("Administradora(s)", adm_opciones)

fondo_opciones = opciones_unicas(df, "run_fm_nombrecorto", df["nom_adm"].isin(administradoras))
fondos = multiselect_con_todo("Fondo(s)", fondo_opciones)

# ===============================
# 🔧 Filtros adicionales
# ===============================
with st.expander("Filtros adicionales"):
    tipo_opciones = opciones_unicas(df, "tipo_fm")
    tipos = multiselect_con_todo("Tipo de Fondo", tipo_opciones)

    serie_opciones = opciones_unicas(df, "serie", df["run_fm_nombrecorto"].isin(fondos))
    series = multiselect_con_todo("Serie(s)", serie_opciones)

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
# 📊 Aplicar filtros finales
# ===============================
df_filtrado = df[
    (df["categoria"].isin(categorias)) &
    (df["nom_adm"].isin(administradoras)) &
    (df["run_fm_nombrecorto"].isin(fondos)) &
    (df["tipo_fm"].isin(tipos)) &
    (df["serie"].isin(series)) &
    (df["fecha_inf_date"].dt.date >= rango[0]) &
    (df["fecha_inf_date"].dt.date <= rango[1])
]

st.session_state.df_filtrado = df_filtrado
st.success(f"✅ Datos filtrados: {df_filtrado.shape[0]:,} filas disponibles")
