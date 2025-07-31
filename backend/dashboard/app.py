
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
if "nombre_corto" in df.columns and "run_fm" in df.columns:
    df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

st.title("Dashboard Fondos Mutuos")

# 📅 Rango de Fechas
fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

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

# ✅ Multiselect con "Seleccionar todo"
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion or not seleccion:
        return list(opciones)
    else:
        return seleccion

# ✅ Filtro Categoría agregada
if "categoria_agregada" in df.columns:
    categoria_agregada_opciones = sorted(df["categoria_agregada"].dropna().unique())
    categoria_agregada_sel = multiselect_con_todo("Categoría agregada", categoria_agregada_opciones)
    df = df[df["categoria_agregada"].isin(categoria_agregada_sel)]

# ✅ Filtro Categoría AFM
if "categoria" in df.columns:
    categoria_opciones = sorted(df["categoria"].dropna().unique())
    categoria_sel = multiselect_con_todo("Categoría", categoria_opciones)
    df = df[df["categoria"].isin(categoria_sel)]

# ✅ Filtro Administradora
if "nom_adm" in df.columns:
    adm_opciones = sorted(df["nom_adm"].dropna().unique())
    adm_sel = multiselect_con_todo("Administradora(s)", adm_opciones)
    df = df[df["nom_adm"].isin(adm_sel)]

# ✅ Filtro Fondo
if "run_fm_nombrecorto" in df.columns:
    fondo_opciones = sorted(df["run_fm_nombrecorto"].dropna().unique())
    fondo_sel = multiselect_con_todo("Fondo(s)", fondo_opciones)
    df = df[df["run_fm_nombrecorto"].isin(fondo_sel)]

# ✅ Filtros adicionales: tipo y serie
with st.expander("🔧 Filtros adicionales"):
    if "tipo_fm" in df.columns:
        tipo_opciones = sorted(df["tipo_fm"].dropna().unique())
        tipo_sel = multiselect_con_todo("Tipo de Fondo", tipo_opciones)
        df = df[df["tipo_fm"].isin(tipo_sel)]
    if "serie" in df.columns:
        serie_opciones = sorted(df["serie"].dropna().unique())
        serie_sel = multiselect_con_todo("Serie(s)", serie_opciones)
        df = df[df["serie"].isin(serie_sel)]

    # Slider de rango exacto
    if "rango_fechas" not in st.session_state:
        st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    hoy_df = fecha_max_real
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=30), fecha_min_real), hoy_df)
    if col_b.button("3M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=90), fecha_min_real), hoy_df)
    if col_c.button("6M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=180), fecha_min_real), hoy_df)
    if col_d.button("MTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, hoy_df.month, 1), hoy_df)
    if col_e.button("YTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, 1, 1), hoy_df)

# Aplicar rango exacto final
rango_fechas = st.session_state["rango_fechas"]
df = df[(df["fecha_inf_date"].dt.date >= rango_fechas[0]) & (df["fecha_inf_date"].dt.date <= rango_fechas[1])]

# Guardar en session_state
st.session_state["df_filtrado"] = df

st.success(f"Datos filtrados: {df.shape[0]} filas entre {rango_fechas[0]} y {rango_fechas[1]}")

# ✅ Footer HTML
st.markdown("<br><br><br><br>", unsafe_allow_html=True)
footer = '''
<style>
.footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    background-color: #f0f2f6;
    color: #333;
    text-align: center;
    font-size: 12px;
    padding: 10px;
    border-top: 1px solid #ccc;
    z-index: 999;
}
</style>
<div class="footer">
    Autor: Nicolás Fernández Ponce, CFA | Dashboard Fondos Mutuos Chile · Datos CMF
</div>
'''
st.markdown(footer, unsafe_allow_html=True)
