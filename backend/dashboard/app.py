# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
import random
import unicodedata
from datetime import date, timedelta
from openai import OpenAI, RateLimitError

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
    # Normalizar nombres
    df.columns = [limpiar_nombre(c) for c in df.columns]
    df.columns = hacer_unicas(df.columns)
    # Seleccionar solo columnas necesarias si existen
    columnas = [c for c in [
        "fecha_inf_date", "run_fm", "nombre_corto", "nom_adm",
        "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
        "tipo_fm", "categoria", "serie"
    ] if c in df.columns]
    return df[columnas]

df = cargar_datos()

if df.empty:
    st.warning("No hay datos disponibles en el archivo Parquet.")
    st.stop()

# -------------------------------
# Preprocesamiento
# -------------------------------
df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# -------------------------------
# Título
# -------------------------------
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; padding-top: 10px;'>
    <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Owl_in_the_Moonlight.jpg/640px-Owl_in_the_Moonlight.jpg'
         width='60' style='border-radius: 50%; box-shadow: 0 2px 6px rgba(0,0,0,0.2);'/>
    <h1 style='margin: 0; font-size: 2.2em;'>Dashboard Fondos Mutuos</h1>
</div>
""", unsafe_allow_html=True)

# -------------------------------
# Rango de Fechas (basado en todo el histórico)
# -------------------------------
st.markdown("### Rango de Fechas")

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

# Filtrar por rango elegido
df = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) &
        (df["fecha_inf_date"].dt.date <= fecha_fin)]

if df.empty:
    st.warning("No hay datos para el rango seleccionado.")
    st.stop()

# -------------------------------
# Session state para slider
# -------------------------------
if "rango_fechas" not in st.session_state:
    st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

# -------------------------------
# Multiselect con "Seleccionar todo"
# -------------------------------
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion or not seleccion:
        return list(opciones)
    else:
        return seleccion

# -------------------------------
# Filtros principales
# -------------------------------
categoria_opciones = sorted(df["categoria"].dropna().unique())
categoria_seleccionadas = multiselect_con_todo("Categoría", categoria_opciones)

adm_opciones = sorted(df[df["categoria"].isin(categoria_seleccionadas)]["nom_adm"].dropna().unique())
adm_seleccionadas = multiselect_con_todo("Administradora(s)", adm_opciones)

fondo_opciones = sorted(df[df["nom_adm"].isin(adm_seleccionadas)]["run_fm_nombrecorto"].dropna().unique())
fondo_seleccionados = multiselect_con_todo("Fondo(s)", fondo_opciones)

with st.expander("🔧 Filtros adicionales"):
    tipo_opciones = sorted(df["tipo_fm"].dropna().unique())
    tipo_seleccionados = multiselect_con_todo("Tipo de Fondo", tipo_opciones)

    serie_opciones = sorted(df[df["run_fm_nombrecorto"].isin(fondo_seleccionados)]["serie"].dropna().unique())
    serie_seleccionadas = multiselect_con_todo("Serie(s)", serie_opciones)

    st.markdown("#### Ajuste fino de fechas")
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

# -------------------------------
# Aplicar filtros al DataFrame
# -------------------------------
rango_fechas = st.session_state["rango_fechas"]

df_filtrado = df[df["tipo_fm"].isin(tipo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["categoria"].isin(categoria_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["nom_adm"].isin(adm_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["run_fm_nombrecorto"].isin(fondo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["serie"].isin(serie_seleccionadas)]
df_filtrado = df_filtrado[(df_filtrado["fecha_inf_date"].dt.date >= rango_fechas[0]) &
                          (df_filtrado["fecha_inf_date"].dt.date <= rango_fechas[1])]

if df_filtrado.empty:
    st.warning("No hay datos disponibles con los filtros seleccionados.")
    st.stop()

# -------------------------------
# Tabs (gráficos, listado, IA)
# -------------------------------
# ... (aquí mantienes las tabs de gráficos, listado y IA igual que en tu versión actual)

# -------------------------------
# Footer
# -------------------------------
st.markdown("<br><br><br><br>", unsafe_allow_html=True)

footer = """
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
    Autor: Nicolás Fernández Ponce, CFA | Este dashboard muestra la evolución del patrimonio y las ventas netas de fondos mutuos en Chile.  
    Datos provistos por la <a href="https://www.cmfchile.cl" target="_blank">CMF</a>
</div>
"""
st.markdown(footer, unsafe_allow_html=True)
