# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import unicodedata
import calendar
import time
from datetime import date, timedelta

# ===============================
# 📂 Ruta y columnas necesarias
# ===============================
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

COLUMNAS_NECESARIAS = [
    "fecha_inf_date", "fecha_inf", "run_fm", "nombre_corto", "run_fm_nombrecorto",
    "nom_adm", "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
    "tipo_fm", "categoria", "categoria_agrupada", "serie"
]

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

# ===============================
# 📊 Carga con barra de progreso
# ===============================
@st.cache_data
def cargar_datos():
    placeholder = st.empty()
    placeholder.info("⏳ Cargando datos, por favor espera...")

    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")

    # Barra de progreso simulada (0–100%)
    progress = st.progress(0)
    for i in range(0, 101, 10):
        time.sleep(0.05)
        progress.progress(i)
    placeholder.empty()
    progress.empty()

    df.columns = [limpiar_nombre(c) for c in df.columns]

    # Compatibilidad fecha_inf
    if "fecha_inf_date" not in df.columns and "fecha_inf" in df.columns:
        df = df.rename(columns={"fecha_inf": "fecha_inf_date"})

    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
    df["fecha_dia"] = df["fecha_inf_date"].dt.date

    if "run_fm_nombrecorto" not in df.columns:
        if "run_fm" in df.columns and "nombre_corto" in df.columns:
            df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

    for col in ["categoria", "categoria_agrupada", "nom_adm", "tipo_fm", "serie", "run_fm_nombrecorto"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

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

st.write("Configura los filtros y presiona **Aplicar filtros** para actualizar los datos.")

# ===============================
# 📅 Filtros de fecha
# ===============================
fechas_unicas = sorted(df["fecha_dia"].unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted({f.year for f in fechas_unicas})
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

# ===============================
# 📌 Cache de opciones fijas
# ===============================
@st.cache_data
def cargar_opciones(df):
    return (
        sorted(df["categoria_agrupada"].dropna().unique()) if "categoria_agrupada" in df.columns else [],
        sorted(df["categoria"].dropna().unique()),
        sorted(df["nom_adm"].dropna().unique()),
        sorted(df["run_fm_nombrecorto"].dropna().unique()),
        sorted(df["tipo_fm"].dropna().unique()),
        sorted(df["serie"].dropna().unique())
    )

categorias_agrupadas_all, categorias_all, administradoras_all, fondos_all, tipos_all, series_all = cargar_opciones(df)

# ✅ Multiselect simple con "(Seleccionar todo)"
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    return seleccion

# ✅ Función para limpiar o expandir "(Seleccionar todo)"
def limpiar_selecciones(seleccion, universo):
    if "(Seleccionar todo)" in seleccion:
        if len(seleccion) == 1:
            return list(universo)  # Si es la única opción → todas las reales
        else:
            return [x for x in seleccion if x != "(Seleccionar todo)"]  # Si hay más → quitarla
    return seleccion

# ===============================
# ✅ Función de filtros (DEFINIDA ARRIBA)
# ===============================
@st.cache_data
def aplicar_filtros(df, categorias_agrupadas, categorias, administradoras, fondos, tipos, series, rango):
    filtro = (
        df["categoria"].isin(categorias) &
        df["nom_adm"].isin(administradoras) &
        df["run_fm_nombrecorto"].isin(fondos) &
        df["tipo_fm"].isin(tipos) &
        df["serie"].isin(series) &
        (df["fecha_dia"] >= rango[0]) &
        (df["fecha_dia"] <= rango[1])
    )
    if "categoria_agrupada" in df.columns and categorias_agrupadas:
        filtro = filtro & df["categoria_agrupada"].isin(categorias_agrupadas)

    return df[filtro]

# ===============================
# 🎛️ Filtros UI
# ===============================
categorias_agrupadas = multiselect_con_todo("Categoría Agrupada", categorias_agrupadas_all)
categorias = multiselect_con_todo("Categoría", categorias_all)
administradoras = multiselect_con_todo("Administradora(s)", administradoras_all)
fondos = multiselect_con_todo("Fondo(s)", fondos_all)

with st.expander("Filtros adicionales"):
    tipos = multiselect_con_todo("Tipo de Fondo", tipos_all)
    series = multiselect_con_todo("Serie(s)", series_all)

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
st.markdown("### 🔍 Aplicar filtros a los datos")

if st.button("✅ Aplicar filtros", use_container_width=True):
    categorias_agrupadas = limpiar_selecciones(categorias_agrupadas, categorias_agrupadas_all)
    categorias = limpiar_selecciones(categorias, categorias_all)
    administradoras = limpiar_selecciones(administradoras, administradoras_all)
    fondos = limpiar_selecciones(fondos, fondos_all)
    tipos = limpiar_selecciones(tipos, tipos_all)
    series = limpiar_selecciones(series, series_all)

    st.session_state.datos_cargados = False
    df_filtrado = aplicar_filtros(df, categorias_agrupadas, categorias, administradoras, fondos, tipos, series, rango)
    st.session_state.df_filtrado = df_filtrado
    st.session_state.datos_cargados = True
    st.success(f"✅ Datos filtrados: {df_filtrado.shape[0]:,} filas disponibles")
elif "df_filtrado" in st.session_state:
    st.info(f"ℹ️ Usando datos filtrados previamente: {st.session_state.df_filtrado.shape[0]:,} filas")
else:
    st.warning("🔎 Configura los filtros y presiona **Aplicar filtros** para ver datos")

# ===============================
# 📌 Footer HTML
# ===============================
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
