# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
from datetime import date, timedelta
from sqlalchemy import create_engine

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
# Filtro de fechas con Mes/Año
# -------------------------------
st.markdown("### Rango de Fechas")
fechas_disponibles = df["fecha_inf_date"].dropna()

if not fechas_disponibles.empty:
    años_disponibles, meses_disponibles = obtener_rango_fechas(df)

    fecha_min_real = fechas_disponibles.min().date()
    fecha_max_real = fechas_disponibles.max().date()

    año_inicio_default = fecha_min_real.year
    mes_inicio_default = fecha_min_real.month - 1

    año_fin_default = fecha_max_real.year
    mes_fin_default = fecha_max_real.month - 1

    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    año_inicio = col1.selectbox("Año inicio", años_disponibles,
                                index=años_disponibles.index(año_inicio_default))
    mes_inicio = col2.selectbox("Mes inicio", meses_disponibles,
                                index=mes_inicio_default)

    año_fin = col3.selectbox("Año fin", años_disponibles,
                             index=años_disponibles.index(año_fin_default))
    mes_fin = col4.selectbox("Mes fin", meses_disponibles,
                             index=mes_fin_default)

    fecha_inicio = date(año_inicio, meses_disponibles.index(mes_inicio)+1, 1)
    ultimo_dia_mes = calendar.monthrange(año_fin, meses_disponibles.index(mes_fin)+1)[1]
    fecha_fin_teorica = date(año_fin, meses_disponibles.index(mes_fin)+1, ultimo_dia_mes)
    fecha_fin = min(fecha_fin_teorica, fecha_max_real)

    rango_fechas = (fecha_inicio, fecha_fin)
else:
    st.warning("No hay fechas disponibles para este filtro.")
    st.stop()

df = df[(df["fecha_inf_date"].dt.date >= rango_fechas[0]) &
        (df["fecha_inf_date"].dt.date <= rango_fechas[1])]

if df.empty:
    st.warning("No hay datos disponibles en el rango seleccionado.")
    st.stop()

# -------------------------------
# Session state para slider
# -------------------------------
if "rango_fechas" not in st.session_state:
    st.session_state["rango_fechas"] = (rango_fechas[0], rango_fechas[1])

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

    # Ajuste fino de fechas
    st.markdown("#### Ajuste fino de fechas")
    fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
    fecha_min_real = fechas_unicas[0]
    fecha_max_real = fechas_unicas[-1]
    hoy = fecha_max_real

    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"):
        st.session_state["rango_fechas"] = (max(hoy - timedelta(days=30), fecha_min_real), hoy)
    if col_b.button("3M"):
        st.session_state["rango_fechas"] = (max(hoy - timedelta(days=90), fecha_min_real), hoy)
    if col_c.button("6M"):
        st.session_state["rango_fechas"] = (max(hoy - timedelta(days=180), fecha_min_real), hoy)
    if col_d.button("MTD"):
        st.session_state["rango_fechas"] = (date(hoy.year, hoy.month, 1), hoy)
    if col_e.button("YTD"):
        st.session_state["rango_fechas"] = (date(hoy.year, 1, 1), hoy)

# -------------------------------
# Aplicar filtros
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
# Tabs
# -------------------------------
tab1, tab2, tab3 = st.tabs([
    "Patrimonio Neto Total (MM CLP)",
    "Venta Neta Acumulada (MM CLP)",
    "Listado de Fondos Mutuos"
])

with tab1:
    st.subheader("Evolución del Patrimonio Neto Total (en millones de CLP)")
    patrimonio_total = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["patrimonio_neto_mm"]
        .sum()
        .sort_index()
    )
    patrimonio_total.index = pd.to_datetime(patrimonio_total.index)
    st.bar_chart(patrimonio_total, height=300, use_container_width=True)

with tab2:
    st.subheader("Evolución acumulada de la Venta Neta (en millones de CLP)")
    venta_neta_acumulada = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["venta_neta_mm"]
        .sum()
        .cumsum()
        .sort_index()
    )
    venta_neta_acumulada.index = pd.to_datetime(venta_neta_acumulada.index)
    st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

with tab3:
    ranking_ventas = (
        df_filtrado
        .groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
        .head(20)
        .copy()
    )

    total_fondos = df_filtrado[["run_fm", "nombre_corto", "nom_adm"]].drop_duplicates().shape[0]
    cantidad_ranking = ranking_ventas.shape[0]

    if total_fondos <= 20:
        titulo = f"Listado de Fondos Mutuos (total: {total_fondos})"
    else:
        titulo = f"Listado de Fondos Mutuos (top {cantidad_ranking} por Venta Neta de {total_fondos})"

    st.subheader(titulo)

    def generar_url_cmf(rut):
        return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI"

    ranking_ventas["URL CMF"] = ranking_ventas["run_fm"].astype(str).apply(generar_url_cmf)

    ranking_ventas = ranking_ventas.rename(columns={
        "run_fm": "RUT",
        "nombre_corto": "Nombre del Fondo",
        "nom_adm": "Administradora",
        "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
    })

    ranking_ventas["Venta Neta Acumulada (MM CLP)"] = ranking_ventas["Venta Neta Acumulada (MM CLP)"].apply(
        lambda x: f"{x:,.0f}".replace(",", ".")
    )

    ranking_ventas["URL CMF"] = ranking_ventas["URL CMF"].apply(lambda x: f'<a href="{x}" target="_blank">Ver en CMF</a>')

    st.markdown(ranking_ventas.to_html(index=False, escape=False), unsafe_allow_html=True)

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
