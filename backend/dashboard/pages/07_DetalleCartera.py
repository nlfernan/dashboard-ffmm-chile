# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import altair as alt
import os

st.title("📑 Detalle Cartera (ACC)")

# ===============================
# 🔧 Definiciones
# ===============================
RUTAS_CANDIDATAS = [
    "app/data_fuentes/cartera_merged_ACC.parquet",
    "backend/data_fuentes/cartera_merged_ACC.parquet",
    "data_fuentes/cartera_merged_ACC.parquet",
]

COLUMNAS_REQUERIDAS = {
    "fecha_dia",
    "nombre_corto",
    "tipo_instrumento",
    "valor_mercado",
    "nemotecnico",
}

@st.cache_data
def cargar_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

def localizar_y_cargar() -> pd.DataFrame:
    """
    Intenta tomar df desde session_state.
    Si no existe, busca el parquet en rutas conocidas y lo deja en session_state.
    """
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera

    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = cargar_parquet(ruta)
            st.session_state.df_cartera = df
            st.info(f"📂 Cartera cargada desde: `{ruta}`")
            return df

    st.error("❌ No encontré `df_cartera` en sesión ni el parquet en rutas conocidas.\n"
             "Verificá que el archivo exista o precarga el DataFrame en el `app.py`.")
    return pd.DataFrame()

# ===============================
# 📥 Carga (session_state-first)
# ===============================
df = localizar_y_cargar()
if df.empty:
    st.stop()

# 🔎 Validación de columnas
faltantes = COLUMNAS_REQUERIDAS - set(df.columns)
if faltantes:
    st.error(f"❌ Faltan columnas requeridas en la cartera: {sorted(faltantes)}")
    st.stop()

# ===============================
# 📅 Filtros
# ===============================
fechas_disponibles = sorted(df["fecha_dia"].unique(), reverse=True)
fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas_disponibles)

fondos_disponibles = sorted(df["nombre_corto"].unique())
fondo_sel = st.selectbox("🏦 Selecciona un fondo", fondos_disponibles)

# ===============================
# 🎯 Filtrar
# ===============================
df_fondo = df[(df["fecha_dia"] == fecha_sel) & (df["nombre_corto"] == fondo_sel)]
if df_fondo.empty:
    st.warning("⚠️ No hay datos para esta combinación.")
    st.stop()

# ===============================
# 📊 Composición por tipo de instrumento
# ===============================
df_tipo = (
    df_fondo.groupby("tipo_instrumento", as_index=False)["valor_mercado"]
    .sum()
    .sort_values("valor_mercado", ascending=False)
)
df_tipo["porcentaje"] = 100 * df_tipo["valor_mercado"] / df_tipo["valor_mercado"].sum()
df_tipo["porcentaje"] = df_tipo["porcentaje"].round(2)

chart = alt.Chart(df_tipo).mark_bar().encode(
    x=alt.X("tipo_instrumento:N", title="Tipo de Instrumento", sort="-y"),
    y=alt.Y("valor_mercado:Q", title="Monto CLP"),
    tooltip=[
        alt.Tooltip("tipo_instrumento:N", title="Tipo"),
        alt.Tooltip("valor_mercado:Q", title="Valor Mercado"),
        alt.Tooltip("porcentaje:Q", title="% del Fondo")
    ]
).properties(
    title=f"Distribución por Tipo de Instrumento — {fondo_sel} ({pd.to_datetime(fecha_sel).date()})",
    height=300
)
st.altair_chart(chart, use_container_width=True)

# ===============================
# 📋 Detalle
# ===============================
df_detalle = (
    df_fondo[["nemotecnico", "tipo_instrumento", "valor_mercado"]]
    .copy()
    .rename(columns={
        "nemotecnico": "Nemotécnico",
        "tipo_instrumento": "Tipo de Instrumento",
        "valor_mercado": "Valor Mercado (CLP)"
    })
)
df_detalle["Valor Mercado (CLP)"] = df_detalle["Valor Mercado (CLP)"].round(0)
st.dataframe(df_detalle, use_container_width=True)
