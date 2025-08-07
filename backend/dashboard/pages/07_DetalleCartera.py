# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import altair as alt
import os

st.title("📑 Detalle Cartera (ACC)")

# Ruta absoluta o relativa al parquet
ruta_parquet = os.path.join(
    "backend", "data_fuentes", "cartera_merged_ACC.parquet"
)

# 🚦 Cargar parquet
@st.cache_data
def cargar_cartera(path):
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        st.error(f"❌ No se encontró el archivo: {path}")
        return pd.DataFrame()

df = cargar_cartera(ruta_parquet)

if df.empty:
    st.stop()

# ===============================
# 📅 Filtro de fecha y fondo
# ===============================
fechas_disponibles = sorted(df["fecha_dia"].unique(), reverse=True)
fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas_disponibles)

fondos_disponibles = df["nombre_corto"].unique()
fondo_sel = st.selectbox("🏦 Selecciona un fondo", sorted(fondos_disponibles))

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
    df_fondo.groupby("tipo_instrumento")["valor_mercado"]
    .sum()
    .reset_index()
)
df_tipo["porcentaje"] = 100 * df_tipo["valor_mercado"] / df_tipo["valor_mercado"].sum()
df_tipo["porcentaje"] = df_tipo["porcentaje"].round(2)

chart = alt.Chart(df_tipo).mark_bar().encode(
    x=alt.X("tipo_instrumento:N", title="Tipo de Instrumento", sort="-y"),
    y=alt.Y("valor_mercado:Q", title="Monto CLP"),
    tooltip=["tipo_instrumento", "valor_mercado", "porcentaje"]
).properties(
    title="Distribución por Tipo de Instrumento",
    height=300
)

st.altair_chart(chart, use_container_width=True)

# ===============================
# 📋 Detalle
# ===============================
df_detalle = df_fondo[["nemotecnico", "tipo_instrumento", "valor_mercado"]].copy()
df_detalle["valor_mercado"] = df_detalle["valor_mercado"].round(0)
df_detalle = df_detalle.rename(columns={
    "nemotecnico": "Nemotécnico",
    "tipo_instrumento": "Tipo de Instrumento",
    "valor_mercado": "Valor Mercado (CLP)"
})

st.dataframe(df_detalle, use_container_width=True)
