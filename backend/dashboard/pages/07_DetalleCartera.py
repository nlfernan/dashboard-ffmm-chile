# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import altair as alt
import os
from datetime import datetime

st.title("📑 Detalle Cartera (ACC)")

RUTAS_CANDIDATAS = [
    "app/data_fuentes/cartera_merged_ACC.parquet",
    "backend/data_fuentes/cartera_merged_ACC.parquet",
    "data_fuentes/cartera_merged_ACC.parquet",
]

ESPERADAS = ["fecha_dia", "nombre_corto", "nemotecnico", "valor_mercado"]

@st.cache_data
def cargar_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

def localizar_y_cargar() -> pd.DataFrame:
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera
    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = cargar_parquet(ruta)
            st.session_state.df_cartera = df
            st.info(f"📂 Cartera cargada desde: `{ruta}`")
            return df
    st.error("❌ No encontré `df_cartera` en sesión ni el parquet en rutas conocidas.")
    return pd.DataFrame()

def _to_datetime_safe(s):
    """Convierte fechas comunes: int tipo 20250730, string '2025-07-30'/'30-07-2025'."""
    if pd.api.types.is_integer_dtype(s):
        # 20250730 -> datetime
        return pd.to_datetime(s.astype(str), format="%Y%m%d", errors="coerce")
    # intenta múltiples formatos
    out = pd.to_datetime(s, errors="coerce")
    if out.isna().all():
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            out = pd.to_datetime(s, format=fmt, errors="coerce")
            if not out.isna().all():
                break
    return out

def normalizar_cartera(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres y crea columnas esperadas si existen con alias."""
    if df.empty:
        return df.copy()

    original_cols = list(df.columns)

    # 1) normalizar nombres: lower, sin espacios/puntos
    ren = {c: c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns}
    df = df.rename(columns=ren)

    # 2) mapa de alias → esperadas
    alias = {
        # nombre_corto
        "nombre_corto": "nombre_corto",
        "nom_fondo": "nombre_corto",
        "nombre_fondo": "nombre_corto",
        "nombre": "nombre_corto",

        # nemotecnico
        "nemotecnico": "nemotecnico",
        "nemo": "nemotecnico",
        "nemotec": "nemotecnico",

        # valor_mercado
        "valor_mercado": "valor_mercado",
        "valor_mercado_clp": "valor_mercado",
        "valor_mercado_total": "valor_mercado",
        "valor_mercado_miles": "valor_mercado",   # si viene en miles, abajo ajustamos opcional

        # fecha_dia
        "fecha_dia": "fecha_dia",
        "fecha": "fecha_dia",
        "fecha_inf": "fecha_dia",
        "fecha_informe": "fecha_dia",
    }

    # 3) renombrar usando alias disponibles
    for col_src, col_dst in alias.items():
        if col_src in df.columns and col_dst not in df.columns:
            df = df.rename(columns={col_src: col_dst})

    # 4) construir fecha_dia si no existe pero hay alguna candidata
    if "fecha_dia" not in df.columns:
        candidatas_fecha = [c for c in ["fecha_dia", "fecha", "fecha_inf", "fecha_informe"] if c in df.columns]
        if candidatas_fecha:
            base = candidatas_fecha[0]
            df["fecha_dia"] = _to_datetime_safe(df[base]).dt.date
        else:
            df["fecha_dia"] = pd.NaT  # forzamos para que falle validación con mensaje claro

    # 5) asegurar tipos
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce")
        # Si sospechás que venía en miles, activá el siguiente ajuste:
        # df["valor_mercado"] = df["valor_mercado"] * 1_000

    # 6) validación final
    faltantes = [c for c in ESPERADAS if c not in df.columns]
    if faltantes:
        st.error(
            "❌ Faltan columnas requeridas en la cartera: "
            f"{faltantes}\n\n"
            f"🔎 Columnas originales: {original_cols}\n"
            f"🧭 Columnas normalizadas: {list(df.columns)}"
        )
        return pd.DataFrame()

    # limpiar nulos críticos
    df = df.dropna(subset=["fecha_dia", "nombre_corto"]).copy()
    return df

# ===============================
# Carga y normalización
# ===============================
df_raw = localizar_y_cargar()
if df_raw.empty:
    st.stop()

df = normalizar_cartera(df_raw)
if df.empty:
    st.stop()

# ===============================
# Filtros
# ===============================
fechas_disponibles = sorted(pd.to_datetime(df["fecha_dia"]).dt.date.unique(), reverse=True)
fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas_disponibles)

fondos_disponibles = sorted(df["nombre_corto"].dropna().unique())
fondo_sel = st.selectbox("🏦 Selecciona un fondo", fondos_disponibles)

# ===============================
# Filtrado
# ===============================
df_fondo = df[(pd.to_datetime(df["fecha_dia"]).dt.date == fecha_sel) & (df["nombre_corto"] == fondo_sel)]
if df_fondo.empty:
    st.warning("⚠️ No hay datos para esta combinación.")
    st.stop()

# ===============================
# Composición por tipo de instrumento
# ===============================
if "tipo_instrumento" not in df_fondo.columns:
    st.warning("⚠️ No se encontró 'tipo_instrumento'. Muestro solo detalle por nemotécnico.")
    df_fondo["tipo_instrumento"] = "N/D"

df_tipo = (
    df_fondo.groupby("tipo_instrumento", as_index=False)["valor_mercado"]
    .sum()
    .sort_values("valor_mercado", ascending=False)
)
total_vm = df_tipo["valor_mercado"].sum()
df_tipo["porcentaje"] = (100 * df_tipo["valor_mercado"] / total_vm).round(2)

chart = alt.Chart(df_tipo).mark_bar().encode(
    x=alt.X("tipo_instrumento:N", title="Tipo de Instrumento", sort="-y"),
    y=alt.Y("valor_mercado:Q", title="Monto CLP"),
    tooltip=[
        alt.Tooltip("tipo_instrumento:N", title="Tipo"),
        alt.Tooltip("valor_mercado:Q", title="Valor Mercado"),
        alt.Tooltip("porcentaje:Q", title="% del Fondo")
    ]
).properties(
    title=f"Distribución por Tipo de Instrumento — {fondo_sel} ({fecha_sel})",
    height=300
)
st.altair_chart(chart, use_container_width=True)

# ===============================
# Detalle
# ===============================
cols_detalle = ["nemotecnico", "tipo_instrumento", "valor_mercado"]
presentes = [c for c in cols_detalle if c in df_fondo.columns]
df_detalle = (
    df_fondo[presentes]
    .copy()
    .rename(columns={
        "nemotecnico": "Nemotécnico",
        "tipo_instrumento": "Tipo de Instrumento",
        "valor_mercado": "Valor Mercado (CLP)"
    })
)
if "Valor Mercado (CLP)" in df_detalle.columns:
    df_detalle["Valor Mercado (CLP)"] = pd.to_numeric(df_detalle["Valor Mercado (CLP)"], errors="coerce").round(0)

st.dataframe(df_detalle, use_container_width=True)
