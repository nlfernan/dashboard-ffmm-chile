# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import altair as alt
import os

st.title("📑 Detalle Cartera (ACC)")

# ===============================
# 🔧 Config
# ===============================
RUTAS_CANDIDATAS = [
    "app/data_fuentes/cartera_merged_ACC.parquet",
    "backend/data_fuentes/cartera_merged_ACC.parquet",
    "data_fuentes/cartera_merged_ACC.parquet",
]
ESPERADAS = ["fecha_dia", "nombre_corto", "nemotecnico", "valor_mercado"]

# ===============================
# 🧠 Carga (session_state-first)
# ===============================
@st.cache_data
def _leer_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

def _localizar_y_cargar():
    """Devuelve (df, path_usado). Prioriza session_state; si no, busca en rutas candidatas."""
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera.copy(), st.session_state.get("path_cartera", "session_state")

    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = _leer_parquet(ruta)
            st.session_state.df_cartera = df
            st.session_state.path_cartera = ruta
            return df.copy(), ruta

    st.error("❌ No encontré `df_cartera` en sesión ni el parquet en rutas conocidas.")
    return pd.DataFrame(), None

df_raw, path_usado = _localizar_y_cargar()
if df_raw.empty:
    st.stop()

st.caption(f"📂 Usando parquet: `{path_usado}`")

# ===============================
# 🧹 Normalización de columnas
# ===============================
def _to_datetime_safe(s: pd.Series) -> pd.Series:
    if pd.api.types.is_integer_dtype(s):
        return pd.to_datetime(s.astype(str), format="%Y%m%d", errors="coerce")
    out = pd.to_datetime(s, errors="coerce")
    if out.isna().all():
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            out = pd.to_datetime(s, format=fmt, errors="coerce")
            if not out.isna().all():
                break
    return out

def normalizar_cartera(df: pd.DataFrame) -> pd.DataFrame:
    original_cols = list(df.columns)
    # 1) nombres más limpios
    df = df.rename(columns={c: c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns})

    # 2) alias → esperadas (incluye tus columnas reales)
    alias = {
        # nombre_corto
        "nombre_corto": "nombre_corto",
        "nombre_fondo": "nombre_corto",
        "nom_fondo": "nombre_corto",

        # nemotecnico
        "nemotecnico": "nemotecnico",
        "nemotecnico_instrumento": "nemotecnico",  # tu caso
        "nemo": "nemotecnico",

        # valor_mercado
        "valor_mercado": "valor_mercado",
        "valorizacion_cierre_m": "valor_mercado",  # tu caso
        "valor_mercado_clp": "valor_mercado",

        # fecha
        "fecha_dia": "fecha_dia",
        "fecha": "fecha_dia",
        "fecha_inf": "fecha_dia",
        "fecha_informe": "fecha_dia",
    }

    for col_src, col_dst in alias.items():
        if col_src in df.columns and col_dst not in df.columns:
            df = df.rename(columns={col_src: col_dst})

    # 3) tipos y saneo
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce")
    if "fecha_dia" in df.columns:
        df["fecha_dia"] = _to_datetime_safe(df["fecha_dia"])

    # 4) validación parcial para reporte útil si faltan
    faltantes = [c for c in ESPERADAS if c not in df.columns]
    if faltantes:
        st.error(
            "❌ Faltan columnas requeridas en la cartera: "
            f"{faltantes}\n\n"
            f"🔎 Columnas originales: {original_cols}\n"
            f"🧭 Columnas normalizadas: {list(df.columns)}"
        )
        return pd.DataFrame()

    # 5) limpiar filas sin fecha/nombre
    df = df.dropna(subset=["nombre_corto"]).copy()
    return df

df = normalizar_cartera(df_raw)
if df.empty:
    st.stop()

# ===============================
# 📅 Fecha de cartera (manejo parquet sin fecha)
# ===============================
tiene_fecha = "fecha_dia" in df.columns and not pd.to_datetime(df["fecha_dia"], errors="coerce").isna().all()

if not tiene_fecha:
    st.info("📅 Fecha de la cartera (el archivo no trae fecha)")
    fecha_sel = st.date_input("Selecciona la fecha del informe", value=pd.Timestamp.today().date())
    df = df.copy()
    df["fecha_dia"] = pd.to_datetime(fecha_sel)  # seteo una sola fecha para todo el df
else:
    # normalizo y armo lista de fechas
    fechas = (
        pd.to_datetime(df["fecha_dia"], errors="coerce")
        .dropna()
        .dt.date
        .sort_values(ascending=False)
        .unique()
        .tolist()
    )
    if not fechas:
        st.error("No hay fechas válidas en la cartera.")
        st.stop()
    fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas)

st.caption(f"🗓️ Fecha efectiva en vista: **{pd.to_datetime(fecha_sel).date()}**")

# ===============================
# 🏦 Fondo
# ===============================
fondos_disponibles = sorted(df["nombre_corto"].dropna().unique().tolist())
if not fondos_disponibles:
    st.error("No hay fondos disponibles en la cartera.")
    st.stop()

fondo_sel = st.selectbox("🏦 Selecciona un fondo", fondos_disponibles)

# ===============================
# 🎯 Filtrar combinación
# ===============================
df_fondo = df[
    (pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date())
    & (df["nombre_corto"] == fondo_sel)
]
if df_fondo.empty:
    st.warning("⚠️ No hay datos para esta combinación.")
    st.stop()

# ===============================
# 📊 Composición por tipo de instrumento
# ===============================
if "tipo_instrumento" not in df_fondo.columns:
    df_fondo = df_fondo.copy()
    df_fondo["tipo_instrumento"] = "N/D"

df_tipo = (
    df_fondo.groupby("tipo_instrumento", as_index=False)["valor_mercado"]
    .sum()
    .sort_values("valor_mercado", ascending=False)
)
total_vm = df_tipo["valor_mercado"].sum()
if total_vm == 0:
    st.warning("⚠️ La valorización de mercado es 0 para esta selección.")
else:
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
        title=f"Distribución por Tipo de Instrumento — {fondo_sel} ({pd.to_datetime(fecha_sel).date()})",
        height=300
    )
    st.altair_chart(chart, use_container_width=True)

# ===============================
# 📋 Detalle por nemotécnico
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
