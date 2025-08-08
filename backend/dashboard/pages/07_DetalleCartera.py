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

# Columnas que REALMENTE usamos en la vista
DEST_COLS = ["fecha_dia", "nombre_corto", "nemotecnico", "tipo_instrumento", "valor_mercado"]

# Alias típicos en bruto -> destino
ALIAS_RAW = {
    # nombre del fondo
    "nombre_fondo": "nombre_corto",
    "nom_fondo": "nombre_corto",
    "nombre_corto": "nombre_corto",
    # nemotécnico
    "nemotecnico_instrumento": "nemotecnico",
    "nemotecnico": "nemotecnico",
    "nemo": "nemotecnico",
    # tipo
    "tipo_instrumento": "tipo_instrumento",
    # valor de mercado
    "valorizacion_cierre_m": "valor_mercado",
    "valor_mercado": "valor_mercado",
    "valor_mercado_clp": "valor_mercado",
    # fecha
    "fecha_dia": "fecha_dia",
    "fecha": "fecha_dia",
    "fecha_inf": "fecha_dia",
    "fecha_informe": "fecha_dia",
}

# Candidatas de lectura mínima (los alias crudos)
CANDIDATAS_MINIMAS = list(ALIAS_RAW.keys())

# ===============================
# 🧠 Utilidades
# ===============================
def _schema_cols(path: str):
    """Lee columnas del schema sin cargar todo (si hay pyarrow)."""
    try:
        import pyarrow.parquet as pq
        return set(pq.ParquetFile(path).schema.names)
    except Exception:
        return None  # si falla, leemos y filtramos después

@st.cache_data
def _leer_minimo(path: str, candidatas: list) -> pd.DataFrame:
    """Intenta leer SOLO las candidatas presentes. Si no puede, lee completo y filtra."""
    cols_schema = _schema_cols(path)
    if cols_schema is not None:
        cols_presentes = [c for c in candidatas if c in cols_schema]
        if not cols_presentes:
            # último recurso: leer completo
            df = pd.read_parquet(path)
        else:
            df = pd.read_parquet(path, columns=cols_presentes)
    else:
        # sin schema: leer completo
        df = pd.read_parquet(path)
    # normalizo nombres crudos
    df = df.rename(columns={c: c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns})
    return df

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

def _localizar_y_cargar_min():
    """Devuelve (df_minimo, path_usado). Prioriza session_state."""
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera.copy(), st.session_state.get("path_cartera", "session_state")

    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = _leer_minimo(ruta, CANDIDATAS_MINIMAS)
            st.session_state.df_cartera = df
            st.session_state.path_cartera = ruta
            return df.copy(), ruta

    st.error("❌ No encontré `df_cartera` en sesión ni el parquet en rutas conocidas.")
    return pd.DataFrame(), None

def _normalizar_y_reducir(df: pd.DataFrame) -> pd.DataFrame:
    """Mapea alias crudos a columnas destino y devuelve SOLO DEST_COLS."""
    if df.empty:
        return df

    # ya vienen normalizados en lower/sin espacios
    renames = {}
    for raw, dst in ALIAS_RAW.items():
        if raw in df.columns and dst not in df.columns:
            renames[raw] = dst
    if renames:
        df = df.rename(columns=renames)

    # fecha
    if "fecha_dia" in df.columns:
        df["fecha_dia"] = _to_datetime_safe(df["fecha_dia"])
    # valor mercado
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce")

    # quedarnos SOLO con lo que usamos (si faltan, se completan luego)
    cols_presentes = [c for c in DEST_COLS if c in df.columns]
    df = df[cols_presentes].copy()

    return df

# ===============================
# 📥 Carga mínima
# ===============================
df_raw, path_usado = _localizar_y_cargar_min()
if df_raw.empty:
    st.stop()

st.caption(f"📂 Usando parquet: `{path_usado}`")

df = _normalizar_y_reducir(df_raw)

# ===============================
# 📅 Fecha (si no viene, la pedimos una sola vez)
# ===============================
tiene_fecha_valida = "fecha_dia" in df.columns and not pd.to_datetime(df["fecha_dia"], errors="coerce").isna().all()

if not tiene_fecha_valida:
    st.info("📅 La cartera no trae fecha. Seleccioná la fecha del informe para esta vista.")
    fecha_sel = st.date_input("Fecha del informe", value=pd.Timestamp.today().date())
    df["fecha_dia"] = pd.to_datetime(fecha_sel)
else:
    # armar lista de fechas disponibles
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
# 🧪 Asegurar columnas mínimas faltantes
# ===============================
for col, default in [
    ("nombre_corto", None),
    ("nemotecnico", None),
    ("tipo_instrumento", "N/D"),
    ("valor_mercado", 0.0),
]:
    if col not in df.columns:
        df[col] = default

# ===============================
# 🏦 Fondo
# ===============================
fondos_disponibles = sorted(df["nombre_corto"].dropna().unique().tolist())
if not fondos_disponibles:
    st.error("No hay fondos disponibles en la cartera.")
    st.stop()

fondo_sel = st.selectbox("🏦 Selecciona un fondo", fondos_disponibles)

# ===============================
# 🎯 Filtrado por combinación
# ===============================
df_fondo = df[
    (pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date())
    & (df["nombre_corto"] == fondo_sel)
].copy()

if df_fondo.empty:
    st.warning("⚠️ No hay datos para esta combinación.")
    st.stop()

# ===============================
# 📊 Composición por tipo de instrumento
# ===============================
if "tipo_instrumento" not in df_fondo.columns:
    df_fondo["tipo_instrumento"] = "N/D"

vm = pd.to_numeric(df_fondo["valor_mercado"], errors="coerce").fillna(0)
df_fondo["valor_mercado"] = vm

df_tipo = (
    df_fondo.groupby("tipo_instrumento", as_index=False)["valor_mercado"]
    .sum()
    .sort_values("valor_mercado", ascending=False)
)
total_vm = float(df_tipo["valor_mercado"].sum())
if total_vm <= 0:
    st.warning("⚠️ La valorización de mercado es 0 para esta selección.")
else:
    df_tipo["porcentaje"] = (100.0 * df_tipo["valor_mercado"] / total_vm).round(2)

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
cols_detalle = [c for c in ["nemotecnico", "tipo_instrumento", "valor_mercado"] if c in df_fondo.columns]
df_detalle = (
    df_fondo[cols_detalle]
    .rename(columns={
        "nemotecnico": "Nemotécnico",
        "tipo_instrumento": "Tipo de Instrumento",
        "valor_mercado": "Valor Mercado (CLP)"
    })
)
if "Valor Mercado (CLP)" in df_detalle.columns:
    df_detalle["Valor Mercado (CLP)"] = pd.to_numeric(df_detalle["Valor Mercado (CLP)"], errors="coerce").round(0)

st.dataframe(df_detalle, use_container_width=True)
