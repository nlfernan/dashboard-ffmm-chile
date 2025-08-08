# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
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

# Columnas que usamos
DEST_COLS = [
    "fecha_dia",          # desde fecha_inf_archivo
    "run_fm",             # RUT fondo
    "nemotecnico",
    "tipo_instrumento",
    "valor_mercado"
]

# Alias crudos -> destino (incluye tu fecha_inf_archivo y run_fondo)
ALIAS_RAW = {
    # fecha
    "fecha_inf_archivo": "fecha_dia",
    "fecha_dia": "fecha_dia",
    "fecha": "fecha_dia",
    "fecha_inf": "fecha_dia",
    "fecha_informe": "fecha_dia",

    # RUT fondo
    "run_fondo": "run_fm",
    "run_fm": "run_fm",

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
}

CANDIDATAS_MINIMAS = list(ALIAS_RAW.keys())

# ===============================
# 🧠 Utilidades
# ===============================
def _schema_cols(path: str):
    try:
        import pyarrow.parquet as pq
        return set(pq.ParquetFile(path).schema.names)
    except Exception:
        return None

@st.cache_data
def _leer_minimo(path: str, candidatas: list) -> pd.DataFrame:
    cols_schema = _schema_cols(path)
    if cols_schema is not None:
        cols_presentes = [c for c in candidatas if c in cols_schema]
        df = pd.read_parquet(path, columns=cols_presentes or None)
    else:
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
    if df.empty:
        return df
    # mapear alias -> destino
    renames = {}
    for raw, dst in ALIAS_RAW.items():
        if raw in df.columns and dst not in df.columns:
            renames[raw] = dst
    if renames:
        df = df.rename(columns=renames)

    # tipos
    if "fecha_dia" in df.columns:
        df["fecha_dia"] = _to_datetime_safe(df["fecha_dia"])
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce")

    # quedarnos SOLO con lo que usamos
    cols_presentes = [c for c in DEST_COLS if c in df.columns]
    df = df[cols_presentes].copy()
    return df

# ===============================
# 📥 Carga mínima + normalización
# ===============================
df_raw, path_usado = _localizar_y_cargar_min()
if df_raw.empty:
    st.stop()

df = _normalizar_y_reducir(df_raw)

# Validaciones base
if "fecha_dia" not in df.columns or pd.to_datetime(df["fecha_dia"], errors="coerce").dropna().empty:
    st.error("❌ No se encontró una fecha válida (fecha_inf_archivo/fecha_dia) en la cartera.")
    st.stop()
if "run_fm" not in df.columns:
    st.error("❌ No se encontró la columna de RUT de fondo (run_fondo/run_fm).")
    st.stop()

# ===============================
# 🔎 Filtros
# ===============================
# Fechas
fechas = (
    pd.to_datetime(df["fecha_dia"], errors="coerce")
    .dropna()
    .dt.date
    .sort_values(ascending=False)
    .unique()
    .tolist()
)
fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas)

# RUTs de fondo con multiselect y "seleccionar todos"
ruts = sorted(df["run_fm"].dropna().unique().tolist())
col_all, col_ms = st.columns([1, 3])
with col_all:
    sel_todos = st.checkbox("Seleccionar todos los RUT", value=True)
with col_ms:
    ruts_sel = st.multiselect("RUT de Fondo", ruts, default=ruts if sel_todos else [])

if not ruts_sel:
    st.warning("Seleccioná al menos un RUT de fondo.")
    st.stop()

# ===============================
# 🎯 Filtrado por combinación
# ===============================
mask = (
    (pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date()) &
    (df["run_fm"].isin(ruts_sel))
)
df_sel = df.loc[mask].copy()
if df_sel.empty:
    st.warning("⚠️ No hay datos para esta combinación.")
    st.stop()

# ===============================
# 📋 Tabla (suma y % del total) — SIN mostrar RUT
# ===============================
# Si faltan columnas opcionales, las creo
for col, default in [
    ("nemotecnico", None),
    ("tipo_instrumento", "N/D"),
    ("valor_mercado", 0.0),
]:
    if col not in df_sel.columns:
        df_sel[col] = default

df_sel["valor_mercado"] = pd.to_numeric(df_sel["valor_mercado"], errors="coerce").fillna(0.0)

# Agrego agregación por RUT + Nemotécnico + Tipo (para cálculo correcto)
agrup = (
    df_sel.groupby(["run_fm", "nemotecnico", "tipo_instrumento"], as_index=False)["valor_mercado"]
    .sum()
    .sort_values("valor_mercado", ascending=False)
)

total = float(agrup["valor_mercado"].sum())
agrup["% del Total"] = (100.0 * agrup["valor_mercado"] / total).round(2) if total > 0 else 0.0

# Fila TOTAL
fila_total = pd.DataFrame({
    "run_fm": ["TOTAL"],
    "nemotecnico": [""],
    "tipo_instrumento": [""],
    "valor_mercado": [round(total, 0)],
    "% del Total": [100.0 if total > 0 else 0.0]
})

tabla = pd.concat([agrup, fila_total], ignore_index=True)

# ---- Mostrar tabla SIN RUT (solo oculto en UI) ----
tabla_mostrar = tabla.drop(columns=["run_fm"]).rename(columns={
    "nemotecnico": "Nemotécnico",
    "tipo_instrumento": "Tipo de Instrumento",
    "valor_mercado": "Valor Mercado (CLP)"
}).copy()

if "Valor Mercado (CLP)" in tabla_mostrar.columns:
    tabla_mostrar["Valor Mercado (CLP)"] = pd.to_numeric(
        tabla_mostrar["Valor Mercado (CLP)"], errors="coerce"
    ).round(0)

st.dataframe(tabla_mostrar, use_container_width=True)

# ===============================
# ⬇️ Descargar CSV (incluye RUT)
# ===============================
@st.cache_data
def _csv_bytes(df_out: pd.DataFrame) -> bytes:
    return df_out.to_csv(index=False).encode("utf-8-sig")

csv_data = _csv_bytes(tabla.rename(columns={
    "run_fm": "RUT",
    "nemotecnico": "Nemotecnico",  # sin tilde para CSV
    "tipo_instrumento": "TipoInstrumento",
    "valor_mercado": "ValorMercadoCLP",
    "% del Total": "PctDelTotal"
}))
st.download_button(
    label="⬇️ Bajar CSV",
    data=csv_data,
    file_name=f"detalle_cartera_{pd.to_datetime(fecha_sel).date()}.csv",
    mime="text/csv"
)

# ===============================
# 📌 Marcas al final
# ===============================
st.markdown(f"📂 Usando parquet: `{path_usado or ''}`")
st.markdown(f"🗓️ Fecha efectiva en vista: **{pd.to_datetime(fecha_sel).date()}**")
