# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar

st.title("📑 Detalle Cartera (ACC)")

# 🔄 Botón de recarga para evitar caché vieja (solo dev)
if st.button("🔄 Forzar recarga de datos (dev)"):
    st.cache_data.clear()
    st.rerun()

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
    "fecha_dia",          # mapeada desde fecha_inf_archivo
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

def _multiselect_con_todo(label: str, opciones: list):
    opciones_ui = ["(Seleccionar todo)"] + opciones
    seleccion_raw = st.multiselect(label, opciones_ui, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion_raw:
        return opciones
    return seleccion_raw

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
# Fechas (tomamos snapshot por día, y para CSV usamos el MES de esa fecha)
fechas = (
    pd.to_datetime(df["fecha_dia"], errors="coerce")
    .dropna()
    .dt.date
    .sort_values(ascending=False)
    .unique()
    .tolist()
)
fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas)

# RUTs disponibles
ruts = sorted(df["run_fm"].dropna().unique().tolist())

# Comparador: Fondo 1 y Fondo 2 (cada uno puede ser “todo”)
colA, colB = st.columns(2)
with colA:
    ruts_fondo1 = _multiselect_con_todo("Fondo 1 (RUT)", ruts)
with colB:
    ruts_fondo2 = _multiselect_con_todo("Fondo 2 (RUT)", ruts)

if not ruts_fondo1 and not ruts_fondo2:
    st.warning("Seleccioná al menos un conjunto (Fondo 1 o Fondo 2).")
    st.stop()

# ===============================
# 🎯 Filtrado por fecha del snapshot
# ===============================
df_day = df[pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date()].copy()
if df_day.empty:
    st.warning("⚠️ No hay datos para esa fecha.")
    st.stop()

for col, default in [("nemotecnico", None), ("tipo_instrumento", "N/D"), ("valor_mercado", 0.0)]:
    if col not in df_day.columns:
        df_day[col] = default
df_day["valor_mercado"] = pd.to_numeric(df_day["valor_mercado"], errors="coerce").fillna(0.0)

# ===============================
# 🧮 Comparador por Nemotécnico
# ===============================
def _agg_por_grupo(df_base: pd.DataFrame, ruts_sel: list, pref: str):
    if not ruts_sel:
        # grupo vacío → columnas en 0 para merge simétrico
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"])
    tmp = df_base[df_base["run_fm"].isin(ruts_sel)]
    if tmp.empty:
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"])
    g = tmp.groupby("nemotecnico", as_index=False)["valor_mercado"].sum()
    total = float(g["valor_mercado"].sum())
    g[f"{pref}_vm"] = g["valor_mercado"]
    g[f"{pref}_pct"] = (100.0 * g["valor_mercado"] / total).round(2) if total > 0 else 0.0
    g = g.drop(columns=["valor_mercado"])
    return g

g1 = _agg_por_grupo(df_day, ruts_fondo1, "F1")
g2 = _agg_por_grupo(df_day, ruts_fondo2, "F2")

tabla = pd.merge(g1, g2, on="nemotecnico", how="outer").fillna(0.0)

# Orden por el mayor VM entre grupos
if not tabla.empty:
    tabla["_orden"] = tabla[["F1_vm", "F2_vm"]].max(axis=1)
    tabla = tabla.sort_values("_orden", ascending=False).drop(columns=["_orden"])

# Mostrar sin RUT (la fila es por nemotécnico)
tabla_mostrar = tabla.rename(columns={
    "nemotecnico": "Nemotécnico",
    "F1_vm": "Fondo1 Valor de Mercado (CLP)",
    "F1_pct": "Fondo1 % del Total",
    "F2_vm": "Fondo2 Valor de Mercado (CLP)",
    "F2_pct": "Fondo2 % del Total",
}).copy()

# Formato para la UI
for col_vm in ["Fondo1 Valor de Mercado (CLP)", "Fondo2 Valor de Mercado (CLP)"]:
    if col_vm in tabla_mostrar.columns:
        tabla_mostrar[col_vm] = pd.to_numeric(tabla_mostrar[col_vm], errors="coerce").round(0)

st.dataframe(tabla_mostrar, use_container_width=True)

# ===============================
# ⬇️ Descargar CSV del MES (todos los fondos)
# ===============================
# Armamos el mes a partir de la fecha seleccionada
fec = pd.to_datetime(fecha_sel)
anio, mes = int(fec.year), int(fec.month)
primer_dia = pd.Timestamp(anio, mes, 1)
ultimo_dia = pd.Timestamp(anio, mes, calendar.monthrange(anio, mes)[1])

df_month = df[
    (pd.to_datetime(df["fecha_dia"]) >= primer_dia) &
    (pd.to_datetime(df["fecha_dia"]) <= ultimo_dia)
].copy()

# Normalizo tipos mínimos para el CSV mensual completo
for col, default in [("nemotecnico", None), ("tipo_instrumento", "N/D"), ("valor_mercado", 0.0)]:
    if col not in df_month.columns:
        df_month[col] = default
df_month["valor_mercado"] = pd.to_numeric(df_month["valor_mercado"], errors="coerce").fillna(0.0)

# Exportamos “todos los fondos para ese mes” (no filtramos por Fondo1/2)
@st.cache_data
def _csv_mes_bytes(df_out: pd.DataFrame) -> bytes:
    cols_csv = []
    for c in ["fecha_dia", "run_fm", "nemotecnico", "tipo_instrumento", "valor_mercado"]:
        if c in df_out.columns:
            cols_csv.append(c)
    return df_out[cols_csv].to_csv(index=False).encode("utf-8-sig")

csv_mes = _csv_mes_bytes(df_month.rename(columns={
    "fecha_dia": "Fecha",
    "run_fm": "RUT",
    "nemotecnico": "Nemotecnico",
    "tipo_instrumento": "TipoInstrumento",
    "valor_mercado": "ValorMercadoCLP"
}))
st.download_button(
    label="⬇️ Bajar CSV — Todos los fondos del mes",
    data=csv_mes,
    file_name=f"cartera_mes_{anio}-{mes:02d}.csv",
    mime="text/csv"
)

# ===============================
# 📌 Marcas al final
# ===============================
st.markdown(f"📂 Usando parquet: `{st.session_state.get('path_cartera', '')}`")
st.markdown(f"🗓️ Fecha efectiva en vista: **{pd.to_datetime(fecha_sel).date()}**")
