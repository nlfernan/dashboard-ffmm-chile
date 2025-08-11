# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from pathlib import Path
import io

st.set_page_config(page_title="AFPs - Métricas VC", layout="wide")
st.title("📈 AFPs — Métricas de Valor Cuota (rolling)")

# ===============================
# 🔧 Rutas candidatas
# ===============================
RUTAS = [
    r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\vc_metricas_rolling.parquet",
    "backend/data_fuentes/vc_metricas_rolling.parquet",
    "data_fuentes/vc_metricas_rolling.parquet",
    "vc_metricas_rolling.parquet",
]
CSV_FALLBACK = "/mnt/data/vc_metricas_rolling.csv"

# ===============================
# ♻️ Carga con cache
# ===============================
@st.cache_data(show_spinner=True)
def cargar_datos():
    for ruta in RUTAS:
        p = Path(ruta)
        if p.exists():
            df = pd.read_parquet(p)
            origen = f"parquet: {p}"
            break
    else:
        p = Path(CSV_FALLBACK)
        if p.exists():
            df = pd.read_csv(p)
            origen = f"csv: {p}"
        else:
            raise FileNotFoundError("No se encontró el archivo en las rutas configuradas.")

    df = df.copy()
    df.columns = df.columns.str.strip()

    posibles_fechas = ["fecha", "fecha_inf_date", "fecha_corte"]
    col_fecha = next((c for c in posibles_fechas if c in df.columns), None)
    if col_fecha:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date

    posibles_afp = ["afp", "administradora", "nom_afp", "nom_adm"]
    col_afp = next((c for c in posibles_afp if c in df.columns), None)

    posibles_fondo = ["fondo", "tipo_fondo", "tipo", "serie"]
    col_fondo = next((c for c in posibles_fondo if c in df.columns), None)

    metricas_sugeridas = [
        "rentabilidad_diaria",
        "ret_anual_1a", "ret_anual_2a", "ret_anual_3a", "ret_anual_5a",
        "std_anual_1a", "std_anual_2a", "std_anual_3a", "std_anual_5a",
    ]

    orden_pref = [c for c in [col_fecha, col_afp, col_fondo] if c] + [m for m in metricas_sugeridas if m in df.columns]
    resto = [c for c in df.columns if c not in orden_pref]
    df = df[orden_pref + resto] if orden_pref else df

    return df, {"origen": origen, "col_fecha": col_fecha, "col_afp": col_afp, "col_fondo": col_fondo}

# ===============================
# 🚚 Cargar datos
# ===============================
df, info = cargar_datos()
st.caption(f"Fuente de datos → **{info['origen']}**")

if df.empty:
    st.warning("El dataset está vacío.")
    st.stop()

# ===============================
# 🎛️ Filtros en la hoja
# ===============================
st.subheader("Filtros")

col1, col2, col3 = st.columns([1.2, 1, 1])

# ---- Fecha única
if info["col_fecha"]:
    fechas_disponibles = sorted(df[info["col_fecha"]].dropna().unique())
    with col1:
        # default última fecha
        fecha_sel = st.selectbox("Fecha", options=fechas_disponibles, index=len(fechas_disponibles) - 1)
else:
    fecha_sel = None

# ---- Universo dinámico de Administradoras por FECHA
def _admins_por_fecha(_df, col_fecha, col_afp, fecha):
    if not col_afp:
        return []
    scope = _df
    if col_fecha and fecha:
        scope = scope[scope[col_fecha] == fecha]
    vals = sorted([v for v in scope[col_afp].dropna().unique()])
    return vals

admins_dyn = _admins_por_fecha(df, info["col_fecha"], info["col_afp"], fecha_sel)

# preservar selección previa si sigue vigente
prev_admins = st.session_state.get("sel_afp_prev", None)
if prev_admins:
    prev_admins = [v for v in prev_admins if v in admins_dyn]
default_admins = prev_admins if prev_admins else admins_dyn

# ---- AFP / Administradora (dinámico por fecha)
if info["col_afp"]:
    with col2:
        sel_afp = st.multiselect("AFP / Administradora", options=admins_dyn, default=default_admins)
        st.session_state["sel_afp_prev"] = sel_afp[:]  # guardar selección vigente
else:
    sel_afp = None

# ---- Fondo / Serie
if info["col_fondo"]:
    vals_fondo = sorted([v for v in df[info["col_fondo"]].dropna().unique()])
    with col3:
        sel_fondo = st.multiselect("Fondo / Serie", options=vals_fondo, default=vals_fondo)
else:
    sel_fondo = None

# Ventana exclusiva (1A, 2A, 3A, 5A)
ventana = st.radio("Ventana", options=["1A", "2A", "3A", "5A"], horizontal=True)
sufijo = {"1A": "1a", "2A": "2a", "3A": "3a", "5A": "5a"}[ventana]

aplicar = st.button("Aplicar filtros", type="primary")

# ===============================
# 🧮 Aplicación de filtros
# ===============================
if "df_filtrado" not in st.session_state or aplicar:
    df_filtrado = df
    if info["col_fecha"] and fecha_sel:
        df_filtrado = df_filtrado[df_filtrado[info["col_fecha"]] == fecha_sel]
    if info["col_afp"] and sel_afp:
        df_filtrado = df_filtrado[df_filtrado[info["col_afp"]].isin(sel_afp)]
    if info["col_fondo"] and sel_fondo:
        df_filtrado = df_filtrado[df_filtrado[info["col_fondo"]].isin(sel_fondo)]
    st.session_state.df_filtrado = df_filtrado.copy()
else:
    df_filtrado = st.session_state.df_filtrado

# ===============================
# 🔎 Columnas según ventana
# ===============================
base_cols = [c for c in [info["col_fecha"], info["col_afp"], info["col_fondo"]] if c and c in df_filtrado.columns]
metricas_base = [c for c in ["rentabilidad_diaria"] if c in df_filtrado.columns]
metricas_ventana = [c for c in [f"ret_anual_{sufijo}", f"std_anual_{sufijo}"] if c in df_filtrado.columns]

cols_finales = base_cols + metricas_base + metricas_ventana
if len(metricas_ventana) == 0:
    cols_finales = [c for c in df_filtrado.columns if c in (base_cols + metricas_base)]

df_vista = df_filtrado[cols_finales].copy()

st.success(f"Registros filtrados: {len(df_vista):,} | Ventana seleccionada: {ventana}")

# ===============================
# 📄 Tabla y descarga CSV
# ===============================
st.dataframe(df_vista, use_container_width=True)

csv_bytes = df_vista.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar CSV filtrado", data=csv_bytes,
                   file_name="afps_metricas_filtrado.csv", mime="text/csv")

# 🚫 Sin descarga Parquet (a pedido)
