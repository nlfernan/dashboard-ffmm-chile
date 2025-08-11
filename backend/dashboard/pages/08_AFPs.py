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

# Fecha única
if info["col_fecha"]:
    fechas_disponibles = sorted(df[info["col_fecha"]].dropna().unique())
    with col1:
        fecha_sel = st.selectbox("Fecha", options=fechas_disponibles, index=len(fechas_disponibles) - 1)
else:
    fecha_sel = None

# AFP / Administradora
if info["col_afp"]:
    vals_afp = sorted([v for v in df[info["col_afp"]].dropna().unique()])
    with col2:
        sel_afp = st.multiselect("AFP / Administradora", options=vals_afp, default=vals_afp)
else:
    sel_afp = None

# Fondo / Serie
if info["col_fondo"]:
    vals_fondo = sorted([v for v in df[info["col_fondo"]].dropna().unique()])
    with col3:
        sel_fondo = st.multiselect("Fondo / Serie", options=vals_fondo, default=vals_fondo)
else:
    sel_fondo = None

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

st.success(f"Registros filtrados: {len(df_filtrado):,}")

# ===============================
# 📄 Tabla y descargas
# ===============================
st.dataframe(df_filtrado, use_container_width=True)

csv_bytes = df_filtrado.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar CSV filtrado", data=csv_bytes,
                   file_name="afps_metricas_filtrado.csv", mime="text/csv")

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
    buffer = io.BytesIO()
    df_filtrado.to_parquet(buffer, index=False)
    st.download_button("⬇️ Descargar Parquet filtrado", data=buffer.getvalue(),
                       file_name="afps_metricas_filtrado.parquet", mime="application/octet-stream")
except Exception:
    st.caption("Para exportar Parquet instalá pyarrow en el entorno de la app.")
