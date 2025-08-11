# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
from pathlib import Path

st.title("📊 Vista AFPs")

# ===============================
# 🔧 Rutas candidatas (primero tu Windows)
# ===============================
RUTAS_CANDIDATAS = [
    r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\vc_combined_largo.parquet",  # tu PC
    "/app/data_fuentes/vc_combined_largo.parquet",           # Railway (backend/data_fuentes)
    "/app/dashboard/backend/data_fuentes/vc_combined_largo.parquet",
    "/app/dashboard/data_fuentes/vc_combined_largo.parquet",
    "backend/data_fuentes/vc_combined_largo.parquet",
    "data_fuentes/vc_combined_largo.parquet",
]

MOSTRAR_MAX_FILAS = 5000

def _primera_ruta_existente(candidatas):
    for p in candidatas:
        if os.path.exists(p):
            return p
    return None

@st.cache_data(show_spinner="Cargando datos de AFPs…")
def cargar_parquet(paths):
    ruta = _primera_ruta_existente(paths)
    if ruta is None:
        return None, pd.DataFrame()
    df = pd.read_parquet(ruta)
    # normalización mínima
    df.columns = df.columns.str.strip()
    return ruta, df

ruta_elegida, df = cargar_parquet(RUTAS_CANDIDATAS)

if df.empty:
    st.error("No encontré el parquet de AFPs en ninguna ruta candidata.")
    with st.expander("Diagnóstico de rutas"):
        for p in RUTAS_CANDIDATAS:
            base = os.path.dirname(p) or "."
            st.write(f"🔎 Ruta candidata: `{p}`")
            st.write(f"📂 Carpeta: `{os.path.abspath(base)}`")
            try:
                if os.path.isdir(base):
                    st.write("🗂 Archivos en carpeta:")
                    st.write(os.listdir(base))
                else:
                    st.write("⚠️ La carpeta no existe.")
            except Exception as e:
                st.write(f"Error listando: {e}")
    st.stop()

st.caption(f"Fuente: `{ruta_elegida}`")

# ===============================
# 🧽 Filtros básicos (opcionales)
# Detecta columnas típicas
# ===============================
cols = df.columns.str.lower().tolist()
c_admin = next((c for c in df.columns if c.lower() in {"administradora","adm","afp","nombre_afp"}), None)
c_fondo = next((c for c in df.columns if c.lower() in {"fondo","nombre_fondo"}), None)
c_fecha = next((c for c in df.columns if c.lower().startswith("fecha")), None)

with st.expander("Filtros", expanded=False):
    admin_sel = None
    fondo_sel = None
    fecha_rng = None

    if c_admin:
        admin_sel = st.selectbox(
            "Administradora",
            ["(Todas)"] + sorted(df[c_admin].dropna().astype(str).unique().tolist())
        )
    if c_fondo:
        fondo_sel = st.selectbox(
            "Fondo",
            ["(Todos)"] + sorted(df[c_fondo].dropna().astype(str).unique().tolist())
        )
    if c_fecha:
        df[c_fecha] = pd.to_datetime(df[c_fecha], errors="coerce")
        min_f, max_f = df[c_fecha].min(), df[c_fecha].max()
        if pd.notna(min_f) and pd.notna(max_f):
            fecha_rng = st.slider(
                "Rango de fechas",
                min_value=min_f.to_pydatetime(),
                max_value=max_f.to_pydatetime(),
                value=((max_f - pd.Timedelta(days=90)).to_pydatetime(), max_f.to_pydatetime())
            )

# ===============================
# 🔎 Aplicar filtros
# ===============================
df_filtrado = df.copy()
if c_admin and admin_sel and admin_sel != "(Todas)":
    df_filtrado = df_filtrado[df_filtrado[c_admin].astype(str) == admin_sel]
if c_fondo and fondo_sel and fondo_sel != "(Todos)":
    df_filtrado = df_filtrado[df_filtrado[c_fondo].astype(str) == fondo_sel]
if c_fecha and fecha_rng:
    desde, hasta = fecha_rng
    mask = (df_filtrado[c_fecha] >= pd.Timestamp(desde)) & (df_filtrado[c_fecha] <= pd.Timestamp(hasta))
    df_filtrado = df_filtrado[mask]

# ===============================
# 📊 KPIs rápidos (opcional)
# ===============================
with st.expander("KPIs rápidos", expanded=False):
    num_cols = df_filtrado.select_dtypes(include=["number"]).columns.tolist()
    if num_cols:
        sel = st.multiselect("Columnas a sumar", num_cols, default=num_cols[:min(3,len(num_cols))])
        if sel:
            st.write(df_filtrado[sel].sum(numeric_only=True).to_frame("Total").T)

# ===============================
# 🧾 Tabla y descarga
# ===============================
st.subheader("Datos AFPs")
n = len(df_filtrado)
if n > MOSTRAR_MAX_FILAS:
    st.caption(f"Mostrando primeras {MOSTRAR_MAX_FILAS:,} de {n:,} filas.")
    df_show = df_filtrado.head(MOSTRAR_MAX_FILAS)
else:
    df_show = df_filtrado

st.dataframe(df_show, use_container_width=True)

st.download_button(
    "⬇️ Descargar CSV (AFPs filtrado)",
    df_filtrado.to_csv(index=False).encode("utf-8"),
    "afps_filtrado.csv",
    "text/csv"
)

st.success("Listo. Carga establecida con ruta local o Railway, según disponibilidad.")
