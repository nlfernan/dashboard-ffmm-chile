# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("📜 Listado de Fondos Mutuos")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)
if df is None or df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# ===============================
# 🛡 Normalización mínima
# ===============================
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def alias_col(d: pd.DataFrame, target: str, candidates, default=np.nan) -> str:
    if target in d.columns:
        return target
    for c in candidates:
        if c in d.columns:
            d[target] = d[c]
            return target
    d[target] = default
    return target

# RUT
alias_col(df, "run_fm", ["rut_fm", "rut_fondo", "id_fondo", "run", "rut"])
df["run_fm"] = df["run_fm"].astype(str)

# Nombre del fondo (si no hay, muestro "RUT - ")
if "run_fm_nombrecorto" in df.columns:
    nombre_display_col = "run_fm_nombrecorto"
else:
    nombre_display_col = alias_col(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"], default="")
    df[nombre_display_col] = np.where(
        df[nombre_display_col].notna() & (df[nombre_display_col].astype(str).str.strip() != ""),
        df[nombre_display_col].astype(str),
        df["run_fm"].astype(str) + " - "
    )

# Administradora
alias_col(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"], default="")

# Venta neta (usar directamente)
alias_col(df, "venta_neta_mm", ["venta_neta_mm"], default=0.0)
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0)

# ===============================
# ⚡ Ranking directo (sin groupby)
# ===============================
cols_base = ["run_fm", "nom_adm", nombre_display_col, "venta_neta_mm"]
ranking = df.loc[:, cols_base].copy()
ranking.sort_values("venta_neta_mm", ascending=False, inplace=True, kind="stable")

total_filas = len(ranking)
top_n = 20 if total_filas > 20 else total_filas
titulo = f"Top {top_n} Fondos por Venta Neta de {total_filas} totales" if total_filas > 0 else "Listado de Fondos Mutuos"
st.subheader(titulo)

# ===============================
# 🌐 URL CMF clickeable
# ===============================
def url_cmf(rut: str) -> str:
    base = "https://www.cmfchile.cl/institucional/mercados/entidad.php"
    qs = f"auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI&row=AAAw+cAAhAABP4UAAB&control=svs&pestania=1"
    return f"{base}?{qs}"

ranking["URL CMF"] = (
    ranking["run_fm"]
    .astype(str)
    .map(lambda r: f'<a href="{url_cmf(r)}" target="_blank" rel="noopener noreferrer">CMF ↗︎</a>')
)

# ===============================
# Renombrar / ordenar columnas y formateo
# ===============================
ranking = ranking.rename(columns={
    "run_fm": "RUT",
    "nom_adm": "Administradora",
    nombre_display_col: "Nombre del Fondo",
    "venta_neta_mm": "Venta Neta (MM CLP)"
})

# formateo rápido de miles para mostrar
ranking["Venta Neta (MM CLP)"] = pd.to_numeric(ranking["Venta Neta (MM CLP)"], errors="coerce").fillna(0).round(0)
ranking["Venta Neta (MM CLP)"] = ranking["Venta Neta (MM CLP)"].map(lambda x: f"{int(x):,}".replace(",", "."))

# ===============================
# 🖥️ Mostrar tabla (HTML para links)
# ===============================
MAX_HTML_FILAS = 2000
mostrar = ranking.head(min(top_n, MAX_HTML_FILAS))
st.markdown(mostrar.to_html(index=False, escape=False), unsafe_allow_html=True)

# ===============================
# 📥 Descargar CSV (de los datos filtrados completos)
# ===============================
MAX_FILAS = 50_000
st.markdown("### ⬇️ Descargar datos filtrados")
st.caption(f"🔢 Total de filas disponibles: {df.shape[0]:,}")

if df.shape[0] > MAX_FILAS:
    st.warning(f"⚠️ La descarga está limitada en {MAX_FILAS:,} filas. Aplica más filtros (actual: {df.shape[0]:,}).")
else:
    @st.cache_data(hash_funcs={pd.DataFrame: lambda _: None})
    def generar_csv(_df: pd.DataFrame) -> bytes:
        return _df.to_csv(index=False).encode("utf-8-sig")

    st.download_button(
        "⬇️ Descargar CSV",
        data=generar_csv(df),
        file_name="ffmm_filtrado.csv",
        mime="text/csv",
        use_container_width=True
    )
