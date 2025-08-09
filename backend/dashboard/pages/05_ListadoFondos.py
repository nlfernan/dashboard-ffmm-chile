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
# 🛡 Blindaje mínimo (sin partir nombres, mantengo RUT en el nombre si viene)
# ===============================
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def alias_col(d, target, candidates, default=np.nan):
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

# Nombre del fondo (no quito el RUT si ya viene unido)
if "run_fm_nombrecorto" in df.columns:
    nombre_display_col = "run_fm_nombrecorto"   # p.ej. "8676 - Fondo X"
else:
    # fallback: nombre_corto > nombre_fondo > nombre > fondo
    nombre_display_col = alias_col(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"], default="")
    # si no hay nada, armo "RUT - " como mínimo
    df[nombre_display_col] = np.where(
        df[nombre_display_col].notna() & (df[nombre_display_col].astype(str).str.strip() != ""),
        df[nombre_display_col].astype(str),
        df["run_fm"].astype(str) + " - "
    )

# Administradora
alias_col(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"], default="")

# Venta neta
if "venta_neta_mm" not in df.columns:
    if {"aportes_mm", "rescates_mm"}.issubset(df.columns):
        df["venta_neta_mm"] = pd.to_numeric(df["aportes_mm"], errors="coerce").fillna(0) + \
                              pd.to_numeric(df["rescates_mm"], errors="coerce").fillna(0)
    else:
        df["venta_neta_mm"] = 0.0
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0)

# ===============================
# 📊 Ranking por venta neta (rápido, cacheado)
# ===============================
@st.cache_data
def calcular_ranking_numpy(vals: np.ndarray, name_col: str):
    # vals: columnas [run_fm, nom_adm, nombre_display, venta_neta_mm]
    tmp = pd.DataFrame(vals, columns=["run_fm", "nom_adm", name_col, "venta_neta_mm"])
    tmp["venta_neta_mm"] = pd.to_numeric(tmp["venta_neta_mm"], errors="coerce").fillna(0)

    # Agrupo por RUT y sumo venta; tomo el primer nombre/admin del grupo (rápido)
    ranking = (
        tmp.groupby("run_fm", as_index=False)
           .agg({"venta_neta_mm":"sum", "nom_adm":"first", name_col:"first"})
           .sort_values("venta_neta_mm", ascending=False)
    )
    return ranking

vals = df[["run_fm", "nom_adm", nombre_display_col, "venta_neta_mm"]].to_numpy(copy=False)
ranking = calcular_ranking_numpy(vals, nombre_display_col)

# Top 20 o todo
total_fondos = len(ranking)
if total_fondos > 20:
    ranking = ranking.head(20)
    titulo = f"Top 20 Fondos por Venta Neta de {total_fondos} totales"
else:
    titulo = f"Listado de Fondos Mutuos (total: {total_fondos})"
st.subheader(titulo)

# ===============================
# 🌐 URL CMF + orden de columnas solicitado
# ===============================
def url_cmf(rut):
    return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI&row=AAAw+cAAhAABP4UAAB&control=svs&pestania=1"

ranking["URL CMF"] = ranking["run_fm"].astype(str).map(url_cmf)

# Renombro y ordeno columnas EXACTO como pediste
ranking = ranking.rename(columns={
    "run_fm": "RUT",
    "nom_adm": "Administradora",
    nombre_display_col: "Nombre del Fondo",
    "venta_neta_mm": "Venta Neta (MM CLP)"
})[["RUT", "Administradora", "Nombre del Fondo", "Venta Neta (MM CLP)", "URL CMF"]]

# Formato miles solo para mostrar
ranking["Venta Neta (MM CLP)"] = ranking["Venta Neta (MM CLP)"].apply(lambda x: f"{x:,.0f}".replace(",", "."))

# ===============================
# 🖥️ Mostrar tabla (HTML para links)
# ===============================
MAX_HTML_FILAS = 2000
mostrar = ranking.head(MAX_HTML_FILAS)
st.markdown(mostrar.to_html(index=False, escape=False), unsafe_allow_html=True)

# ===============================
# 📥 Descargar CSV (de los datos filtrados, no solo top)
# ===============================
MAX_FILAS = 50_000
st.markdown("### ⬇️ Descargar datos filtrados")
st.caption(f"🔢 Total de filas disponibles: {df.shape[0]:,}")

if df.shape[0] > MAX_FILAS:
    st.warning(f"⚠️ La descarga está limitada a {MAX_FILAS:,} filas. Aplica más filtros (actual: {df.shape[0]:,}).")
else:
    @st.cache_data(hash_funcs={pd.DataFrame: lambda _: None})
    def generar_csv(_df):
        return _df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "⬇️ Descargar CSV",
        data=generar_csv(df),
        file_name="ffmm_filtrado.csv",
        mime="text/csv",
        use_container_width=True
    )
