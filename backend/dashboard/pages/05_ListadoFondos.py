# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
import re

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("📜 Listado de Fondos Mutuos")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)

if df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# ===============================
# 🛡 Blindaje mínimo de columnas requeridas para el ranking
# ===============================
# Normalizo nombres a minúsculas por si el parquet vino distinto
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def _alias(_df, target, candidates):
    if target in _df.columns:
        return
    for c in candidates:
        if c in _df.columns:
            _df[target] = _df[c]
            return

# 1) nombre_corto → derivar desde run_fm_nombrecorto o aliases (split tolerante)
if "nombre_corto" not in df.columns or df["nombre_corto"].isna().all() or (df["nombre_corto"].astype(str).str.strip() == "").all():
    if "run_fm_nombrecorto" in df.columns:
        parts = df["run_fm_nombrecorto"].astype(str).str.split(r"\s*-\s*", n=1, regex=True, expand=True)
        if parts.shape[1] == 2:
            if "run_fm" not in df.columns or df["run_fm"].isna().all() or (df["run_fm"].astype(str).str.strip() == "").all():
                df["run_fm"] = parts[0]
            df["nombre_corto"] = parts[1]
        else:
            _alias(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"])
    else:
        _alias(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"])
    if "nombre_corto" not in df.columns:
        df["nombre_corto"] = np.nan

# 2) run_fm → derivar si falta
if "run_fm" not in df.columns or df["run_fm"].isna().all() or (df["run_fm"].astype(str).str.strip() == "").all():
    if "run_fm_nombrecorto" in df.columns:
        df["run_fm"] = df["run_fm_nombrecorto"].astype(str).str.split(r"\s*-\s*", n=1, regex=True, expand=True)[0]
    else:
        _alias(df, "run_fm", ["run", "rut_fm", "rut_fondo", "id_fondo"])
    if "run_fm" not in df.columns:
        df["run_fm"] = ""

# 3) nom_adm → alias + limpieza (como en tu ETL)
_alias(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"])
if "nom_adm" not in df.columns:
    df["nom_adm"] = np.nan
else:
    df["nom_adm"] = (
        df["nom_adm"].astype(str)
        .str.replace("  ", " ", regex=False)
        .str.replace("ADMINISTRADORA GENERAL DE FONDOS", "", regex=False)
        .str.replace("S.A.", "", regex=False)
        .str.replace("ASSET MANAGEMENT", "AM", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    ).replace({"": np.nan})

# 4) venta_neta_mm → si falta, aportes + rescates
if "venta_neta_mm" not in df.columns:
    if {"aportes_mm", "rescates_mm"}.issubset(df.columns):
        df["venta_neta_mm"] = pd.to_numeric(df["aportes_mm"], errors="coerce").fillna(0) + \
                              pd.to_numeric(df["rescates_mm"], errors="coerce").fillna(0)
    else:
        df["venta_neta_mm"] = 0.0
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0)

# Validación mínima antes de armar el ranking
req_cols = ["run_fm", "venta_neta_mm"]
faltan = [c for c in req_cols if c not in df.columns]
if faltan:
    st.error(f"Faltan columnas para el ranking: {faltan}")
    st.stop()

# ===============================
# 📊 Ranking por venta neta (cache estable, por RUT)
# ===============================
@st.cache_data
def calcular_ranking(tab):
    print("🔄 Recalculando ranking de fondos...")

    base = tab[["run_fm", "venta_neta_mm", "nombre_corto", "nom_adm"]].copy()
    base["venta_neta_mm"] = pd.to_numeric(base["venta_neta_mm"], errors="coerce").fillna(0)

    # Sumamos por RUT
    suma = (
        base.groupby("run_fm", as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
    )

    # Nombre y admin: modo (o primero no nulo) por RUT
    def _modo_o_primero(s):
        s = s.dropna()
        if s.empty:
            return np.nan
        m = s.mode(dropna=True)
        return m.iat[0] if not m.empty else s.iloc[0]

    nombres = base.dropna(subset=["nombre_corto"]).groupby("run_fm")["nombre_corto"].agg(_modo_o_primero)
    admins  = base.dropna(subset=["nom_adm"]).groupby("run_fm")["nom_adm"].agg(_modo_o_primero)

    ranking = suma.merge(nombres, on="run_fm", how="left").merge(admins, on="run_fm", how="left")

    # Si aún falta nombre, relleno con string seguro
    ranking["nombre_corto"] = ranking["nombre_corto"].fillna("(Sin nombre)")
    ranking["nom_adm"] = ranking["nom_adm"].fillna("(Sin adm)")

    return ranking

ranking = calcular_ranking(df)

# Determinar si mostrar top 20 o todo
total_fondos = ranking.shape[0]
if total_fondos > 20:
    ranking = ranking.nlargest(20, "venta_neta_mm")
    titulo = f"Top 20 Fondos por Venta Neta de {total_fondos} totales"
else:
    titulo = f"Listado de Fondos Mutuos (total: {total_fondos})"

st.subheader(titulo)

# ===============================
# 🌐 Agregar URL CMF (con el nuevo formato)
# ===============================
def generar_url_cmf(rut):
    return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI&row=AAAw+cAAhAABP4UAAB&control=svs&pestania=1"

ranking["URL CMF"] = ranking["run_fm"].astype(str).apply(generar_url_cmf)

# Formatear columnas para mostrar
ranking = ranking.rename(columns={
    "run_fm": "RUT",
    "nombre_corto": "Nombre del Fondo",
    "nom_adm": "Administradora",
    "venta_neta_mm": "Venta Neta (MM CLP)"
})

ranking["Venta Neta (MM CLP)"] = ranking["Venta Neta (MM CLP)"].apply(lambda x: f"{x:,.0f}".replace(",", "."))

# Convertir URL a link HTML
ranking["URL CMF"] = ranking["URL CMF"].apply(lambda x: f'<a href="{x}" target="_blank">Ver en CMF</a>')

# ===============================
# 🖥️ Mostrar tabla como HTML (con límite de filas)
# ===============================
MAX_HTML_FILAS = 2000
if len(ranking) > MAX_HTML_FILAS:
    st.info(f"Mostrando solo las primeras {MAX_HTML_FILAS:,} filas para mejorar la performance.")
    mostrar = ranking.head(MAX_HTML_FILAS)
else:
    mostrar = ranking

st.markdown(mostrar.to_html(index=False, escape=False), unsafe_allow_html=True)

# ===============================
# 📥 Descargar CSV
# ===============================
MAX_FILAS = 50_000
st.markdown("### ⬇️ Descargar datos filtrados")
st.caption(f"🔢 Total de filas disponibles: {df.shape[0]:,}")

if df.shape[0] > MAX_FILAS:
    st.warning(f"⚠️ La descarga está limitada a {MAX_FILAS:,} filas. Aplica más filtros para reducir el tamaño (actual: {df.shape[0]:,} filas).")
else:
    @st.cache_data(hash_funcs={pd.DataFrame: lambda _: None})
    def generar_csv(_df):
        return _df.to_csv(index=False).encode("utf-8-sig")

    csv_data = generar_csv(df)
    st.download_button(
        label="⬇️ Descargar CSV",
        data=csv_data,
        file_name="ffmm_filtrado.csv",
        mime="text/csv"
    )
