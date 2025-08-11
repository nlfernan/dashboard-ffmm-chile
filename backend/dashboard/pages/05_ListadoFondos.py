# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from collections import Counter

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
# 🛡 Normalización mínima (rápida)
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

# RUT (clave del fondo)
alias_col(df, "run_fm", ["rut_fm", "rut_fondo", "id_fondo", "run", "rut"])
df["run_fm"] = df["run_fm"].astype(str).str.strip()

# Administradora
alias_col(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"], default="")
df["nom_adm"] = df["nom_adm"].astype(str).str.strip()

# Nombre del fondo (puede venir con múltiples variantes)
name_col = "nombre_corto" if "nombre_corto" in df.columns else alias_col(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"], default="")
df[name_col] = df[name_col].astype(str).str.strip()

# Venta neta
alias_col(df, "venta_neta_mm", ["venta_neta_mm", "venta_neta", "venta_neta_millones"], default=0.0)
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0.0)

# ===============================
# 🔑 Subset mínimo para agrupar (performance)
# ===============================
df_keys = df[["run_fm", "nom_adm", name_col, "venta_neta_mm"]].copy()

# 🧠 Elegimos un "nombre representativo" por (run_fm, nom_adm): el más frecuente
# Esto evita que múltiples variantes del nombre inflen la cantidad de grupos
def nombre_mas_frecuente(series: pd.Series) -> str:
    # Counter es rápido y robusto
    cnt = Counter(series.dropna().astype(str).str.strip())
    if not cnt:
        return ""
    return cnt.most_common(1)[0][0]

# Primero agregamos venta_neta; por separado calculamos nombre representativo
agr_vn = (
    df_keys.groupby(["run_fm", "nom_adm"], as_index=False, sort=False)["venta_neta_mm"]
          .sum()
)

nombres_rep = (
    df_keys.groupby(["run_fm", "nom_adm"], as_index=False)[name_col]
           .agg(nombre_mas_frecuente)
           .rename(columns={name_col: "nombre_representativo"})
)

ranking = (
    agr_vn.merge(nombres_rep, on=["run_fm", "nom_adm"], how="left")
          .sort_values("venta_neta_mm", ascending=False, kind="stable")
          .reset_index(drop=True)
)

total_fondos = ranking.shape[0]
top_n = min(20, total_fondos)
titulo = f"Top {top_n} Fondos por Venta Neta de {total_fondos} totales" if total_fondos > 0 else "Listado de Fondos Mutuos"
st.subheader(titulo)

# ===============================
# 🌐 URL CMF (link)
# ===============================
def url_cmf(rut: str) -> str:
    base = "https://www.cmfchile.cl/institucional/mercados/entidad.php"
    qs = f"auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI&row=AAAw+cAAhAABP4UAAB&control=svs&pestania=1"
    return f"{base}?{qs}"

ranking["URL CMF"] = ranking["run_fm"].astype(str).map(url_cmf)

# ===============================
# Renombrar / ordenar columnas
# ===============================
ranking = ranking.rename(columns={
    "run_fm": "RUT",
    "nom_adm": "Administradora",
    "nombre_representativo": "Nombre del Fondo",
    "venta_neta_mm": "Venta Neta (MM CLP)"
})

# ===============================
# 🖥️ Mostrar tabla (rápida)
# ===============================
# Mostramos solo Top N en pantalla para no trabar la UI
mostrar = ranking.head(top_n)

st.dataframe(
    mostrar,
    use_container_width=True,
    column_config={
        "RUT": st.column_config.TextColumn("RUT"),
        "Administradora": st.column_config.TextColumn("Administradora"),
        "Nombre del Fondo": st.column_config.TextColumn("Nombre del Fondo"),
        "Venta Neta (MM CLP)": st.column_config.NumberColumn("Venta Neta (MM CLP)", format="%.0f"),
        "URL CMF": st.column_config.LinkColumn("CMF", display_text="CMF ↗︎"),
    },
    hide_index=True,
)

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
