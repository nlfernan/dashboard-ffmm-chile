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

df = st.session_state.get("df_filtrado", st.session_state.df)
if df is None or df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# Limpieza básica
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def _alias(_df, target, candidates, default=np.nan):
    if target in _df.columns and _df[target].notna().any():
        return target
    for c in candidates:
        if c in _df.columns and _df[c].notna().any():
            _df[target] = _df[c]
            return target
    if target not in _df.columns:
        _df[target] = default
    return target

# run_fm
_alias(df, "run_fm", ["rut_fm", "rut_fondo", "id_fondo", "run", "rut"])
df["run_fm"] = df["run_fm"].astype(str).str.strip()

# nom_adm
_alias(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"])
df["nom_adm"] = df["nom_adm"].astype(str).str.strip()

# nombre_corto
_alias(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"], default=pd.NA)
df["nombre_corto"] = df["nombre_corto"].astype(str).str.strip().replace({"": pd.NA})

# venta_neta_mm
_alias(df, "venta_neta_mm", ["venta_neta_mm"], default=0.0)
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0.0)

# ===============================
# Top 20 por (RUT, Adm)
# ===============================
def _nombre_mas_frecuente(s: pd.Series):
    cnt = Counter(s.dropna().astype(str).str.strip())
    return cnt.most_common(1)[0][0] if cnt else pd.NA

agr_vn = (
    df.groupby(["run_fm", "nom_adm"], as_index=False)["venta_neta_mm"]
      .sum()
)
nombres_rep = (
    df.groupby(["run_fm", "nom_adm"], as_index=False)["nombre_corto"]
      .agg(_nombre_mas_frecuente)
      .rename(columns={"nombre_corto": "nombre_representativo"})
)

ranking = (
    agr_vn.merge(nombres_rep, on=["run_fm", "nom_adm"], how="left")
          .sort_values("venta_neta_mm", ascending=False, kind="stable")
          .reset_index(drop=True)
)

# Fallback: si no hay nombre, usar RUT - 
ranking["nombre_representativo"] = ranking["nombre_representativo"].fillna(
    ranking["run_fm"].astype(str) + " - "
)

# URL CMF
def url_cmf(rut: str) -> str:
    return (
        "https://www.cmfchile.cl/institucional/mercados/entidad.php"
        f"?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI&row=AAAw+cAAhAABP4UAAB&control=svs&pestania=1"
    )
ranking["URL CMF"] = ranking["run_fm"].astype(str).map(url_cmf)

# Mostrar
total_fondos = ranking.shape[0]
top_n = min(20, total_fondos)
st.subheader(f"Top {top_n} Fondos por Venta Neta de {total_fondos} totales")

st.dataframe(
    ranking.head(top_n).rename(columns={
        "run_fm": "RUT",
        "nom_adm": "Administradora",
        "nombre_representativo": "Nombre del Fondo",
        "venta_neta_mm": "Venta Neta (MM CLP)"
    }),
    use_container_width=True,
    hide_index=True,
    column_config={
        "URL CMF": st.column_config.LinkColumn("CMF", display_text="CMF ↗︎")
    }
)
