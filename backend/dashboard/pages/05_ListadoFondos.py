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
# 🧹 Normalización mínima
# ===============================
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def _alias(_df: pd.DataFrame, target: str, candidates, default=np.nan):
    if target in _df.columns and _df[target].notna().any():
        return target
    for c in candidates:
        if c in _df.columns and _df[c].notna().any():
            _df[target] = _df[c]
            return target
    if target not in _df.columns:
        _df[target] = default
    return target

# Claves y campos usados
_alias(df, "run_fm", ["rut_fm", "rut_fondo", "id_fondo", "run", "rut"])
df["run_fm"] = df["run_fm"].astype(str).str.strip()

_alias(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"], default="")
df["nom_adm"] = df["nom_adm"].astype(str).str.strip()

_alias(df, "nombre_corto", ["run_fm_nombrecorto", "nombre_fondo", "nombre", "fondo"], default=pd.NA)
df["nombre_corto"] = df["nombre_corto"].astype(str).str.strip().replace({"": pd.NA})

# Fecha: preferimos fecha_dia (para igualar 02_Patrimonio.py), luego fecha_inf_date/fecha/fecha_inf
fecha_col = None
for cand in ["fecha_dia", "fecha_inf_date", "fecha", "fecha_inf"]:
    if cand in df.columns:
        fecha_col = cand
        break

if fecha_col is not None:
    df["fecha_inf_date"] = pd.to_datetime(df[fecha_col], errors="coerce")
else:
    df["fecha_inf_date"] = pd.NaT

# Venta neta en MM
_alias(df, "venta_neta_mm", ["venta_neta_mm", "venta_neta", "venta_neta_millones"], default=0.0)
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0.0)

# Patrimonio neto en MM
_alias(
    df,
    "patrimonio_neto_mm",
    ["patrimonio_neto_mm", "patrimonio_neto", "patrimonio_mm", "aum_mm", "aum", "patrimonio"],
    default=np.nan
)
df["patrimonio_neto_mm"] = pd.to_numeric(df["patrimonio_neto_mm"], errors="coerce")

# Subset mínimo
base = df[["run_fm", "nom_adm", "nombre_corto", "venta_neta_mm", "patrimonio_neto_mm", "fecha_inf_date"]].copy()

# ===============================
# 🧷 Nombre representativo: el de la ÚLTIMA FECHA
# ===============================
tiene_fecha = base["fecha_inf_date"].notna().any()
if tiene_fecha:
    ultima_fecha = base["fecha_inf_date"].max()
    nombres_rep = (
        base.loc[base["fecha_inf_date"] == ultima_fecha]
            .sort_values(["run_fm", "nom_adm"])
            .groupby(["run_fm", "nom_adm"], as_index=False)["nombre_corto"]
            .first()
            .rename(columns={"nombre_corto": "nombre_representativo"})
    )
else:
    ultima_fecha = None
    nombres_rep = (
        base.groupby(["run_fm", "nom_adm"], as_index=False)["nombre_corto"]
            .agg(lambda s: s.dropna().iloc[0] if s.dropna().size else pd.NA)
            .rename(columns={"nombre_corto": "nombre_representativo"})
    )

# ===============================
# 🔢 Agregación por (RUT, Adm) y merge con nombre de última fecha
# ===============================
agr_vn = (
    base.groupby(["run_fm", "nom_adm"], as_index=False, sort=False)["venta_neta_mm"]
        .sum()
)

ranking = (
    agr_vn.merge(nombres_rep, on=["run_fm", "nom_adm"], how="left")
          .sort_values("venta_neta_mm", ascending=False, kind="stable")
          .reset_index(drop=True)
)

# ===============================
# 💰 Patrimonio Neto (MM CLP) del ÚLTIMO DÍA (por fondo y total)
#   -> Igual a 02_Patrimonio.py: SUMA en la última fecha
# ===============================
if tiene_fecha:
    # Patrimonio por fondo (sumado en la última fecha)
    pat_ult_dia = (
        base.loc[base["fecha_inf_date"] == ultima_fecha]
            .groupby(["run_fm", "nom_adm"], as_index=False)["patrimonio_neto_mm"]
            .sum()
            .rename(columns={"patrimonio_neto_mm": "patrimonio_neto_ult_dia_mm"})
    )
    # Total del día (exactamente como tu otro py)
    total_patrimonio_ult_dia = (
        base.loc[base["fecha_inf_date"] == ultima_fecha, "patrimonio_neto_mm"].sum()
    )
else:
    pat_ult_dia = pd.DataFrame(columns=["run_fm", "nom_adm", "patrimonio_neto_ult_dia_mm"])
    total_patrimonio_ult_dia = np.nan

ranking = ranking.merge(pat_ult_dia, on=["run_fm", "nom_adm"], how="left")
ranking["patrimonio_total_ult_dia_mm"] = total_patrimonio_ult_dia

# Fallback final de nombre
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

# ===============================
# 🖥️ Mostrar Top N
# ===============================
total_fondos = ranking.shape[0]
top_n = min(20, total_fondos)

sub = f"Top {top_n} Fondos por Venta Neta de {total_fondos} totales"
if ultima_fecha is not None and pd.notna(ultima_fecha):
    sub += f" · Patrimonio al {ultima_fecha.date():%Y-%m-%d}"
st.subheader(sub)

mostrar = ranking.head(top_n).rename(columns={
    "run_fm": "RUT",
    "nom_adm": "Administradora",
    "venta_neta_mm": "Venta Neta (MM CLP)",
    "nombre_representativo": "Nombre del Fondo",
    "patrimonio_neto_ult_dia_mm": "Patrimonio Neto (MM CLP)",
    "patrimonio_total_ult_dia_mm": "Patrimonio Neto Total (MM CLP)",
})

st.dataframe(
    mostrar[[
        "RUT",
        "Administradora",
        "Venta Neta (MM CLP)",
        "Patrimonio Neto (MM CLP)",
        "Patrimonio Neto Total (MM CLP)",
        "Nombre del Fondo",
        "URL CMF"
    ]],
    use_container_width=True,
    hide_index=True,
    column_config={
        "Venta Neta (MM CLP)": st.column_config.NumberColumn("Venta Neta (MM CLP)", format="%.0f"),
        "Patrimonio Neto (MM CLP)": st.column_config.NumberColumn("Patrimonio Neto (MM CLP)", format="%.0f"),
        "Patrimonio Neto Total (MM CLP)": st.column_config.NumberColumn("Patrimonio Neto Total (MM CLP)", format="%.0f"),
        "URL CMF": st.column_config.LinkColumn("CMF", display_text="CMF ↗︎"),
    },
)
