# -*- coding: utf-8 -*-
"""
ACC por Acción — AFP (solo parquet ACC) con filtros 'Seleccionar todo'
- Fecha (descendente)
- Tipo de fondo y Nemo con botón 'Seleccionar todo' (sin modificar session_state después del widget)
- AUM_FONDO = total_inversion_grupo
- Columnas visibles: afp, AUM_FONDO_MM, AUM_ACCION_MM, pct_en_fondo, delta_pct_vs_total, delta_aum_vs_total_MM
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Parquet ACC en rutas candidatas
PARQUET_PATHS = [
    Path("app/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("backend/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("data_fuentes/cartera_mensual_ACC.parquet"),
]

st.set_page_config(page_title="ACC por Acción — AFP", layout="wide")
st.title("📈 ACC por Acción — por AFP")

def encontrar_parquet(rutas):
    for p in rutas:
        if p.exists():
            return p
    return None

@st.cache_data(show_spinner=True)
def cargar(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    req = {"fecha", "afp", "tipo_de_fondo", "inversion", "total_inversion_grupo"}
    faltan = [c for c in req if c not in df.columns]
    if faltan:
        raise KeyError(f"Faltan columnas requeridas en el parquet: {faltan}")

    df["fecha"] = df["fecha"].astype(str)
    df["afp"] = df["afp"].astype(str).str.lower().str.strip()
    df["tipo_de_fondo"] = df["tipo_de_fondo"].astype(str).str.upper().str.strip()
    # nemo
    if "nemotecnico_del_instrumento" in df.columns:
        df["nemo"] = df["nemotecnico_del_instrumento"].astype(str).str.upper().str.strip()
    elif "nemo" in df.columns:
        df["nemo"] = df["nemo"].astype(str).str.upper().str.strip()
    else:
        raise KeyError("Falta columna de nemotécnico ('nemotecnico_del_instrumento' o 'nemo').")
    # numéricos
    df["inversion"] = pd.to_numeric(df["inversion"], errors="coerce")
    df["total_inversion_grupo"] = pd.to_numeric(df["total_inversion_grupo"], errors="coerce")
    return df

parquet_path = encontrar_parquet(PARQUET_PATHS)
if parquet_path is None:
    st.error(f"No encontré el parquet en ninguna de estas rutas: {PARQUET_PATHS}")
    st.stop()

df = cargar(parquet_path)
st.caption(f"Fuente: `{parquet_path}`")

# ---------- Helpers UI: botón 'Seleccionar todo' sin tocar session_state post-widget ----------
def multiselect_con_boton_todo(label: str, opciones: list, key: str):
    """
    Dibuja primero el botón 'Seleccionar todo'. Si se hace click,
    setea session_state[key] y hace st.rerun() ANTES de crear el multiselect.
    Nunca modifica session_state después del widget.
    """
    # init (primera carga)
    if key not in st.session_state:
        st.session_state[key] = opciones[:]  # por defecto: todos

    c1, c2 = st.columns([4, 1])
    with c2:
        if st.button("Seleccionar todo", key=f"{key}_todo"):
            st.session_state[key] = opciones[:]
            st.rerun()  # vuelve a renderizar; el multiselect nace con todos

    with c1:
        sel = st.multiselect(label, opciones, default=st.session_state[key], key=key)

    # Si quedó vacío, forzamos todos (pero sin reescribir post-widget)
    if not sel:
        sel = opciones[:]
    return sel

# ---------- Filtros ----------
# Fecha descendente
fechas_desc = sorted(df["fecha"].unique().tolist(), reverse=True)
fecha_sel = st.selectbox("Fecha", fechas_desc, index=0, key="acc_fecha_desc")

# Tipo de fondo y Nemo con botón 'Seleccionar todo'
fondos_opts = sorted(df["tipo_de_fondo"].unique().tolist())
nemos_opts = sorted(df["nemo"].unique().tolist())
col_f, col_n = st.columns(2)
with col_f:
    fondos_sel = multiselect_con_boton_todo("Tipo de fondo", fondos_opts, key="acc_fondos_sel")
with col_n:
    nemos_sel = multiselect_con_boton_todo("Acción (nemo)", nemos_opts, key="acc_nemos_sel")

# ---------- Subconjunto ----------
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"].isin(fondos_sel))].copy()

# 1) AUM_FONDO desde total_inversion_grupo:
aum_fondo_por_fondo = (
    base.groupby(["afp", "tipo_de_fondo"], as_index=False)["total_inversion_grupo"]
        .max()
)
aum_fondo = (
    aum_fondo_por_fondo.groupby("afp", as_index=False)["total_inversion_grupo"]
        .sum()
        .rename(columns={"total_inversion_grupo": "AUM_FONDO"})
)

# 2) AUM de la(s) ACCIÓN(es) seleccionada(s)
aum_accion = (
    base[base["nemo"].isin(nemos_sel)]
        .groupby("afp", as_index=False)["inversion"]
        .sum()
        .rename(columns={"inversion": "AUM_ACCION"})
)

# Merge + métricas
tab = aum_fondo.merge(aum_accion, on="afp", how="left").fillna({"AUM_ACCION": 0.0})

# 3) % en fondo
tab["pct_en_fondo"] = np.where(tab["AUM_FONDO"] > 0, tab["AUM_ACCION"] / tab["AUM_FONDO"], np.nan)

# Fila TOTAL (misma fórmula) y deltas vs TOTAL
sum_aum_fondo   = tab["AUM_FONDO"].sum()
sum_aum_accion  = tab["AUM_ACCION"].sum()
pct_fondo_total = (sum_aum_accion / sum_aum_fondo) if sum_aum_fondo > 0 else np.nan

tab["delta_pct_vs_total"] = tab["pct_en_fondo"] - pct_fondo_total
tab["delta_aum_vs_total"] = tab["AUM_FONDO"] * tab["delta_pct_vs_total"]

fila_total = pd.DataFrame([{
    "afp": "TOTAL",
    "AUM_FONDO": sum_aum_fondo,
    "AUM_ACCION": sum_aum_accion,
    "pct_en_fondo": pct_fondo_total,
    "delta_pct_vs_total": 0.0,
    "delta_aum_vs_total": 0.0,
}])
tab = pd.concat([tab, fila_total], ignore_index=True)

# ---------- Presentación: solo columnas pedidas ----------
def mm(x): return x / 1_000_000.0

view = tab.copy()
view["AUM_FONDO_MM"]          = view["AUM_FONDO"].apply(mm)
view["AUM_ACCION_MM"]         = view["AUM_ACCION"].apply(mm)
view["delta_aum_vs_total_MM"] = view["delta_aum_vs_total"].apply(mm)

columnas_finales = [
    "afp",
    "AUM_FONDO_MM",
    "AUM_ACCION_MM",
    "pct_en_fondo",
    "delta_pct_vs_total",
    "delta_aum_vs_total_MM",
]

st.subheader(f"Fecha {fecha_sel} • Fondos: {', '.join(fondos_sel) if len(fondos_sel)<=6 else f'{len(fondos_sel)} seleccionados'} • Nemos: {len(nemos_sel)} seleccionados")
st.dataframe(
    view[columnas_finales].style.format({
        "AUM_FONDO_MM": "{:,.0f}",
        "AUM_ACCION_MM": "{:,.0f}",
        "pct_en_fondo": "{:.2%}",
        "delta_pct_vs_total": "{:.2%}",
        "delta_aum_vs_total_MM": "{:,.0f}",
    }),
    use_container_width=True
)

# Descarga CSV (solo lo visible)
csv = view[columnas_finales].rename(columns={
    "afp": "AFP",
    "AUM_FONDO_MM": "Inversión Total Fondo (MM)",
    "AUM_ACCION_MM": "Inversión en Acción (MM)",
    "pct_en_fondo": "% en fondo",
    "delta_pct_vs_total": "Δ % vs total",
    "delta_aum_vs_total_MM": "Δ AUM vs total (MM)",
})
st.download_button(
    "⬇️ Descargar CSV",
    data=csv.to_csv(index=False, encoding="utf-8-sig"),
    file_name=f"ACC_{fecha_sel}.csv",
    mime="text/csv",
)
