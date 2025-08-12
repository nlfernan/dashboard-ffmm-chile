# -*- coding: utf-8 -*-
"""
ACC por Acción — usando parquet ACC y filtros estilo app.py
- Fecha (descendente)
- Tipo de fondo y Nemo con "(Seleccionar todo)"
- Métricas por AFP:
  AUM_FONDO = total_inversion_grupo (suma entre fondos seleccionados)
  AUM_ACCION = inversión en la(s) acción(es) seleccionada(s)
  pct_en_fondo = AUM_ACCION / AUM_FONDO
  delta_pct_vs_total = pct_en_fondo_fila - pct_en_fondo_TOTAL
  delta_aum_vs_total = AUM_FONDO * delta_pct_vs_total
- Vista solo con columnas: afp, AUM_FONDO_MM, AUM_ACCION_MM, pct_en_fondo, delta_pct_vs_total, delta_aum_vs_total_MM
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Rutas candidatas (parquet ACC)
PARQUET_PATHS = [
    Path("app/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("backend/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("data_fuentes/cartera_mensual_ACC.parquet"),
]

TODO = "(Seleccionar todo)"

st.set_page_config(page_title="ACC por Acción — AFP", layout="wide")
st.title("📈 ACC por Acción — por AFP")

# ---------------- Utils de filtros: mismo estilo que app.py ---------------- #
def multiselect_con_todo(label, opciones, key):
    """Multiselect con '(Seleccionar todo)' como primera opción."""
    opciones_mostradas = [TODO] + list(opciones)
    sel = st.multiselect(label, opciones_mostradas, default=[TODO], key=key)
    # Limpieza: si selecciona TODO + otros, quito TODO. Si queda vacío o solo TODO => todos.
    if TODO in sel and len(sel) > 1:
        sel = [v for v in sel if v != TODO]
        st.session_state[key] = sel
    if not sel or sel == [TODO]:
        return list(opciones)
    return sel

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

    # Normalización mínima
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

# ---------------- Carga ---------------- #
parquet_path = encontrar_parquet(PARQUET_PATHS)
if parquet_path is None:
    st.error(f"No encontré el parquet en ninguna de estas rutas: {PARQUET_PATHS}")
    st.stop()

df = cargar(parquet_path)
st.caption(f"Fuente: `{parquet_path}`")

# ---------------- Filtros ---------------- #
# Fecha en orden descendente
fechas_desc = sorted(df["fecha"].unique().tolist(), reverse=True)
fecha_sel = st.selectbox("Fecha", fechas_desc, index=0, key="acc_fecha_desc")

# Tipo de fondo y Nemo con "(Seleccionar todo)"
fondos_opts = sorted(df["tipo_de_fondo"].unique().tolist())
nemos_opts = sorted(df["nemo"].unique().tolist())
col_f, col_n = st.columns(2)
with col_f:
    fondos_sel = multiselect_con_todo("Tipo de fondo", fondos_opts, key="acc_fondos_sel")
with col_n:
    nemos_sel = multiselect_con_todo("Acción (nemo)", nemos_opts, key="acc_nemos_sel")

# ---------------- Subconjunto ---------------- #
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"].isin(fondos_sel))].copy()

# 1) AUM_FONDO desde total_inversion_grupo:
#    primero colapso por (afp,fondo) con el valor único (máx por seguridad), luego sumo entre fondos seleccionados
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

# Merge + métricas por AFP
tab = aum_fondo.merge(aum_accion, on="afp", how="left").fillna({"AUM_ACCION": 0.0})

# 3) % en fondo
tab["pct_en_fondo"] = np.where(tab["AUM_FONDO"] > 0, tab["AUM_ACCION"] / tab["AUM_FONDO"], np.nan)

# 4) Deltas vs TOTAL (primero calculo TOTAL con misma fórmula)
sum_aum_fondo   = tab["AUM_FONDO"].sum()
sum_aum_accion  = tab["AUM_ACCION"].sum()
pct_fondo_total = (sum_aum_accion / sum_aum_fondo) if sum_aum_fondo > 0 else np.nan

tab["delta_pct_vs_total"] = tab["pct_en_fondo"] - pct_fondo_total
tab["delta_aum_vs_total"] = tab["AUM_FONDO"] * tab["delta_pct_vs_total"]

# Fila TOTAL (misma fórmula y deltas en 0 por construcción)
fila_total = pd.DataFrame([{
    "afp": "TOTAL",
    "AUM_FONDO": sum_aum_fondo,
    "AUM_ACCION": sum_aum_accion,
    "pct_en_fondo": pct_fondo_total,
    "delta_pct_vs_total": 0.0,
    "delta_aum_vs_total": 0.0
}])
tab = pd.concat([tab, fila_total], ignore_index=True)

# ---------------- Presentación: solo columnas pedidas ---------------- #
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

st.subheader(f"{' • '.join(['Fecha ' + fecha_sel, 'Fondos: ' + (', '.join(fondos_sel) if len(fondos_sel)<=6 else f'{len(fondos_sel)} seleccionados'), f'Nemos: {len(nemos_sel)} seleccionados'])}")
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
