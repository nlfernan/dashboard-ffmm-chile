# -*- coding: utf-8 -*-
"""
ACC por Acción (solo usa el parquet indicado)
Seleccionás: acción (nemo), fecha y tipo de fondo.
Muestra por AFP:
1) Inversión Total del Fondo (AUM_FONDO)
2) Inversión del Fondo en la acción (AUM_ACCION)
3) % en fondo = 2 / 1
4) Comparativo vs total = (% en fondo) - (AUM_ACCION / total AUM de la acción)
5) AUM relativo = 1 * 4
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# —— Ruta ÚNICA (la que indicaste) ——
PARQUET_PATH = Path(
    r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\cartera_mensual_ACC.parquet"
)

st.set_page_config(page_title="ACC por Acción", layout="wide")
st.title("📈 ACC por Acción — por AFP")

@st.cache_data(show_spinner=True)
def cargar(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No encuentro el parquet: {path}")
    df = pd.read_parquet(path)
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
    df["inversion"] = pd.to_numeric(df["inversion"], errors="coerce")
    return df

df = cargar(PARQUET_PATH)
st.caption(f"Fuente: `{PARQUET_PATH}`")

# —— Filtros (independientes) ——
fechas = sorted(df["fecha"].unique().tolist())
fondos = sorted(df["tipo_de_fondo"].unique().tolist())
nemos  = sorted(df["nemo"].unique().tolist())

c1, c2, c3 = st.columns([1,1,2])
with c1:
    fecha_sel = st.selectbox("Fecha", fechas, index=len(fechas)-1, key="accx_fecha")
with c2:
    idx_e = fondos.index("E") if "E" in fondos else 0
    fondo_sel = st.selectbox("Tipo de fondo", fondos, index=idx_e, key="accx_fondo")
with c3:
    nemo_sel = st.selectbox("Acción (nemo)", nemos, key="accx_nemo")

# —— Subconjunto seleccionado ——
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"] == fondo_sel)].copy()

# 1) AUM total del fondo por AFP (en este parquet)
aum_fondo = (
    base.groupby("afp", as_index=False)["inversion"]
        .sum()
        .rename(columns={"inversion": "AUM_FONDO"})
)

# 2) AUM de la ACCIÓN por AFP
aum_accion = (
    base[base["nemo"] == nemo_sel]
        .groupby("afp", as_index=False)["inversion"]
        .sum()
        .rename(columns={"inversion": "AUM_ACCION"})
)

# Merge + métricas
tab = aum_fondo.merge(aum_accion, on="afp", how="left").fillna({"AUM_ACCION": 0.0})

# 3) % en fondo
tab["pct_en_fondo"] = np.where(tab["AUM_FONDO"] > 0, tab["AUM_ACCION"] / tab["AUM_FONDO"], np.nan)

# Participación total de la acción (en el set filtrado)
total_accion = tab["AUM_ACCION"].sum()
tab["pct_en_total_accion"] = np.where(total_accion > 0, tab["AUM_ACCION"] / total_accion, 0.0)

# 4) Comparativo vs total
tab["comparativo_vs_total"] = tab["pct_en_fondo"] - tab["pct_en_total_accion"]

# 5) AUM relativo
tab["AUM_relativo"] = tab["AUM_FONDO"] * tab["comparativo_vs_total"]

# Orden AFPs (opcional)
orden_afps = ["cap", "cup", "hab", "mod", "pli", "prv", "uno"]
tab["afp"] = tab["afp"].astype(str).str.lower()
if set(orden_afps).issubset(set(tab["afp"].unique())):
    tab = tab.set_index("afp").reindex(orden_afps).reset_index()

# —— Presentación ——
def mm(x): return x / 1_000_000.0

view = tab.copy()
view["AUM_FONDO_MM"]    = view["AUM_FONDO"].apply(mm)
view["AUM_ACCION_MM"]   = view["AUM_ACCION"].apply(mm)
view["AUM_relativo_MM"] = view["AUM_relativo"].apply(mm)

cols_show = [
    "afp",
    "AUM_FONDO_MM",
    "AUM_ACCION_MM",
    "pct_en_fondo",
    "pct_en_total_accion",
    "comparativo_vs_total",
    "AUM_relativo_MM",
]
view = view[cols_show]

st.subheader(f"{nemo_sel} · Fondo {fondo_sel} · {fecha_sel}")
st.dataframe(
    view.style.format({
        "AUM_FONDO_MM": "{:,.0f}",
        "AUM_ACCION_MM": "{:,.0f}",
        "pct_en_fondo": "{:.2%}",
        "pct_en_total_accion": "{:.2%}",
        "comparativo_vs_total": "{:.2%}",
        "AUM_relativo_MM": "{:,.0f}",
    }),
    use_container_width=True
)

# —— Descarga CSV ——
csv = view.rename(columns={
    "afp":"AFP",
    "AUM_FONDO_MM":"Inversión Total Fondo (MM)",
    "AUM_ACCION_MM":"Inversión en Acción (MM)",
    "pct_en_fondo":"% en fondo",
    "pct_en_total_accion":"% en total acción",
    "comparativo_vs_total":"Comparativo vs total",
    "AUM_relativo_MM":"AUM relativo (MM)",
})
st.download_button(
    "⬇️ Descargar CSV",
    data=csv.to_csv(index=False, encoding="utf-8-sig"),
    file_name=f"ACC_{nemo_sel}_{fondo_sel}_{fecha_sel}.csv",
    mime="text/csv",
)
