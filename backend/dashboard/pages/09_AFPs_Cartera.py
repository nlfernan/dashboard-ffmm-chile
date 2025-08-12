# -*- coding: utf-8 -*-
"""
Vista simple por ACCIÓN (nemo) / fecha / tipo de fondo

Métricas por AFP:
1) Inversión Total del Fondo (AUM_FONDO)
2) Inversión Total del Fondo en la acción seleccionada (AUM_ACCION)
3) % en fondo = AUM_ACCION / AUM_FONDO
4) Comparativo vs total = (% en fondo) - (AUM_ACCION / Total AUM de la ACCION en el set)
5) AUM relativo = AUM_FONDO * (comparativo)
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# ---- Ruta a tu parquet maestro ----
PARQUET_PATH = Path(r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\cartera_mensual_unificado.parquet")

st.set_page_config(page_title="ACC por Acción", layout="wide")
st.title("📈 ACC por Acción · Comparativo por AFP")

@st.cache_data(show_spinner=True)
def cargar(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # Normalización mínima
    df["fecha"] = df["fecha"].astype(str)
    df["afp"] = df["afp"].astype(str).str.lower().str.strip()
    df["tipo_de_fondo"] = df["tipo_de_fondo"].astype(str).str.upper().str.strip()
    df["tipo_de_instrumento"] = df["tipo_de_instrumento"].astype(str).str.upper().str.strip()
    # el nemo puede venir con otro nombre, alias rápidos:
    if "nemotecnico_del_instrumento" in df.columns:
        df["nemo"] = df["nemotecnico_del_instrumento"].astype(str).str.upper().str.strip()
    elif "nemo" in df.columns:
        df["nemo"] = df["nemo"].astype(str).str.upper().str.strip()
    else:
        raise KeyError("No encuentro columna de nemotécnico (esperaba 'nemotecnico_del_instrumento' o 'nemo').")
    df["inversion"] = pd.to_numeric(df["inversion"], errors="coerce")
    return df

df = cargar(PARQUET_PATH)

# ---- Filtros ----
fechas = sorted(df["fecha"].unique().tolist())
fondos = sorted(df["tipo_de_fondo"].unique().tolist())
# universo de nemotécnicos disponible
nemos = sorted(df["nemo"].unique().tolist())

c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    fecha_sel = st.selectbox("Fecha", fechas, index=len(fechas)-1)
with c2:
    fondo_sel = st.selectbox("Tipo de fondo", fondos, index=(fondos.index("E") if "E" in fondos else 0))
with c3:
    nemo_sel = st.selectbox("Acción (nemo)", nemos)

st.caption(f"Fuente: {PARQUET_PATH}")

# ---- Subconjunto seleccionado ----
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"] == fondo_sel)].copy()

# 1) AUM total del fondo por AFP (todas las clases de instrumento)
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

# Participación total de la acción (suma en el set)
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

# ---- Presentación ----
def as_mm(x):  # millones
    return x / 1_000_000.0

view = tab.copy()
view["AUM_FONDO_MM"] = view
