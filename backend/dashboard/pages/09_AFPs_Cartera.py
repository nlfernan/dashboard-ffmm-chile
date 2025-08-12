# -*- coding: utf-8 -*-
"""
ACC por Acción (usa parquet ACC)
Seleccionás: acción (nemo), fecha y tipo de fondo.
Muestra por AFP:
1) AUM_FONDO = total_inversion_grupo (del parquet ACC)
2) AUM_ACCION = inversión del fondo en la acción (nemo)
3) % en fondo = 2 / 1
4) Comparativo vs total = (% en fondo) - (AUM_ACCION / total AUM de la acción)
5) AUM relativo = 1 * 4
Incluye fila TOTAL con la MISMA fórmula.
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Rutas candidatas (nombre correcto del archivo)
PARQUET_PATHS = [
    Path("app/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("backend/data_fuentes/cartera_mensual_ACC.parquet"),
    Path("data_fuentes/cartera_mensual_ACC.parquet"),
]

st.set_page_config(page_title="ACC por Acción", layout="wide")
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

    # Normalización mínima
    df["fecha"] = df["fecha"].astype(str)
    df["afp"] = df["afp"].astype(str).str.lower().str.strip()
    df["tipo_de_fondo"] = df["tipo_de_fondo"].astype(str).str.upper().str.strip()
    if "nemotecnico_del_instrumento" in df.columns:
        df["nemo"] = df["nemotecnico_del_instrumento"].astype(str).str.upper().str.strip()
    elif "nemo" in df.columns:
        df["nemo"] = df["nemo"].astype(str).str.upper().str.strip()
    else:
        raise KeyError("Falta columna de nemotécnico ('nemotecnico_del_instrumento' o 'nemo').")

    df["inversion"] = pd.to_numeric(df["inversion"], errors="coerce")
    df["total_inversion_grupo"] = pd.to_numeric(df["total_inversion_grupo"], errors="coerce")
    return df

parquet_path = encontrar_parquet(PARQUET_PATHS)
if parquet_path is None:
    st.error(f"No encontré el parquet en ninguna de estas rutas: {PARQUET_PATHS}")
    st.stop()

df = cargar(parquet_path)
st.caption(f"Fuente: `{parquet_path}`")

# ===== FILTROS (independientes) =====
fechas = sorted(df["fecha"].unique().tolist())
fondos = sorted(df["tipo_de_fondo"].unique().tolist())
nemos  = sorted(df["nemo"].unique().tolist())

c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    fecha_sel = st.selectbox("Fecha", fechas, index=len(fechas)-1, key="accx_fecha")
with c2:
    idx_e = fondos.index("E") if "E" in fondos else 0
    fondo_sel = st.selectbox("Tipo de fondo", fondos, index=idx_e, key="accx_fondo")
with c3:
    nemo_sel = st.selectbox("Acción (nemo)", nemos, key="accx_nemo")

# ===== SUBCONJUNTO SELECCIONADO =====
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"] == fondo_sel)].copy()

# 1) AUM_FONDO desde total_inversion_grupo (si hay varias filas por AFP, tomamos el máx: debe ser constante)
aum_fondo = (
    base.groupby("afp", as_index=False)["total_inversion_grupo"]
        .max()
        .rename(columns={"total_inversion_grupo": "AUM_FONDO"})
)

# 2) AUM de la ACCIÓN (nemo) por AFP
aum_accion = (
    base[base["nemo"] == nemo_sel]
        .groupby("afp", as_index=False)["inversion"]
        .sum()
        .rename(columns={"inversion": "AUM_ACCION"})
)

# Merge + métricas por AFP
tab = aum_fondo.merge(aum_accion, on="afp", how="left").fillna({"AUM_ACCION": 0.0})

# 3) % en fondo
tab["pct_en_fondo"] = np.where(tab["AUM_FONDO"] > 0, tab["AUM_ACCION"] / tab["AUM_FONDO"], np.nan)

# 4) % participación en el total de la acción del set
total_accion = tab["AUM_ACCION"].sum()
tab["pct_en_total_accion"] = np.where(total_accion > 0, tab["AUM_ACCION"] / total_accion, 0.0)

# 5) Comparativo vs total y AUM relativo
tab["comparativo_vs_total"] = tab["pct_en_fondo"] - tab["pct_en_total_accion"]
tab["AUM_relativo"] = tab["AUM_FONDO"] * tab["comparativo_vs_total"]

# ===== FILA TOTAL (MISMA FÓRMULA) =====
sum_aum_fondo   = tab["AUM_FONDO"].sum()
sum_aum_accion  = tab["AUM_ACCION"].sum()
pct_fondo_total = (sum_aum_accion / sum_aum_fondo) if sum_aum_fondo > 0 else np.nan
pct_total_acc   = 1.0 if sum_aum_accion > 0 else 0.0
comparativo_tot = (pct_fondo_total if pd.notna(pct_fondo_total) else 0.0) - pct_total_acc
aum_rel_tot     = sum_aum_fondo * comparativo_tot

fila_total = pd.DataFrame([{
    "afp": "TOTAL",
    "AUM_FONDO": sum_aum_fondo,
    "AUM_ACCION": sum_aum_accion,
    "pct_en_fondo": pct_fondo_total,
    "pct_en_total_accion": pct_total_acc,
    "comparativo_vs_total": comparativo_tot,
    "AUM_relativo": aum_rel_tot
}]])

tab = pd.concat([tab, fila_total], ignore_index=True)

# ===== PRESENTACIÓN =====
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

# ===== DESCARGA CSV =====
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
