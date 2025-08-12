# -*- coding: utf-8 -*-
"""
ACC por Acción (usa parquet ACC)
- Filtros: Fecha (desc), Tipo de fondo (multi con 'Seleccionar todo'), Nemo (multi con 'Seleccionar todo')
- Métricas por AFP:
  AUM_FONDO = total_inversion_grupo (suma por AFP de los fondos seleccionados)
  AUM_ACCION = inversión en la(s) acción(es) seleccionada(s)
  pct_en_fondo = AUM_ACCION / AUM_FONDO
  delta_pct_vs_total = pct_en_fondo_fila - pct_en_fondo_TOTAL
  delta_aum_vs_total = AUM_FONDO * delta_pct_vs_total
- Muestra solo columnas: afp, AUM_FONDO_MM, AUM_ACCION_MM, pct_en_fondo, delta_pct_vs_total, delta_aum_vs_total_MM
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# Rutas candidatas
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
    # Normalización
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

# ==== UI helpers ====
def multiselect_con_todo(label, opciones, key):
    """Multiselect con botón 'Seleccionar todo' al estilo de la app."""
    c1, c2 = st.columns([4, 1])
    with c1:
        sel = st.multiselect(label, opciones, default=opciones, key=key)
    with c2:
        if st.button("Seleccionar todo", key=key+"_todo"):
            sel = opciones[:]  # todas
            st.session_state[key] = sel
    return sel

# ==== FILTROS ====
# Fecha descendente
fechas_desc = sorted(df["fecha"].unique().tolist(), reverse=True)
fecha_sel = st.selectbox("Fecha", fechas_desc, index=0, key="accx_fecha_desc")

# Fondos y Nemos (multi + seleccionar todo)
fondos_opts = sorted(df["tipo_de_fondo"].unique().tolist())
fondos_sel = multiselect_con_todo("Tipo de fondo", fondos_opts, key="accx_fondos")

nemos_opts = sorted(df["nemo"].unique().tolist())
nemos_sel = multiselect_con_todo("Acción (nemo)", nemos_opts, key="accx_nemos")

# Si por cualquier motivo quedan vacíos, forzamos a todos
if not fondos_sel:
    fondos_sel = fondos_opts[:]
if not nemos_sel:
    nemos_sel = nemos_opts[:]

# ==== SUBCONJUNTO SELECCIONADO ====
base = df[(df["fecha"] == fecha_sel) & (df["tipo_de_fondo"].isin(fondos_sel))].copy()

# 1) AUM_FONDO desde total_inversion_grupo:
#    - Primero colapsamos por (afp, fondo) con el valor único (máximo por seguridad),
#    - luego sumamos entre fondos seleccionados para tener el AUM por AFP.
aum_fondo_por_fondo = (
    base.groupby(["afp", "tipo_de_fondo"], as_index=False)["total_inversion_grupo"]
        .max()
)
aum_fondo = (
    aum_fondo_por_fondo.groupby("afp", as_index=False)["total_inversion_grupo"]
        .sum()
        .rename(columns={"total_inversion_grupo": "AUM_FONDO"})
)

# 2) AUM de la(s) ACCIÓN(es) por AFP
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

# % participación en el total de la(s) acción(es)
total_accion = tab["AUM_ACCION"].sum()
tab["pct_en_total_accion"] = np.where(total_accion > 0, tab["AUM_ACCION"] / total_accion, 0.0)

# Comparativo vs total y AUM relativo (se usan más abajo para referencia, no se muestran)
tab["comparativo_vs_total"] = tab["pct_en_fondo"] - tab["pct_en_total_accion"]
tab["AUM_relativo"] = tab["AUM_FONDO"] * tab["comparativo_vs_total"]

# ==== FILA TOTAL (misma fórmula) ====
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
}])

tab = pd.concat([tab, fila_total], ignore_index=True)

# ==== Deltas vs TOTAL ====
pct_total_ref = pct_fondo_total
tab["delta_pct_vs_total"] = tab["pct_en_fondo"] - pct_total_ref
tab["delta_aum_vs_total"] = tab["AUM_FONDO"] * tab["delta_pct_vs_total"]

# ==== PRESENTACIÓN (solo columnas pedidas) ====
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

st.subheader(f"LTM · Fondos {', '.join(fondos_sel)} · {fecha_sel}")
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

# (Opcional) descarga CSV de lo visible
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
    file_name=f"ACC_{'-'.join(fondos_sel)}_{fecha_sel}.csv",
    mime="text/csv",
)
