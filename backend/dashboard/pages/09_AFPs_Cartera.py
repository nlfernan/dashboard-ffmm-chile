# -*- coding: utf-8 -*-
"""
ACC por Acción — por AFP (estilo de filtros 'administradora')
- Parquet: cartera_mensual_ACC.parquet (busca en 3 rutas)
- Filtros:
  * Fecha (desc)
  * Tipo de fondo (multiselect con '(Seleccionar todo)')
  * Acción (nemo) (multiselect con '(Seleccionar todo)')
  * Botón 'Aplicar filtros'
- Métricas por AFP:
  AUM_FONDO = total_inversion_grupo (suma entre fondos seleccionados; NO depende del filtro de acción)
  AUM_ACCION = inversión en la(s) acción(es) seleccionada(s)
  pct_en_fondo = AUM_ACCION / AUM_FONDO
  delta_pct_vs_total = pct_en_fondo_fila - pct_en_fondo_TOTAL
  delta_aum_vs_total = AUM_FONDO * delta_pct_vs_total
- Vista: solo columnas pedidas. Incluye fila TOTAL.
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path

# ===============================
# 🔧 Rutas candidatas
# ===============================
RUTAS = [
    "app/data_fuentes/cartera_mensual_ACC.parquet",
    "backend/data_fuentes/cartera_mensual_ACC.parquet",
    "data_fuentes/cartera_mensual_ACC.parquet",
]

TODO = "(Seleccionar todo)"

# Mapeo sigla -> nombre AFP
MAP_AFP = {
    "cap": "CAPITAL",
    "cup": "CUPRUM",
    "hab": "HABITAT",
    "mod": "MODELO",
    "pli": "PLANVITAL",
    "prv": "PROVIDA",
    "uno": "UNO",
}

st.set_page_config(page_title="ACC por Acción — AFP", layout="wide")
st.title("📈 ACC por Acción — por AFP")

# ===============================
# ♻️ Carga con cache
# ===============================
@st.cache_data(show_spinner=True)
def cargar_datos():
    for ruta in RUTAS:
        p = Path(ruta)
        if p.exists():
            df = pd.read_parquet(p)
            origen = f"parquet: {p}"
            break
    else:
        raise FileNotFoundError("No se encontró cartera_mensual_ACC.parquet en las rutas configuradas.")

    # Normalización mínima
    req = {"fecha", "afp", "tipo_de_fondo", "inversion", "total_inversion_grupo"}
    faltan = [c for c in req if c not in df.columns]
    if faltan:
        raise KeyError(f"Faltan columnas requeridas: {faltan}")

    df = df.copy()
    df["fecha"] = df["fecha"].astype(str)

    # afp: normalizo y aplico mapeo con fallback a sigla
    df["afp"] = (
        df["afp"].astype(str).str.lower().str.strip()
        .apply(lambda x: MAP_AFP.get(x, x))
    )

    df["tipo_de_fondo"] = df["tipo_de_fondo"].astype(str).str.upper().str.strip()

    # nemo
    if "nemotecnico_del_instrumento" in df.columns:
        df["nemo"] = df["nemotecnico_del_instrumento"].astype(str).str.upper().str.strip()
    elif "nemo" in df.columns:
        df["nemo"] = df["nemo"].astype(str).str.upper().str.strip()
    else:
        raise KeyError("Falta columna de nemotécnico ('nemotecnico_del_instrumento' o 'nemo').")

    df["inversion"] = pd.to_numeric(df["inversion"], errors="coerce")
    df["total_inversion_grupo"] = pd.to_numeric(df["total_inversion_grupo"], errors="coerce")
    return df, origen

df, _origen = cargar_datos()
st.caption(f"Fuente: {_origen}")

# ===============================
# 🔽 Multiselect + “(Seleccionar todo)”
# ===============================
def multiselect_con_todo(label, opciones, key=None):
    opciones_mostradas = [TODO] + list(opciones)
    return st.multiselect(label, opciones_mostradas, default=[TODO], key=key)

def limpiar_selecciones(seleccion, universo):
    if TODO in seleccion and len(seleccion) > 1:
        seleccion = [v for v in seleccion if v != TODO]
    if not seleccion or (len(seleccion) == 1 and seleccion[0] == TODO):
        return universo[:]
    return seleccion

# ===============================
# 🎛️ Filtros (con keys únicos)
# ===============================
st.subheader("Filtros")

fechas_desc = sorted(df["fecha"].dropna().unique(), reverse=True)
col1, col2, col3 = st.columns([1.2, 1, 1])

with col1:
    fecha_sel = st.selectbox("Fecha", options=fechas_desc, index=0, key="acc_fecha")

fondos_univ = sorted(df.loc[df["fecha"] == fecha_sel, "tipo_de_fondo"].dropna().astype(str).unique().tolist())
with col2:
    fondos_sel_raw = multiselect_con_todo("Tipo de fondo", fondos_univ, key="acc_fondos")

nemos_univ = sorted(df.loc[df["fecha"] == fecha_sel, "nemo"].dropna().astype(str).unique().tolist())
with col3:
    nemos_sel_raw = multiselect_con_todo("Acción (nemo)", nemos_univ, key="acc_nemos")

aplicar = st.button("Aplicar filtros", type="primary", key="acc_aplicar")

# ===============================
# 🧮 Aplicación de filtros (solo al click)
# ===============================
if "acc_df_fondos" not in st.session_state or "acc_df_accion" not in st.session_state or aplicar:
    fondos_sel = limpiar_selecciones(fondos_sel_raw, fondos_univ)
    nemos_sel = limpiar_selecciones(nemos_sel_raw, nemos_univ)

    # DENOMINADOR (AUM_FONDO): depende SOLO de fecha + tipo_de_fondo
    cond_fondos = (
        (df["fecha"] == fecha_sel) &
        (df["tipo_de_fondo"].astype(str).isin(fondos_sel))
    )
    df_fondos = df.loc[cond_fondos].copy()

    # NUMERADOR (AUM_ACCION): mismo universo anterior + filtro de nemos
    cond_accion = cond_fondos & (df["nemo"].astype(str).isin(nemos_sel))
    df_accion = df.loc[cond_accion].copy()

    st.session_state.acc_df_fondos = df_fondos
    st.session_state.acc_df_accion = df_accion
    st.session_state.acc_fondos_sel = fondos_sel
    st.session_state.acc_nemos_sel = nemos_sel
    st.session_state.acc_fecha_sel = fecha_sel

# Variables seguras para mostrar (solo si existen)
df_fondos = st.session_state.get("acc_df_fondos", pd.DataFrame())
df_accion = st.session_state.get("acc_df_accion", pd.DataFrame())
fondos_sel = st.session_state.get("acc_fondos_sel", [])
nemos_sel = st.session_state.get("acc_nemos_sel", [])
fecha_sel_shown = st.session_state.get("acc_fecha_sel", None)

if df_fondos.empty:
    st.info("Configurá los filtros y presioná **Aplicar filtros** para ver resultados.")
    st.stop()

# ===============================
# 📐 Cálculo de métricas
# ===============================
# 1) AUM_FONDO desde total_inversion_grupo — NO depende de nemos
aum_fondo_por_fondo = (
    df_fondos.groupby(["afp", "tipo_de_fondo"], as_index=False)["total_inversion_grupo"]
             .max()   # el valor es único por (afp, fondo); max por seguridad
)
aum_fondo = (
    aum_fondo_por_fondo.groupby("afp", as_index=False)["total_inversion_grupo"]
             .sum()
             .rename(columns={"total_inversion_grupo": "AUM_FONDO"})
)

# 2) AUM de la(s) ACCIÓN(es) seleccionada(s) — SÍ depende de nemos
aum_accion = (
    df_accion.groupby("afp", as_index=False)["inversion"]
             .sum()
             .rename(columns={"inversion": "AUM_ACCION"})
)

# Merge + métricas por AFP
tab = aum_fondo.merge(aum_accion, on="afp", how="left").fillna({"AUM_ACCION": 0.0})

# 3) % en fondo
tab["pct_en_fondo"] = np.where(tab["AUM_FONDO"] > 0, tab["AUM_ACCION"] / tab["AUM_FONDO"], np.nan)

# 4) Deltas vs TOTAL (mismo criterio)
sum_aum_fondo   = tab["AUM_FONDO"].sum()
sum_aum_accion  = tab["AUM_ACCION"].sum()
pct_fondo_total = (sum_aum_accion / sum_aum_fondo) if sum_aum_fondo > 0 else np.nan

tab["delta_pct_vs_total"] = tab["pct_en_fondo"] - pct_fondo_total
tab["delta_aum_vs_total"] = tab["AUM_FONDO"] * tab["delta_pct_vs_total"]

# Fila TOTAL (deltas = 0)
fila_total = pd.DataFrame([{
    "afp": "TOTAL",
    "AUM_FONDO": sum_aum_fondo,
    "AUM_ACCION": sum_aum_accion,
    "pct_en_fondo": pct_fondo_total,
    "delta_pct_vs_total": 0.0,
    "delta_aum_vs_total": 0.0,
}])
tab = pd.concat([tab, fila_total], ignore_index=True)

# ===============================
# 🖼️ Vista final (solo columnas pedidas)
# ===============================
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

# Subtítulo
if fecha_sel_shown is not None and fondos_sel:
    fondos_txt = ', '.join(fondos_sel) if len(fondos_sel) <= 6 else f"{len(fondos_sel)} seleccionados"
    nemos_txt = f"{len(nemos_sel)} seleccionados" if isinstance(nemos_sel, list) else "—"
    st.subheader(f"Fecha {fecha_sel_shown} • Fondos: {fondos_txt} • Nemos: {nemos_txt}")

st.dataframe(
    view[columnas_finales].style.format({
        "AUM_FONDO_MM": "{:,.0f}",
        "AUM_ACCION_MM": "{:,.0f}",
        "pct_en_fondo": "{:.2%}",
        "delta_pct_vs_total": "{:.2%}",
        "delta_aum_vs_total_MM": "{:,.0f}",
    }),
    use_container_width=True,
    hide_index=True
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
    file_name=f"ACC_{fecha_sel_shown or fechas_desc[0]}.csv",
    mime="text/csv",
)
