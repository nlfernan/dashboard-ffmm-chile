# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from pathlib import Path
import altair as alt

st.set_page_config(page_title="AFPs - Métricas VC", layout="wide")
st.title("📈 AFPs — Métricas de Valor Cuota (rolling)")

# ===============================
# 🔧 Rutas candidatas
# ===============================
RUTAS = [
    r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\vc_metricas_rolling.parquet",
    "backend/data_fuentes/vc_metricas_rolling.parquet",
    "data_fuentes/vc_metricas_rolling.parquet",
    "vc_metricas_rolling.parquet",
]
CSV_FALLBACK = "/mnt/data/vc_metricas_rolling.csv"
TODO = "(Seleccionar todo)"

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
        p = Path(CSV_FALLBACK)
        if p.exists():
            df = pd.read_csv(p)
            origen = f"csv: {p}"
        else:
            raise FileNotFoundError("No se encontró el archivo en las rutas configuradas.")

    # Normalización mínima
    df = df.copy()
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date

    return df, origen

# ===============================
# 🚚 Cargar datos
# ===============================
df, origen = cargar_datos()
st.caption(f"Fuente de datos → **{origen}**")

if df.empty:
    st.warning("El dataset está vacío.")
    st.stop()

# Validaciones de columnas claves
req_cols = {"administradora", "Fondo", "fecha", "ventana", "rentab_anualizada", "std_anualizada"}
faltan = [c for c in req_cols if c not in df.columns]
if faltan:
    st.error(f"Faltan columnas en el dataset: {faltan}")
    st.stop()

# ===============================
# 🎛️ Filtros en la hoja
# ===============================
st.subheader("Filtros")
col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])

# ---- Fecha única (última por defecto)
fechas_disponibles = sorted(df["fecha"].dropna().unique())
with col1:
    fecha_sel = st.selectbox("Fecha", options=fechas_disponibles, index=len(fechas_disponibles) - 1)

# ---- Ventana (desde el parquet, priorizando 1A,2A,3A,5A)
preferidas = ["1A", "2A", "3A", "5A"]
ventanas_data = list(dict.fromkeys(list(df["ventana"].dropna().astype(str).unique())))
ordenadas = [v for v in preferidas if v in ventanas_data] + [v for v in ventanas_data if v not in preferidas]
with col2:
    ventana = st.radio("Ventana", options=ordenadas, horizontal=True, index=0)

# ---- Administradora con “Seleccionar todo”
admins_dyn = sorted(df.loc[df["fecha"] == fecha_sel, "administradora"].dropna().unique().tolist())
prev_admins = st.session_state.get("sel_afp_prev", None)
if prev_admins:
    prev_admins = [v for v in prev_admins if v in admins_dyn]

with col3:
    opciones_afp = [TODO] + admins_dyn
    default_afp = prev_admins if prev_admins else [TODO]
    sel_afp_raw = st.multiselect("AFP / Administradora", options=opciones_afp, default=default_afp)
    sel_afp = admins_dyn[:] if (TODO in sel_afp_raw or not sel_afp_raw) else sel_afp_raw[:]
    st.session_state["sel_afp_prev"] = sel_afp[:]

# ---- Fondo / Serie
fondos_dyn = sorted(df["Fondo"].dropna().unique().tolist())
with col4:
    sel_fondo = st.multiselect("Fondo / Serie", options=fondos_dyn, default=fondos_dyn)

aplicar = st.button("Aplicar filtros", type="primary")

# ===============================
# 🧮 Aplicación de filtros
# ===============================
if "df_filtrado" not in st.session_state or aplicar:
    df_filtrado = df[
        (df["fecha"] == fecha_sel) &
        (df["ventana"].astype(str) == str(ventana))
    ]
    if sel_afp:
        df_filtrado = df_filtrado[df_filtrado["administradora"].isin(sel_afp)]
    if sel_fondo:
        df_filtrado = df_filtrado[df_filtrado["Fondo"].isin(sel_fondo)]
    st.session_state.df_filtrado = df_filtrado.copy()
else:
    df_filtrado = st.session_state.df_filtrado

# ===============================
# 🔎 Vista fija y formateo
# ===============================
cols_finales = ["fecha", "administradora", "Fondo", "rentab_anualizada", "std_anualizada"]
df_vista = df_filtrado[cols_finales].copy().reset_index(drop=True)

# Vista formateada en % (sin tocar el export)
df_show = df_vista.copy()
for c in ["rentab_anualizada", "std_anualizada"]:
    df_show[c] = pd.to_numeric(df_show[c], errors="coerce")
    df_show[c] = (df_show[c] * 100).round(2).astype(str) + "%"

st.success(f"Registros filtrados: {len(df_show):,} | Ventana seleccionada: {ventana}")
st.dataframe(df_show, use_container_width=True, hide_index=True)

# ===============================
# 📈 Gráfico XY Riesgo vs Retorno
# ===============================
st.subheader("Riesgo vs Retorno (anualizado)")

chart_df = df_filtrado[["administradora", "Fondo", "rentab_anualizada", "std_anualizada"]].copy()
chart_df["rentab_anualizada"] = pd.to_numeric(chart_df["rentab_anualizada"], errors="coerce")
chart_df["std_anualizada"]   = pd.to_numeric(chart_df["std_anualizada"], errors="coerce")
chart_df = chart_df.dropna(subset=["rentab_anualizada", "std_anualizada"])

colA, colB = st.columns([1, 1])
with colA:
    color_por = st.selectbox("Color por", options=["administradora", "Fondo"], index=0)
with colB:
    mostrar_labels = st.checkbox("Mostrar etiquetas (hasta 30 puntos)", value=False)

base = alt.Chart(chart_df).encode(
    x=alt.X("std_anualizada:Q",
            title="Riesgo (Desv. Std. anualizada)",
            axis=alt.Axis(format="%")),
    y=alt.Y("rentab_anualizada:Q",
            title="Retorno anualizado",
            axis=alt.Axis(format="%")),
    tooltip=[
        alt.Tooltip("administradora:N", title="Administradora"),
        alt.Tooltip("Fondo:N", title="Fondo"),
        alt.Tooltip("rentab_anualizada:Q", title="Retorno", format=".2%"),
        alt.Tooltip("std_anualizada:Q", title="Riesgo",  format=".2%")
    ],
    color=alt.Color(f"{color_por}:N", title=color_por.capitalize())
)

puntos = base.mark_circle(size=80, opacity=0.9)
graf = puntos.properties(height=500).interactive()

if mostrar_labels:
    df_lbl = chart_df.head(30)
    labels = alt.Chart(df_lbl).mark_text(dy=-10, fontSize=10).encode(
        x="std_anualizada:Q",
        y="rentab_anualizada:Q",
        text="Fondo:N",
        color=alt.value("black")
    )
    graf = graf + labels

st.altair_chart(graf, use_container_width=True)

# ===============================
# ⬇️ Descarga CSV (sin formateo)
# ===============================
csv_bytes = df_vista.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️ Descargar CSV filtrado",
    data=csv_bytes,
    file_name=f"afps_metricas_{fecha_sel}_{ventana}.csv",
    mime="text/csv"
)
