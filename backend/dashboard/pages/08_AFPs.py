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

# Validaciones
req_cols = {"administradora", "Fondo", "fecha", "ventana", "rentab_anualizada", "std_anualizada"}
faltan = [c for c in req_cols if c not in df.columns]
if faltan:
    st.error(f"Faltan columnas en el dataset: {faltan}")
    st.stop()

# ===============================
# 🔧 Helper: multiselect con “(Seleccionar todo)”
# ===============================
def multiselect_con_todo(label, universe, state_key):
    """Multiselect con opción '(Seleccionar todo)'.
    - Si incluye TODO o queda vacío -> toma todas.
    - Preserva selección previa en session_state.
    """
    opciones = [TODO] + universe
    default_prev = st.session_state.get(state_key, [TODO])
    default_prev = [x for x in default_prev if (x == TODO or x in opciones)]
    if not default_prev:
        default_prev = [TODO]
    sel_raw = st.multiselect(label, options=opciones, default=default_prev, key=f"{state_key}_raw")
    seleccion = universe[:] if (TODO in sel_raw or not sel_raw) else sel_raw[:]
    st.session_state[state_key] = seleccion[:]  # guardo sólo valores reales
    return seleccion

# ===============================
# 🎛️ Filtros
# ===============================
st.subheader("Filtros")
col1, col2, col3 = st.columns([1.2, 1, 1])

# Fecha (última por defecto)
fechas_disponibles = sorted(df["fecha"].dropna().unique())
with col1:
    fecha_sel = st.selectbox("Fecha", options=fechas_disponibles, index=len(fechas_disponibles) - 1)

# Ventana (prioriza 1A/2A/3A/5A)
preferidas = ["1A", "2A", "3A", "5A"]
ventanas_data = list(dict.fromkeys(list(df["ventana"].dropna().astype(str).unique())))
ordenadas = [v for v in preferidas if v in ventanas_data] + [v for v in ventanas_data if v not in preferidas]
with col2:
    ventana = st.radio("Ventana", options=ordenadas, horizontal=True, index=0)

# Administradora por fecha con “Seleccionar todo”
admins_dyn = sorted(df.loc[df["fecha"] == fecha_sel, "administradora"].dropna().unique().tolist())
with col3:
    sel_afp = multiselect_con_todo("Administradora", admins_dyn, state_key="sel_afp_prev")

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
    st.session_state.df_filtrado = df_filtrado.copy()
else:
    df_filtrado = st.session_state.df_filtrado

# ===============================
# 🔎 Vista base fija
# ===============================
cols_finales = ["fecha", "administradora", "Fondo", "rentab_anualizada", "std_anualizada"]
df_vista = df_filtrado[cols_finales].copy().reset_index(drop=True)

# ===============================
# 🧠 Frontera eficiente (upper hull)
# ===============================
def efficient_frontier(points_df: pd.DataFrame) -> pd.DataFrame:
    pts = points_df.dropna(subset=["std_anualizada", "rentab_anualizada"]).copy()
    pts = pts.drop_duplicates(subset=["std_anualizada", "rentab_anualizada"])
    pts = pts.sort_values(["std_anualizada", "rentab_anualizada"]).reset_index(drop=True)

    def cross(o, a, b):
        return (a[0]-o[0])*(b[1]-o[1]) - (a[1]-o[1])*(b[0]-o[0])

    coords = pts[["std_anualizada", "rentab_anualizada"]].values.tolist()
    upper = []
    for p in reversed(coords):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    upper = list(reversed(upper))
    hull_df = pd.DataFrame(upper, columns=["std_anualizada", "rentab_anualizada"])
    hull_df = hull_df.sort_values("std_anualizada").drop_duplicates(subset=["std_anualizada"], keep="last")
    return hull_df.reset_index(drop=True)

# ===============================
# 🧾 Subpestañas: Tabla y Gráfico
# ===============================
tab_tabla, tab_graf = st.tabs(["📄 Tabla", "📈 Gráfico"])

with tab_tabla:
    df_show = df_vista.copy()
    for c in ["rentab_anualizada", "std_anualizada"]:
        df_show[c] = pd.to_numeric(df_show[c], errors="coerce")
        df_show[c] = (df_show[c] * 100).round(2).astype(str) + "%"

    st.success(f"Registros filtrados: {len(df_show):,} | Ventana seleccionada: {ventana}")
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    csv_bytes = df_vista.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Descargar CSV filtrado",
        data=csv_bytes,
        file_name=f"afps_metricas_{fecha_sel}_{ventana}.csv",
        mime="text/csv"
    )

with tab_graf:
    st.subheader("Riesgo vs Retorno (anualizado)")

    chart_df = df_filtrado[["administradora", "Fondo", "rentab_anualizada", "std_anualizada"]].copy()
    chart_df["rentab_anualizada"] = pd.to_numeric(chart_df["rentab_anualizada"], errors="coerce")
    chart_df["std_anualizada"]   = pd.to_numeric(chart_df["std_anualizada"], errors="coerce")
    chart_df = chart_df.dropna(subset=["rentab_anualizada", "std_anualizada"])

    colA, colB = st.columns([1, 1])
    with colA:
        color_por = st.selectbox("Color por", options=["administradora"], index=0)
    with colB:
        mostrar_labels = st.checkbox("Mostrar etiquetas (hasta 30 puntos)", value=False)

    base = alt.Chart(chart_df).encode(
        x=alt.X("std_anualizada:Q", title="Riesgo (Desv. Std. anualizada)", axis=alt.Axis(format="%")),
        y=alt.Y("rentab_anualizada:Q", title="Retorno anualizado", axis=alt.Axis(format="%")),
        tooltip=[
            alt.Tooltip("administradora:N", title="Administradora"),
            alt.Tooltip("Fondo:N", title="Fondo"),
            alt.Tooltip("rentab_anualizada:Q", title="Retorno", format=".2%"),
            alt.Tooltip("std_anualizada:Q", title="Riesgo",  format=".2%")
        ],
        color=alt.Color(f"{color_por}:N", title=color_por.capitalize())
    )

    puntos = base.mark_circle(size=80, opacity=0.9)
    graf = puntos.properties(height=520).interactive()

    # Frontera eficiente
    if not chart_df.empty:
        frontier_df = efficient_frontier(chart_df[["std_anualizada", "rentab_anualizada"]])
        if len(frontier_df) >= 2:
            linea = alt.Chart(frontier_df).mark_line(point=True).encode(
                x=alt.X("std_anualizada:Q"),
                y=alt.Y("rentab_anualizada:Q"),
                tooltip=[
                    alt.Tooltip("rentab_anualizada:Q", title="Retorno frontera", format=".2%"),
                    alt.Tooltip("std_anualizada:Q", title="Riesgo frontera",  format=".2%")
                ]
            )
            graf = graf + linea

    if mostrar_labels and not chart_df.empty:
        df_lbl = chart_df.head(30)
        labels = alt.Chart(df_lbl).mark_text(dy=-10, fontSize=10).encode(
            x="std_anualizada:Q",
            y="rentab_anualizada:Q",
            text="Fondo:N",
            color=alt.value("black")
        )
        graf = graf + labels

    st.altair_chart(graf, use_container_width=True)
