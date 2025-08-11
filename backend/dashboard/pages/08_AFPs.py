# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
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
    opciones = [TODO] + universe
    default_prev = st.session_state.get(state_key, [TODO])
    default_prev = [x for x in default_prev if (x == TODO or x in opciones)]
    if not default_prev:
        default_prev = [TODO]
    sel_raw = st.multiselect(label, options=opciones, default=default_prev, key=f"{state_key}_raw")
    seleccion = universe[:] if (TODO in sel_raw or not sel_raw) else sel_raw[:]
    st.session_state[state_key] = seleccion[:]
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

# Administradora (con “Seleccionar todo” y dinámica por fecha)
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
# 🧠 Frontera estilo Markowitz (μ = α + β·σ²)
# ===============================
def frontier_markowitz(df_points: pd.DataFrame, nbins: int = 20):
    """
    1) Binea por riesgo (σ) y toma el punto de mayor retorno por bin (upper envelope muestral).
    2) Ajusta μ = α + β·σ² (lineal en la varianza).
    3) Devuelve curva evaluada en todo el rango de σ.
    """
    pts = df_points.dropna(subset=["std_anualizada", "rentab_anualizada"]).copy()
    if pts.empty:
        return None, None, None

    pts = pts.sort_values("std_anualizada")
    q = min(nbins, max(1, pts["std_anualizada"].nunique()))
    pts["bin"] = pd.qcut(pts["std_anualizada"], q=q, duplicates="drop")

    tops = pts.loc[pts.groupby("bin")["rentab_anualizada"].idxmax()].sort_values("std_anualizada")
    sigma = tops["std_anualizada"].values
    mu    = tops["rentab_anualizada"].values

    if len(sigma) < 2:
        return None, None, None

    var = sigma**2
    # Ajuste lineal μ ~ α + β·σ²
    coef = np.polyfit(var, mu, deg=1)
    beta, alpha = coef[0], coef[1]  # μ = alpha + beta*σ²

    mu_hat = alpha + beta * var
    ss_res = np.sum((mu - mu_hat)**2)
    ss_tot = np.sum((mu - mu.mean())**2)
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else None

    # Curva sobre todo el rango observado de σ
    sigma_grid = np.linspace(pts["std_anualizada"].min(), pts["std_anualizada"].max(), 200)
    mu_grid = alpha + beta * (sigma_grid**2)

    curva_df = pd.DataFrame({"std_anualizada": sigma_grid, "rentab_anualizada": mu_grid})
    ecuacion = f"μ = {alpha:.4f} + {beta:.4f}·σ²"
    return curva_df, ecuacion, r2

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

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        color_por = st.selectbox("Color por", options=["administradora"], index=0)
    with colB:
        mostrar_labels = st.checkbox("Mostrar etiquetas (hasta 30 puntos)", value=False)
    with colC:
        nbins = st.slider("Suavizado (bins de riesgo)", min_value=6, max_value=40, value=20, step=2)

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

    # ======= Frontera estilo Markowitz (abre hacia la derecha) =======
    if not chart_df.empty:
        curva_df, ecuacion, r2 = frontier_markowitz(chart_df, nbins=nbins)
        if curva_df is not None and len(curva_df) > 1:
            linea = alt.Chart(curva_df).mark_line(size=2).encode(
                x=alt.X("std_anualizada:Q"),
                y=alt.Y("rentab_anualizada:Q"),
            )
            graf = graf + linea
            if ecuacion:
                st.caption(f"Frontera tipo Markowitz: {ecuacion}" + (f" — R² = {r2:.3f}" if r2 is not None else ""))

    # Etiquetas opcionales
    if mostrar_labels and not chart_df.empty:
        df_lbl = chart_df.head(30)
        labels = alt.Chart(df_lbl).mark_text(dy=-10, fontSize=10).encode(
            x="std_anualizada:Q",
            y="rentab_anualizada:Q",
            text="Fondo:N",
        )
        graf = graf + labels

    st.altair_chart(graf, use_container_width=True)
