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

df, _origen = cargar_datos()
if df.empty:
    st.warning("El dataset está vacío.")
    st.stop()

req_cols = {"administradora", "Fondo", "fecha", "ventana", "rentab_anualizada", "std_anualizada"}
faltan = [c for c in req_cols if c not in df.columns]
if faltan:
    st.error(f"Faltan columnas en el dataset: {faltan}")
    st.stop()

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

fechas_disponibles = sorted(df["fecha"].dropna().unique())
with col1:
    fecha_sel = st.selectbox("Fecha", options=fechas_disponibles, index=len(fechas_disponibles) - 1)

preferidas = ["1A", "2A", "3A", "5A"]
ventanas_data = list(dict.fromkeys(list(df["ventana"].dropna().astype(str).unique())))
ordenadas = [v for v in preferidas if v in ventanas_data] + [v for v in ventanas_data if v not in preferidas]
with col2:
    ventana = st.radio("Ventana", options=ordenadas, horizontal=True, index=0)

admins_dyn = sorted(df.loc[df["fecha"] == fecha_sel, "administradora"].dropna().unique().tolist())
with col3:
    sel_afp = multiselect_con_todo("Administradora", admins_dyn, state_key="sel_afp_prev")

aplicar = st.button("Aplicar filtros", type="primary")

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

cols_finales = ["fecha", "administradora", "Fondo", "rentab_anualizada", "std_anualizada"]
df_vista = df_filtrado[cols_finales].copy().reset_index(drop=True)

def frontier_sideways(df_points: pd.DataFrame, nbins: int = 20):
    pts = df_points.dropna(subset=["std_anualizada", "rentab_anualizada"]).copy()
    if pts.empty:
        return None, None, None, None, None

    pts = pts.sort_values("std_anualizada")
    q = min(nbins, max(1, pts["std_anualizada"].nunique()))
    pts["bin"] = pd.qcut(pts["std_anualizada"], q=q, duplicates="drop")
    tops = pts.loc[pts.groupby("bin")["rentab_anualizada"].idxmax()].sort_values("rentab_anualizada")

    mu = tops["rentab_anualizada"].values
    var = (tops["std_anualizada"].values) ** 2
    if len(mu) < 3:
        return None, tops, "Datos insuficientes para parábola", None, None

    X = np.vstack([np.ones_like(mu), mu, mu**2]).T
    coef, _, _, _ = np.linalg.lstsq(X, var, rcond=None)
    alpha, beta, gamma = coef

    var_hat = X @ coef
    ss_res = np.sum((var - var_hat)**2)
    ss_tot = np.sum((var - var.mean())**2)
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else None

    mu_grid = np.linspace(pts["rentab_anualizada"].min(), pts["rentab_anualizada"].max(), 400)
    var_grid = alpha + beta*mu_grid + gamma*(mu_grid**2)
    var_grid = np.maximum(var_grid, 0.0)
    sigma_grid = np.sqrt(var_grid)

    curva_df = pd.DataFrame({"std_anualizada": sigma_grid, "rentab_anualizada": mu_grid}).sort_values("rentab_anualizada")

    mu_star = -beta/(2*gamma) if gamma != 0 else None
    sigma_star = np.sqrt(max(alpha + beta*mu_star + gamma*mu_star**2, 0)) if mu_star is not None else None

    ecuacion = f"σ² = {alpha:.4f} + {beta:.4f}·μ + {gamma:.4f}·μ²"
    return curva_df, tops, ecuacion, r2, (sigma_star, mu_star)

# ===============================
# 🧾 Subpestañas: ahora primero Gráfico, luego Tabla
# ===============================
tab_graf, tab_tabla = st.tabs(["📈 Gráfico", "📄 Tabla"])

with tab_graf:
    chart_df = df_filtrado[["administradora", "Fondo", "rentab_anualizada", "std_anualizada"]].copy()
    chart_df["rentab_anualizada"] = pd.to_numeric(chart_df["rentab_anualizada"], errors="coerce")
    chart_df["std_anualizada"]   = pd.to_numeric(chart_df["std_anualizada"], errors="coerce")
    chart_df = chart_df.dropna(subset=["rentab_anualizada", "std_anualizada"])

    base = alt.Chart(chart_df).encode(
        x=alt.X("std_anualizada:Q", title="Riesgo (Desv. Std. anualizada)", axis=alt.Axis(format="%")),
        y=alt.Y("rentab_anualizada:Q", title="Retorno anualizado", axis=alt.Axis(format="%")),
        tooltip=[
            alt.Tooltip("administradora:N", title="Administradora"),
            alt.Tooltip("Fondo:N", title="Fondo"),
            alt.Tooltip("rentab_anualizada:Q", title="Retorno", format=".2%"),
            alt.Tooltip("std_anualizada:Q", title="Riesgo",  format=".2%")
        ],
        color=alt.Color("administradora:N", title="Administradora")
    )

    puntos = base.mark_circle(size=80, opacity=0.9)
    graf = puntos.properties(height=520).interactive()

    if not chart_df.empty:
        curva_df, tops_df, ecuacion, r2, vertex = frontier_sideways(chart_df, nbins=20)
        if curva_df is not None and len(curva_df) > 1:
            mu_star = vertex[1] if (vertex and vertex[1] is not None) else curva_df["rentab_anualizada"].median()
            curva_sup = curva_df[curva_df["rentab_anualizada"] >= mu_star]
            curva_inf = curva_df[curva_df["rentab_anualizada"] <= mu_star]

            linea_sup = alt.Chart(curva_sup).mark_line(size=2, color="red").encode(
                x="std_anualizada:Q", y="rentab_anualizada:Q"
            )
            linea_inf = alt.Chart(curva_inf).mark_line(size=2, color="red", strokeDash=[6, 4]).encode(
                x="std_anualizada:Q", y="rentab_anualizada:Q"
            )
            graf = graf + linea_sup + linea_inf

    st.altair_chart(graf, use_container_width=True)

    if not chart_df.empty:
        _, _, ecuacion, r2, vertex = frontier_sideways(chart_df, nbins=20)
        if ecuacion:
            cap = f"Frontera (parábola horizontal): {ecuacion}"
            if r2 is not None:
                cap += f" — R² = {r2:.3f}"
            if vertex and vertex[0] is not None and vertex[1] is not None:
                cap += f" — vértice σ* ≈ {vertex[0]:.2%}, μ* ≈ {vertex[1]:.2%}"
            st.caption(cap)

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
