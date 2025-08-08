# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from datetime import datetime

st.title("📑 Detalle Cartera (ACC)")

# ===============================
# 🔎 Utilitarios
# ===============================
def listar_archivos_en(ruta: Path) -> pd.DataFrame:
    """Devuelve un DataFrame con archivos de la ruta (si existe): nombre, tamaño y fecha modif."""
    try:
        if not ruta.exists():
            return pd.DataFrame({"archivo": [], "tamano_bytes": [], "modificado": []})
        filas = []
        for p in sorted(ruta.glob("*")):
            if p.is_file():
                filas.append({
                    "archivo": p.name,
                    "tamano_bytes": p.stat().st_size,
                    "modificado": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })
        return pd.DataFrame(filas)
    except Exception as e:
        return pd.DataFrame({"archivo": [f"Error leyendo: {e}"], "tamano_bytes": [None], "modificado": [None]})

# ===============================
# 📂 Carga parquet con búsqueda robusta
# ===============================
@st.cache_data
def cargar_cartera() -> pd.DataFrame:
    base = Path(__file__).resolve().parents[1]  # /app/dashboard (sube un nivel desde /pages)
    candidatos = [
        base / "backend" / "data_fuentes",
        base / "data_fuentes",
        Path("/app/data_fuentes"),
        Path("/app/dashboard/data_fuentes"),
    ]
    filename = "cartera_merged_ACC.parquet"

    # Intento encontrar el archivo en el primer match
    seleccionado = None
    for carpeta in candidatos:
        path = carpeta / filename
        if path.exists():
            seleccionado = path
            break

    if seleccionado is None:
        st.error("❌ No se encontró el parquet de cartera en rutas conocidas.")
        st.write("Estos son los contenidos detectados en cada carpeta candidata:")
        for carpeta in candidatos:
            st.caption(f"📁 {carpeta}")
            df_listado = listar_archivos_en(carpeta)
            if df_listado.empty:
                st.write("— (vacío o no existe)")
            else:
                st.dataframe(df_listado, use_container_width=True)
        st.stop()

    st.caption(f"📂 Usando parquet: {seleccionado}")
    df = pd.read_parquet(seleccionado)

    # Normalización de nombres según tu esquema real
    ren = {
        "run_fondo": "run_fm",
        "nombre_fondo": "nombre_corto",
        "nemotecnico_instrumento": "nemotecnico",
        "valorizacion_cierre_m": "valor_mercado",
    }
    df = df.rename(columns={src: dst for src, dst in ren.items() if src in df.columns})

    # Dedupe total y tipos
    df = df.drop_duplicates()
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce").fillna(0)

    return df

df = cargar_cartera()

# ===============================
# 📅 Fecha: si no viene, la creo
# ===============================
cand_fecha = [c for c in ["fecha_dia", "fecha", "fecha_corte"] if c in df.columns]
if cand_fecha:
    col_fecha = cand_fecha[0]
    df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
else:
    fecha_sel_tmp = st.date_input("📅 Fecha de la cartera (el archivo no trae fecha)", value=pd.Timestamp.today().date())
    df["fecha_dia"] = pd.to_datetime(fecha_sel_tmp)
    col_fecha = "fecha_dia"

# ===============================
# ✅ Validaciones mínimas
# ===============================
req = ["nombre_corto", "tipo_instrumento", "nemotecnico", "valor_mercado", col_fecha]
faltan = [c for c in req if c not in df.columns]
if faltan:
    st.error(f"❌ Faltan columnas requeridas: {faltan}\n\n🔎 Columnas disponibles: {list(df.columns)}")
    st.stop()

# Limpieza básica de nulos clave
df["tipo_instrumento"] = df["tipo_instrumento"].fillna("SIN CLASIFICAR")
df["nombre_corto"] = df["nombre_corto"].fillna("(sin nombre)")

# ===============================
# 🔽 Filtros
# ===============================
fechas = sorted(pd.Series(df[col_fecha].dropna().unique()).tolist(), reverse=True)
if not fechas:
    st.error("❌ No hay fechas válidas en el dataset.")
    st.stop()

fecha_sel = st.selectbox("📅 Selecciona una fecha", fechas, format_func=lambda x: pd.to_datetime(x).date())
fondos = sorted(df.loc[df[col_fecha] == fecha_sel, "nombre_corto"].dropna().unique().tolist())
if not fondos:
    st.warning("⚠️ No hay fondos para la fecha seleccionada.")
    st.stop()

fondo_sel = st.selectbox("🏦 Selecciona un fondo", fondos)

base = df[(df[col_fecha] == fecha_sel) & (df["nombre_corto"] == fondo_sel)].copy()
if base.empty:
    st.warning("⚠️ No hay datos para esa combinación.")
    st.stop()

# ===============================
# 📊 Agregación por tipo de instrumento
# ===============================
res_tipo = (
    base.groupby("tipo_instrumento", as_index=False)["valor_mercado"]
        .sum()
        .sort_values("valor_mercado", ascending=False)
)
total_fondo = float(res_tipo["valor_mercado"].sum())
res_tipo["participacion_%"] = (res_tipo["valor_mercado"] / total_fondo * 100).round(2)

chart = alt.Chart(res_tipo).mark_bar().encode(
    x=alt.X("tipo_instrumento:N", title="Tipo de Instrumento", sort="-y"),
    y=alt.Y("valor_mercado:Q", title="Monto (CLP)"),
    tooltip=[
        alt.Tooltip("tipo_instrumento:N", title="Tipo"),
        alt.Tooltip("valor_mercado:Q", title="Monto CLP", format=",.0f"),
        alt.Tooltip("participacion_%:Q", title="% Participación", format=".2f"),
    ],
).properties(
    title=f"Distribución — {fondo_sel} ({pd.to_datetime(fecha_sel).date()})",
    height=320,
    width="container"
)
st.altair_chart(chart, use_container_width=True)

# ===============================
# 📜 Detalle por instrumento
# ===============================
detalle = (
    base[["nemotecnico", "tipo_instrumento", "valor_mercado"]]
    .groupby(["nemotecnico", "tipo_instrumento"], as_index=False)
    .sum()
    .sort_values("valor_mercado", ascending=False)
)
detalle["participacion_%"] = (detalle["valor_mercado"] / total_fondo * 100).round(2)
detalle = detalle.rename(columns={
    "nemotecnico": "Nemotécnico",
    "tipo_instrumento": "Tipo de Instrumento",
    "valor_mercado": "Valor Mercado (CLP)",
})
st.dataframe(detalle, use_container_width=True)

# ===============================
# ⬇️ Descarga
# ===============================
@st.cache_data
def _csv(df_): 
    return df_.to_csv(index=False).encode("utf-8-sig")

st.download_button(
    "⬇️ Descargar detalle (CSV)",
    data=_csv(detalle),
    file_name=f"detalle_cartera_{fondo_sel}_{pd.to_datetime(fecha_sel).date()}.csv",
    mime="text/csv"
)

# ===============================
# 🧮 Total
# ===============================
st.caption(f"💡 Total fondo (CLP): {total_fondo:,.0f}".replace(",", "."))
