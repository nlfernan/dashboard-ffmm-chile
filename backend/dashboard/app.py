# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import unicodedata
import calendar
import time
from datetime import date, timedelta
import numpy as np

# ===============================
# 📂 Ruta y columnas necesarias
# ===============================
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

COLUMNAS_NECESARIAS = [
    "fecha_inf_date", "fecha_inf", "run_fm", "nombre_corto", "run_fm_nombrecorto",
    "nom_adm", "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
    "tipo_fm", "categoria", "categoria_agrupada", "serie"
]

SINDATO = "(Sin dato)"
TODO = "(Seleccionar todo)"

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

def _to_num(s):
    return pd.to_numeric(s, errors="coerce")

def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ===============================
# 📊 Carga con barra de progreso
# ===============================
@st.cache_data
def cargar_datos():
    placeholder = st.empty()
    placeholder.info("⏳ Cargando datos, por favor espera...")

    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")

    progress = st.progress(0)
    for i in range(0, 101, 10):
        time.sleep(0.03)
        progress.progress(i)
    placeholder.empty()
    progress.empty()

    # Normalizo nombres y fechas
    df.columns = [limpiar_nombre(c) for c in df.columns]
    if "fecha_inf_date" not in df.columns and "fecha_inf" in df.columns:
        df = df.rename(columns={"fecha_inf": "fecha_inf_date"})
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"], errors="coerce")
    df["fecha_dia"] = df["fecha_inf_date"].dt.date

    # ------- Aliases mínimos para que no reviente ninguna página -------
    # run_fm / nombre_corto / run_fm_nombrecorto
    if "run_fm_nombrecorto" not in df.columns and {"run_fm", "nombre_corto"}.issubset(df.columns):
        df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

    if "nombre_corto" not in df.columns:
        if "run_fm_nombrecorto" in df.columns:
            parts = df["run_fm_nombrecorto"].astype(str).str.split(" - ", n=1, expand=True)
            if parts.shape[1] == 2:
                df["nombre_corto"] = parts[1]
                if "run_fm" not in df.columns:
                    df["run_fm"] = parts[0]
            else:
                for cand in ["nombre_fondo", "nombre", "fondo"]:
                    if cand in df.columns:
                        df["nombre_corto"] = df[cand].astype(str)
                        break
        elif "nombre_fondo" in df.columns:
            df["nombre_corto"] = df["nombre_fondo"].astype(str)
        else:
            df["nombre_corto"] = ""

    if "run_fm" not in df.columns:
        if "run_fm_nombrecorto" in df.columns:
            df["run_fm"] = df["run_fm_nombrecorto"].astype(str).str.split(" - ", n=1, expand=True)[0]
        else:
            for cand in ["run", "rut_fm", "rut_fondo", "id_fondo"]:
                if cand in df.columns:
                    df["run_fm"] = df[cand].astype(str)
                    break
        if "run_fm" not in df.columns:
            df["run_fm"] = ""

    # ✅ Forzar numéricos para que sumen bien
    for c in ["patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm"]:
        if c in df.columns:
            df[c] = _to_num(df[c])

    # 🔧 FIX Tipo de Fondo (estándar: tipo_de_fondo)
    # 1) Encontrar candidata existente
    col_tipo_src = _pick_col(df, [
        "tipo_de_fondo", "tipo_fm", "tipo", "tipo_de_fondo_cmf",
        "tipofm", "tipo_fondo", "tipo_de_fondos"
    ])
    # 2) Si no existe, derivar desde categoria_agrupada o categoria
    if col_tipo_src is None:
        if "categoria_agrupada" in df.columns:
            df["tipo_de_fondo"] = df["categoria_agrupada"]
        elif "categoria" in df.columns:
            df["tipo_de_fondo"] = df["categoria"]
        else:
            df["tipo_de_fondo"] = SINDATO
    else:
        # Normalizo al nombre estándar
        if col_tipo_src != "tipo_de_fondo":
            df["tipo_de_fondo"] = df[col_tipo_src]

    # Normalización suave de strings
    for c in ["categoria", "categoria_agrupada", "nom_adm", "tipo_de_fondo", "serie", "run_fm_nombrecorto"]:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype("string")
                .str.strip()
                .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            )
            # casteo a categoría para memoria/UI
            df[c] = df[c].astype("category")

    return df

# ===============================
# 🚦 Carga inicial
# ===============================
if "df" not in st.session_state:
    st.session_state.datos_cargados = False
    st.session_state.df = cargar_datos()
    st.session_state.datos_cargados = True

df = st.session_state.df

# ===============================
# 🦉 Logo y título
# ===============================
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; padding-top: 10px;'>
    <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Owl_in_the_Moonlight.jpg/640px-Owl_in_the_Moonlight.jpg'
         width='60' style='border-radius: 50%; box-shadow: 0 2px 6px rgba(0,0,0,0.2);'/>
    <h1 style='margin: 0; font-size: 2.2em;'>Dashboard Fondos Mutuos</h1>
</div>
""", unsafe_allow_html=True)

st.write("Configura los filtros y presiona **Aplicar filtros** para actualizar los datos.")

# ===============================
# 📅 Filtros de fecha
# ===============================
fechas_unicas = sorted(df["fecha_dia"].dropna().unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted({f.year for f in fechas_unicas})
meses_disponibles = list(calendar.month_name)[1:]

col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

año_inicio = col1.selectbox("Año inicio", años_disponibles, index=0)
mes_inicio = col2.selectbox("Mes inicio", meses_disponibles, index=0)
año_fin = col3.selectbox("Año fin", años_disponibles, index=len(años_disponibles)-1)
mes_fin = col4.selectbox("Mes fin", meses_disponibles, index=len(meses_disponibles)-1)

fecha_inicio = date(año_inicio, meses_disponibles.index(mes_inicio)+1, 1)
ultimo_dia_mes_fin = calendar.monthrange(año_fin, meses_disponibles.index(mes_fin)+1)[1]
fecha_fin = date(año_fin, meses_disponibles.index(mes_fin)+1, ultimo_dia_mes_fin)

# ===============================
# 📌 Cache de opciones fijas (incluye NaN como "(Sin dato)")
# ===============================
@st.cache_data
def cargar_opciones(df):
    def universo(col):
        if col not in df.columns:
            return []
        vals = list(df[col].cat.categories if pd.api.types.is_categorical_dtype(df[col]) else df[col].unique())
        vals = [v for v in vals if pd.notna(v)]
        vals = sorted(map(str, vals))
        if df[col].isna().any():
            vals = [SINDATO] + vals
        return vals

    return (
        universo("categoria_agrupada") if "categoria_agrupada" in df.columns else [],
        universo("categoria"),
        universo("nom_adm"),
        universo("run_fm_nombrecorto"),
        universo("tipo_de_fondo"),   # 👈 usamos el estándar
        universo("serie"),
    )

categorias_agrupadas_all, categorias_all, administradoras_all, fondos_all, tipos_all, series_all = cargar_opciones(df)

# ===============================
# 🔽 Multiselect + “Seleccionar todo” (con fix)
# ===============================
def multiselect_con_todo(label, opciones):
    opciones_mostradas = [TODO] + list(opciones)
    return st.multiselect(label, opciones_mostradas, default=[TODO])

def limpiar_selecciones(seleccion, universo):
    if TODO in seleccion and len(seleccion) > 1:
        seleccion = [v for v in seleccion if v != TODO]
    if not seleccion or (len(seleccion) == 1 and seleccion[0] == TODO):
        return universo[:]
    return seleccion

# ===============================
# ✅ Filtro por columna (sin perder NaN si el usuario lo selecciona)
# ===============================
def _filtro_col(df, col, seleccion, universo):
    if col not in df.columns:
        return pd.Series(True, index=df.index)
    if set(seleccion) == set(universo):
        return pd.Series(True, index=df.index)

    sel = set(seleccion)
    incluye_nan = SINDATO in sel
    sel_vals = [v for v in sel if v != SINDATO]

    cond = df[col].astype(str).isin(sel_vals)
    if incluye_nan:
        cond = cond | df[col].isna()
    return cond

# ===============================
# 🎛️ Filtros UI
# ===============================
categorias_agrupadas = multiselect_con_todo("Categoría Agrupada", categorias_agrupadas_all)
categorias = multiselect_con_todo("Categoría", categorias_all)
administradoras = multiselect_con_todo("Administradora(s)", administradoras_all)
fondos = multiselect_con_todo("Fondo(s)", fondos_all)

with st.expander("Filtros adicionales"):
    tipos = multiselect_con_todo("Tipo de Fondo", tipos_all)  # 👈 ahora viene de tipo_de_fondo
    series = multiselect_con_todo("Serie(s)", series_all)

    st.markdown("#### Ajuste fino de fechas")
    if "rango_fechas" not in st.session_state:
        st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    hoy = fecha_max_real
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=30), fecha_min_real), hoy)
    if col_b.button("3M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=90), fecha_min_real), hoy)
    if col_c.button("6M"): st.session_state["rango_fechas"] = (max(hoy - timedelta(days=180), fecha_min_real), hoy)
    if col_d.button("MTD"): st.session_state["rango_fechas"] = (date(hoy.year, hoy.month, 1), hoy)
    if col_e.button("YTD"): st.session_state["rango_fechas"] = (date(hoy.year, 1, 1), hoy)

rango = st.session_state["rango_fechas"]

# ===============================
# ✅ Aplicar filtros
# ===============================
st.markdown("### 🔍 Aplicar filtros a los datos")

if st.button("✅ Aplicar filtros", use_container_width=True):
    categorias_agrupadas = limpiar_selecciones(categorias_agrupadas, categorias_agrupadas_all)
    categorias = limpiar_selecciones(categorias, categorias_all)
    administradoras = limpiar_selecciones(administradoras, administradoras_all)
    fondos = limpiar_selecciones(fondos, fondos_all)
    tipos = limpiar_selecciones(tipos, tipos_all)
    series = limpiar_selecciones(series, series_all)

    cond = (
        _filtro_col(df, "categoria", categorias, categorias_all)
        & _filtro_col(df, "nom_adm", administradoras, administradoras_all)
        & _filtro_col(df, "run_fm_nombrecorto", fondos, fondos_all)
        & _filtro_col(df, "tipo_de_fondo", tipos, tipos_all)  # 👈 estándar
        & _filtro_col(df, "serie", series, series_all)
        & (df["fecha_dia"] >= rango[0])
        & (df["fecha_dia"] <= rango[1])
    )
    if "categoria_agrupada" in df.columns and categorias_agrupadas_all:
        cond = cond & _filtro_col(df, "categoria_agrupada", categorias_agrupadas, categorias_agrupadas_all)

    df_filtrado = df.loc[cond].copy()

    st.session_state.df_filtrado = df_filtrado
    st.session_state.datos_cargados = True
    st.success(f"✅ Datos filtrados: {df_filtrado.shape[0]:,} filas disponibles")
elif "df_filtrado" in st.session_state:
    st.info(f"ℹ️ Usando datos filtrados previamente: {st.session_state.df_filtrado.shape[0]:,} filas")
else:
    st.warning("🔎 Configura los filtros y presiona **Aplicar filtros** para ver datos")

# ===============================
# 📊 Verificación de duplicados (en expander)
# ===============================
with st.expander("🔎 Verificación de duplicados en el dataset", expanded=False):
    clave_duplicados = ["fecha_inf_date", "run_fm", "serie"]
    total_registros = len(df)
    duplicados_exactos = df.duplicated().sum()

    st.markdown(f"📦 **Total de registros:** {total_registros:,}")
    st.markdown(f"🔁 **Filas completamente duplicadas:** {duplicados_exactos:,}")

    if all(c in df.columns for c in clave_duplicados):
        duplicados_clave = df.duplicated(subset=clave_duplicados).sum()
        st.markdown(f"🔁 **Filas duplicadas por clave** {clave_duplicados}: {duplicados_clave:,}")
    else:
        faltantes = [c for c in clave_duplicados if c not in df.columns]
        st.warning(f"⚠️ No se puede verificar duplicados por clave. Faltan columnas: {faltantes}")

# ===============================
# 🧪 Diagnóstico de pérdida de monto (expander)
# ===============================
with st.expander("🧪 Diagnóstico de filtros y montos"):
    df_base_periodo = df[(df["fecha_dia"] >= rango[0]) & (df["fecha_dia"] <= rango[1])]
    tot_base = {
        "patrimonio": df_base_periodo["patrimonio_neto_mm"].sum(skipna=True) if "patrimonio_neto_mm" in df.columns else 0,
        "venta": df_base_periodo["venta_neta_mm"].sum(skipna=True) if "venta_neta_mm" in df.columns else 0,
        "aportes": df_base_periodo["aportes_mm"].sum(skipna=True) if "aportes_mm" in df.columns else 0,
        "rescates": df_base_periodo["rescates_mm"].sum(skipna=True) if "rescates_mm" in df.columns else 0,
        "filas": len(df_base_periodo),
    }
    df_filtrado = st.session_state.get("df_filtrado", pd.DataFrame())
    if not df_filtrado.empty:
        tot_filtrado = {
            "patrimonio": df_filtrado["patrimonio_neto_mm"].sum(skipna=True) if "patrimonio_neto_mm" in df_filtrado.columns else 0,
            "venta": df_filtrado["venta_neta_mm"].sum(skipna=True) if "venta_neta_mm" in df_filtrado.columns else 0,
            "aportes": df_filtrado["aportes_mm"].sum(skipna=True) if "aportes_mm" in df_filtrado.columns else 0,
            "rescates": df_filtrado["rescates_mm"].sum(skipna=True) if "rescates_mm" in df_filtrado.columns else 0,
            "filas": len(df_filtrado),
        }
        st.write("**Totales en el período (antes vs después de filtrar)**")
        st.write(pd.DataFrame([tot_base, tot_filtrado], index=["Antes", "Después"]))
        # Chequeo Venta ≈ Aportes + Rescates (por día)
        req = [c for c in ["fecha_dia", "venta_neta_mm", "aportes_mm", "rescates_mm"] if c in df_filtrado.columns]
        if set(["fecha_dia","venta_neta_mm","aportes_mm","rescates_mm"]).issubset(req):
            agg = (
                df_filtrado[req]
                .groupby("fecha_dia", as_index=False).sum()
            )
            agg["dif"] = (agg["aportes_mm"] + agg["rescates_mm"]) - agg["venta_neta_mm"]
            TOL = 1e-6
            fuera = agg[agg["dif"].abs() > TOL]
            st.write(f"✅ Días OK: {(1 - len(fuera)/max(len(agg),1)):.2%}")
            if not fuera.empty:
                st.warning("Días con descalce (abs(dif)>TOL):")
                st.dataframe(fuera.sort_values("fecha_dia"))
        else:
            st.info("No están todas las columnas para validar Venta = Aportes + Rescates.")
    else:
        st.info("Aplicá filtros para ver diagnóstico.")

# ===============================
# 📌 Footer HTML
# ===============================
st.markdown("<br><br><br><br>", unsafe_allow_html=True)
footer = """
<style>
.footer {
    position: fixed; left: 0; bottom: 0; width: 100%;
    background-color: #f0f2f6; color: #333; text-align: center;
    font-size: 12px; padding: 10px; border-top: 1px solid #ccc; z-index: 999;
}
</style>
<div class="footer">
    Autor: Nicolás Fernández Ponce, CFA | Dashboard de fondos mutuos en Chile – Datos: <a href="https://www.cmfchile.cl" target="_blank">CMF</a>
</div>
"""
st.markdown(footer, unsafe_allow_html=True)