# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from pathlib import Path
import io

st.set_page_config(page_title="AFPs - Métricas VC", layout="wide")
st.title("📈 AFPs — Métricas de Valor Cuota (rolling)")

# ===============================
# 🔧 Rutas candidatas (parquet y csv fallback)
# ===============================
RUTAS = [
    r"C:\Users\nlfer\Desktop\Proyectos\Fondos Mutuos Chile\dashboard-ffmm-chile\backend\data_fuentes\vc_metricas_rolling.parquet",
    "backend/data_fuentes/vc_metricas_rolling.parquet",
    "data_fuentes/vc_metricas_rolling.parquet",
    "vc_metricas_rolling.parquet",
]
CSV_FALLBACK = "/mnt/data/vc_metricas_rolling.csv"

# ===============================
# ♻️ Carga con cache
# ===============================
@st.cache_data(show_spinner=True)
def cargar_datos():
    # 1) Intentar Parquet
    for ruta in RUTAS:
        p = Path(ruta)
        if p.exists():
            df = pd.read_parquet(p)
            origen = f"parquet: {p}"
            break
    else:
        # 2) Fallback CSV
        p = Path(CSV_FALLBACK)
        if p.exists():
            df = pd.read_csv(p)
            origen = f"csv: {p}"
        else:
            raise FileNotFoundError(
                "No encontré el parquet en las rutas candidatas ni el CSV fallback.\n"
                f"Probé:\n- " + "\n- ".join(RUTAS) + f"\nY fallback: {CSV_FALLBACK}"
            )

    df = df.copy()
    df.columns = df.columns.str.strip()

    # Detectar fecha
    posibles_fechas = ["fecha", "fecha_inf_date", "fecha_corte"]
    col_fecha = next((c for c in posibles_fechas if c in df.columns), None)
    if col_fecha:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce").dt.date

    # Detectar AFP y Fondo/Serie
    posibles_afp = ["afp", "administradora", "nom_afp", "nom_adm"]
    col_afp = next((c for c in posibles_afp if c in df.columns), None)

    posibles_fondo = ["fondo", "tipo_fondo", "tipo", "serie"]
    col_fondo = next((c for c in posibles_fondo if c in df.columns), None)

    # Orden sugerido
    orden_pref = []
    if col_fecha: orden_pref.append(col_fecha)
    if col_afp:   orden_pref.append(col_afp)
    if col_fondo: orden_pref.append(col_fondo)

    metricas_sugeridas = [
        "rentabilidad_diaria",
        "ret_anual_1a", "ret_anual_2a", "ret_anual_3a", "ret_anual_5a",
        "std_anual_1a", "std_anual_2a", "std_anual_3a", "std_anual_5a",
        "vol_anual_1a", "vol_anual_2a", "vol_anual_3a", "vol_anual_5a",
    ]
    orden_pref += [m for m in metricas_sugeridas if m in df.columns]
    resto = [c for c in df.columns if c not in orden_pref]
    df = df[orden_pref + resto] if orden_pref else df

    info = {"origen": origen, "col_fecha": col_fecha, "col_afp": col_afp, "col_fondo": col_fondo}
    return df, info

# ===============================
# 🚚 Cargar
# ===============================
try:
    df, info = cargar_datos()
    st.caption(f"Fuente de datos → **{info['origen']}**")
except Exception as e:
    st.error(f"❌ Error cargando datos: {e}")
    with st.expander("Ver rutas probadas"):
        st.write({"parquet_candidates": RUTAS, "csv_fallback": CSV_FALLBACK})
    st.stop()

if df.empty:
    st.warning("El dataset está vacío.")
    st.stop()

# ===============================
# 🎛️ Filtros en la hoja (sin sidebar)
# ===============================
st.subheader("Filtros")

col1, col2, col3 = st.columns([1.2, 1, 1])

# Rango de fecha
if info["col_fecha"]:
    fmin = df[info["col_fecha"]].min()
    fmax = df[info["col_fecha"]].max()
    with col1:
        rango = st.date_input(
            "Rango de fecha",
            value=(fmin, fmax),
            min_value=fmin, max_value=fmax,
            format="YYYY/MM/DD",
            key="filtro_fecha"
        )
else:
    rango = None

# AFP / Administradora
if info["col_afp"]:
    vals_afp = sorted([v for v in df[info["col_afp"]].dropna().unique()])
    with col2:
        sel_afp = st.multiselect(
            "AFP / Administradora",
            options=vals_afp,
            default=vals_afp,
            key="filtro_afp"
        )
else:
    sel_afp = None

# Fondo / Serie
if info["col_fondo"]:
    vals_fondo = sorted([v for v in df[info["col_fondo"]].dropna().unique()])
    with col3:
        sel_fondo = st.multiselect(
            "Fondo / Serie",
            options=vals_fondo,
            default=vals_fondo,
            key="filtro_fondo"
        )
else:
    sel_fondo = None

# Botón aplicar (para evitar recalcular cada cambio)
aplicar = st.button("Aplicar filtros", type="primary")

# ===============================
# 🧮 Aplicación de filtros
# ===============================
if "df_filtrado" not in st.session_state or aplicar:
    df_filtrado = df
    if info["col_fecha"] and rango:
        ini, fin = rango if isinstance(rango, tuple) else (rango, rango)
        df_filtrado = df_filtrado[
            (df_filtrado[info["col_fecha"]] >= ini) & (df_filtrado[info["col_fecha"]] <= fin)
        ]
    if info["col_afp"] and sel_afp:
        df_filtrado = df_filtrado[df_filtrado[info["col_afp"]].isin(sel_afp)]
    if info["col_fondo"] and sel_fondo:
        df_filtrado = df_filtrado[df_filtrado[info["col_fondo"]].isin(sel_fondo)]
    st.session_state.df_filtrado = df_filtrado.copy()
else:
    df_filtrado = st.session_state.df_filtrado

st.success(f"Registros filtrados: {len(df_filtrado):,}")

# ===============================
# 📄 Tabla y descargas
# ===============================
st.dataframe(df_filtrado, use_container_width=True)

csv_bytes = df_filtrado.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar CSV filtrado", data=csv_bytes,
                   file_name="afps_metricas_filtrado.csv", mime="text/csv")

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
    buffer = io.BytesIO()
    df_filtrado.to_parquet(buffer, index=False)
    st.download_button("⬇️ Descargar Parquet filtrado", data=buffer.getvalue(),
                       file_name="afps_metricas_filtrado.parquet", mime="application/octet-stream")
except Exception:
    st.caption("Para exportar Parquet instalá pyarrow en el entorno de la app.")

# ===============================
# 🔎 Diagnóstico opcional
# ===============================
with st.expander("Ver columnas y tipos"):
    st.write(df_filtrado.dtypes.astype(str))
