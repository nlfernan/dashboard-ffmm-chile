# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar

st.title("📑 Detalle Cartera (ACC) — Valores en MM CLP")

# ===============================
# 🔧 Config
# ===============================
RUTAS_CANDIDATAS = [
    "app/data_fuentes/cartera_merged_ACC.parquet",
    "backend/data_fuentes/cartera_merged_ACC.parquet",
    "data_fuentes/cartera_merged_ACC.parquet",
]

DEST_COLS = ["fecha_dia","run_fm","nombre_fondo","nemotecnico","tipo_instrumento","valor_mercado"]

ALIAS_RAW = {
    # fecha
    "fecha_inf_archivo":"fecha_dia","fecha_dia":"fecha_dia","fecha":"fecha_dia",
    "fecha_inf":"fecha_dia","fecha_informe":"fecha_dia",
    # RUT fondo
    "run_fondo":"run_fm","run_fm":"run_fm",
    # nombre del fondo
    "nombre_fondo":"nombre_fondo",
    # nemotécnico
    "nemotecnico_instrumento":"nemotecnico","nemotecnico":"nemotecnico","nemo":"nemotecnico",
    # tipo
    "tipo_instrumento":"tipo_instrumento",
    # valor (en miles)
    "valorizacion_cierre_m":"valor_mercado","valor_mercado":"valor_mercado","valor_mercado_clp":"valor_mercado",
}
CANDIDATAS_MINIMAS = list(ALIAS_RAW.keys())

ESCALA_MM = 1000.0  # viene en miles → mostramos/descargamos en millones

# ===============================
# 🧠 Utilidades
# ===============================
def _schema_cols(path: str):
    try:
        import pyarrow.parquet as pq
        return set(pq.ParquetFile(path).schema.names)
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def _leer_minimo(path: str, candidatas: list) -> pd.DataFrame:
    cols_schema = _schema_cols(path)
    if cols_schema is not None:
        cols_presentes = [c for c in candidatas if c in cols_schema]
        df = pd.read_parquet(path, columns=cols_presentes or None)
    else:
        df = pd.read_parquet(path)
    # normalizo nombres
    df = df.rename(columns={c: c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns})
    return df

def _localizar_y_cargar_min():
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera.copy(), st.session_state.get("path_cartera", "session_state")
    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = _leer_minimo(ruta, CANDIDATAS_MINIMAS)
            st.session_state.df_cartera = df
            st.session_state.path_cartera = ruta
            return df.copy(), ruta
    st.error("❌ No encontré el parquet en rutas conocidas.")
    return pd.DataFrame(), None

def _to_datetime_safe(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, errors="coerce")
    if out.isna().all() and pd.api.types.is_integer_dtype(s):
        out = pd.to_datetime(s.astype(str), format="%Y%m%d", errors="coerce")
    return out

def _normalizar_y_reducir(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    # mapear alias -> destino
    renames = {}
    for raw, dst in ALIAS_RAW.items():
        if raw in df.columns and dst not in df.columns:
            renames[raw] = dst
    if renames:
        df = df.rename(columns=renames)

    # tipos
    if "fecha_dia" in df.columns:
        df["fecha_dia"] = _to_datetime_safe(df["fecha_dia"])
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce").astype(float)
    for c in ["nemotecnico","run_fm","nombre_fondo","tipo_instrumento"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    cols_presentes = [c for c in DEST_COLS if c in df.columns]
    return df[cols_presentes].copy()

def _multiselect_con_todo(label: str, opciones: list):
    opciones_ui = ["(Seleccionar todo)"] + opciones
    return st.multiselect(label, opciones_ui, default=["(Seleccionar todo)"])

def _limpiar_seleccion(seleccion, universo):
    if "(Seleccionar todo)" in seleccion:
        if len(seleccion) == 1:
            return list(universo)
        else:
            return [x for x in seleccion if x != "(Seleccionar todo)"]
    return seleccion

# ===============================
# 📥 Carga + normalización
# ===============================
df_raw, path_usado = _localizar_y_cargar_min()
if df_raw.empty: st.stop()
df = _normalizar_y_reducir(df_raw)

# Validaciones mínimas
if "fecha_dia" not in df.columns or pd.to_datetime(df["fecha_dia"], errors="coerce").dropna().empty:
    st.error("❌ No hay fecha válida en la cartera."); st.stop()
if "run_fm" not in df.columns:
    st.error("❌ Falta RUT de fondo (run_fm)."); st.stop()
if "nombre_fondo" not in df.columns:
    st.error("❌ Falta la columna `nombre_fondo` en el parquet."); st.stop()

# ===============================
# 🔎 Filtros — label RUT - nombre_fondo
# ===============================
df["_rut_nombre"] = df["run_fm"].astype(str) + " - " + df["nombre_fondo"].astype(str)

fechas = (
    pd.to_datetime(df["fecha_dia"], errors="coerce")
    .dropna().dt.date.sort_values(ascending=False).unique().tolist()
)
fecha_sel_raw = st.selectbox("📅 Fecha de snapshot", fechas)

fondos_labels = sorted(df["_rut_nombre"].dropna().unique().tolist())

colA, colB = st.columns(2)
with colA:
    sel_f1_raw = _multiselect_con_todo("Fondo 1 (RUT - Nombre)", fondos_labels)
with colB:
    sel_f2_raw = _multiselect_con_todo("Fondo 2 (RUT - Nombre)", fondos_labels)

aplicar = st.button("✅ Aplicar filtros", use_container_width=True)

# map label -> RUT
label_to_rut = dict(zip(df["_rut_nombre"], df["run_fm"]))
def _labels_a_ruts(labels: list) -> list:
    return sorted({label_to_rut.get(x) for x in labels if x in label_to_rut})

if aplicar:
    st.session_state.fecha_sel = fecha_sel_raw
    st.session_state.f1 = _labels_a_ruts(_limpiar_seleccion(sel_f1_raw, fondos_labels))
    st.session_state.f2 = _labels_a_ruts(_limpiar_seleccion(sel_f2_raw, fondos_labels))
elif "fecha_sel" not in st.session_state:
    st.session_state.fecha_sel = fecha_sel_raw
    st.session_state.f1 = sorted(df["run_fm"].unique().tolist())
    st.session_state.f2 = sorted(df["run_fm"].unique().tolist())

fecha_sel = st.session_state.fecha_sel
ruts_fondo1 = st.session_state.f1
ruts_fondo2 = st.session_state.f2
if not ruts_fondo1 and not ruts_fondo2:
    st.warning("Seleccioná al menos un conjunto (Fondo 1 o Fondo 2)."); st.stop()

# ===============================
# 🎯 Filtrado por fecha del snapshot
# ===============================
df_day = df[pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date()].copy()
if df_day.empty:
    st.warning("⚠️ No hay datos para esa fecha."); st.stop()

for col, default in [("nemotecnico", None), ("tipo_instrumento", "N/D"), ("valor_mercado", 0.0)]:
    if col not in df_day.columns: df_day[col] = default
df_day["valor_mercado"] = pd.to_numeric(df_day["valor_mercado"], errors="coerce").astype(float).fillna(0.0)
df_day["nemotecnico"] = df_day["nemotecnico"].astype(str)

# ===============================
# 🧮 Comparador + fila (Total) — usando millones
# ===============================
def _agg_por_grupo(df_base: pd.DataFrame, ruts_sel: list, pref: str):
    if not ruts_sel:
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"]), 0.0
    tmp = df_base[df_base["run_fm"].isin(ruts_sel)].copy()
    if tmp.empty:
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"]), 0.0
    tmp["vm_m"] = tmp["valor_mercado"] / ESCALA_MM  # a MM
    g = tmp.groupby("nemotecnico", as_index=False)["vm_m"].sum()
    total = float(g["vm_m"].sum()) if not g.empty else 0.0
    g[f"{pref}_vm"] = g["vm_m"].astype(float)
    g[f"{pref}_pct"] = (100.0 * g["vm_m"] / total) if total > 0 else 0.0
    g = g.drop(columns=["vm_m"])
    return g, total

g1, tot1 = _agg_por_grupo(df_day, ruts_fondo1, "F1")
g2, tot2 = _agg_por_grupo(df_day, ruts_fondo2, "F2")

tabla = pd.merge(g1, g2, on="nemotecnico", how="outer").fillna(0.0)

if not tabla.empty:
    tabla["_orden"] = tabla[["F1_vm", "F2_vm"]].max(axis=1)
    tabla = tabla.sort_values("_orden", ascending=False).drop(columns=["_orden"])

# Fila total
fila_total = pd.DataFrame({
    "nemotecnico": ["(Total)"],
    "F1_vm": [float(tot1)],
    "F1_pct": [100.0 if tot1 > 0 else 0.0],
    "F2_vm": [float(tot2)],
    "F2_pct": [100.0 if tot2 > 0 else 0.0],
})
tabla = pd.concat([tabla, fila_total], ignore_index=True)

# ===============================
# ➕ Columna %Dif (numérica)
# ===============================
tabla["pct_dif"] = pd.to_numeric(tabla["F1_pct"], errors="coerce") - pd.to_numeric(tabla["F2_pct"], errors="coerce")

# ===============================
# 🖼️ Vista UI — headers cortos y formato “gringo” (display text)
# ===============================
def _fmt_us_int(x):
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return ""

tabla_ui = pd.DataFrame({
    "Nemotécnico": tabla["nemotecnico"].astype(str),
    "F1 V°deM°": tabla["F1_vm"],         # num
    "F1 %": tabla["F1_pct"],              # num
    "F2 V°deM°": tabla["F2_vm"],         # num
    "F2 %": tabla["F2_pct"],              # num
    "%Dif": tabla["pct_dif"],             # num
})

# columnas de display con separador de miles (texto)
tabla_ui["F1 V°deM° (disp)"] = tabla_ui["F1 V°deM°"].apply(_fmt_us_int)
tabla_ui["F2 V°deM° (disp)"] = tabla_ui["F2 V°deM°"].apply(_fmt_us_int)

# dataframe para mostrar: usamos las columnas (disp) para ver comas
mostrar = tabla_ui[[
    "Nemotécnico",
    "F1 V°deM° (disp)", "F1 %",
    "F2 V°deM° (disp)", "F2 %",
    "%Dif"
]].rename(columns={
    "F1 V°deM° (disp)": "F1 V°deM°",
    "F2 V°deM° (disp)": "F2 V°deM°",
})

# Config: % y %Dif numéricos (2 decimales). V°deM° es texto (con comas).
col_config = {
    "Nemotécnico": st.column_config.TextColumn("Nemotécnico", width="medium"),
    "F1 V°deM°": st.column_config.TextColumn("F1 V°deM°", width="small"),
    "F1 %": st.column_config.NumberColumn("F1 %", format="%.2f%%"),
    "F2 V°deM°": st.column_config.TextColumn("F2 V°deM°", width="small"),
    "F2 %": st.column_config.NumberColumn("F2 %", format="%.2f%%"),
    "%Dif": st.column_config.NumberColumn("%Dif", format="%.2f%%"),
}

st.dataframe(
    mostrar,
    use_container_width=True,
    hide_index=True,
    column_config=col_config
)
st.caption(f"🔢 Filas: {len(mostrar):,}")

# ===============================
# ⬇️ Descargar **vista actual** a CSV (numérico, en MM)
# ===============================
@st.cache_data
def _csv_vista_bytes(tab_num: pd.DataFrame) -> bytes:
    df_out = pd.DataFrame({
        "Nemotecnico": tab_num["Nemotécnico"],
        "F1_V_de_M": pd.to_numeric(tab_num["F1 V°deM°"], errors="coerce").round(0),
        "F1_pct": pd.to_numeric(tab_num["F1 %"], errors="coerce").round(2),
        "F2_V_de_M": pd.to_numeric(tab_num["F2 V°deM°"], errors="coerce").round(0),
        "F2_pct": pd.to_numeric(tab_num["F2 %"], errors="coerce").round(2),
        "pct_dif": pd.to_numeric(tab_num["%Dif"], errors="coerce").round(2),
    })
    return df_out.to_csv(index=False).encode("utf-8-sig")

csv_vista = _csv_vista_bytes(tabla_ui)  # usa columnas numéricas internas
st.download_button(
    "📥 Descargar vista actual (CSV, MM)",
    data=csv_vista,
    file_name=f"detalle_cartera_ACC_{pd.to_datetime(fecha_sel).date()}_MM.csv",
    mime="text/csv",
    use_container_width=True
)

# ===============================
# ⬇️ CSV del MES (todos los fondos) — en MM
# ===============================
fec = pd.to_datetime(fecha_sel)
anio, mes = int(fec.year), int(fec.month)
primer_dia = pd.Timestamp(anio, mes, 1)
ultimo_dia = pd.Timestamp(anio, mes, calendar.monthrange(anio, mes)[1])

df_month = df[(pd.to_datetime(df["fecha_dia"]) >= primer_dia) & (pd.to_datetime(df["fecha_dia"]) <= ultimo_dia)].copy()
df_month["valor_mercado"] = pd.to_numeric(df_month["valor_mercado"], errors="coerce").astype(float).fillna(0.0)

@st.cache_data
def _csv_mes_bytes(df_out: pd.DataFrame) -> bytes:
    df_out = df_out.copy()
    df_out["ValorMercadoMM"] = (df_out["valor_mercado"] / ESCALA_MM).round(0)
    df_csv = df_out.rename(columns={
        "fecha_dia":"Fecha","run_fm":"RUT","nemotecnico":"Nemotecnico",
        "tipo_instrumento":"TipoInstrumento"
    })
    cols = [c for c in ["Fecha","RUT","Nemotecnico","TipoInstrumento","ValorMercadoMM"] if c in df_csv.columns]
    return df_csv[cols].to_csv(index=False).encode("utf-8-sig")

csv_mes = _csv_mes_bytes(df_month)
st.download_button(
    label="⬇️ Bajar CSV — Todos los fondos del mes (MM CLP)",
    data=csv_mes,
    file_name=f"cartera_mes_{anio}-{mes:02d}_MM.csv",
    mime="text/csv",
    use_container_width=True
)

# ===============================
# 📌 Marcas
# ===============================
st.markdown(f"📂 Usando parquet: `{st.session_state.get('path_cartera', '')}`")
st.markdown(f"🗓️ Fecha efectiva en vista: **{pd.to_datetime(fecha_sel).date()}**")
