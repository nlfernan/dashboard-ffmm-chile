# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
from io import BytesIO

st.title("📑 Detalle Cartera (ACC) — Valores en CLP")

# 🔄 Botón de recarga para evitar caché vieja (solo dev)
if st.button("🔄 Forzar recarga de datos (dev)"):
    st.cache_data.clear()
    st.rerun()

# ===============================
# 🔧 Config
# ===============================
RUTAS_CANDIDATAS = [
    "app/data_fuentes/cartera_merged_ACC.parquet",
    "backend/data_fuentes/cartera_merged_ACC.parquet",
    "data_fuentes/cartera_merged_ACC.parquet",
]

DEST_COLS = ["fecha_dia","run_fm","nemotecnico","tipo_instrumento","valor_mercado"]

ALIAS_RAW = {
    # fecha
    "fecha_inf_archivo":"fecha_dia","fecha_dia":"fecha_dia","fecha":"fecha_dia",
    "fecha_inf":"fecha_dia","fecha_informe":"fecha_dia",
    # RUT fondo
    "run_fondo":"run_fm","run_fm":"run_fm",
    # nemo
    "nemotecnico_instrumento":"nemotecnico","nemotecnico":"nemotecnico","nemo":"nemotecnico",
    # tipo
    "tipo_instrumento":"tipo_instrumento",
    # VM
    "valorizacion_cierre_m":"valor_mercado","valor_mercado":"valor_mercado","valor_mercado_clp":"valor_mercado",
}
CANDIDATAS_MINIMAS = list(ALIAS_RAW.keys())

# ===============================
# 🧠 Utilidades
# ===============================
def _schema_cols(path: str):
    try:
        import pyarrow.parquet as pq
        return set(pq.ParquetFile(path).schema.names)
    except Exception:
        return None

def _listar_archivos_candidatos():
    for ruta in set(os.path.dirname(p) or "." for p in RUTAS_CANDIDATAS):
        try:
            if os.path.isdir(ruta):
                archivos = sorted(os.listdir(ruta))
                st.info(f"📁 Contenido en `{ruta}`:\n\n" + "\n".join(f"- {a}" for a in archivos[:200]))
            else:
                st.warning(f"📁 Ruta no existe: `{ruta}`")
        except Exception as e:
            st.warning(f"⚠️ No pude listar `{ruta}`: {e}")

@st.cache_data(show_spinner=False)
def _leer_minimo(path: str, candidatas: list) -> pd.DataFrame:
    cols_schema = _schema_cols(path)
    if cols_schema is not None:
        cols_presentes = [c for c in candidatas if c in cols_schema]
        df = pd.read_parquet(path, columns=cols_presentes or None)
    else:
        df = pd.read_parquet(path)
    df = df.rename(columns={c: c.strip().lower().replace(" ", "_").replace(".", "_") for c in df.columns})
    return df

def _to_datetime_safe(s: pd.Series) -> pd.Series:
    if pd.api.types.is_integer_dtype(s):
        return pd.to_datetime(s.astype(str), format="%Y%m%d", errors="coerce")
    out = pd.to_datetime(s, errors="coerce")
    if out.isna().all():
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
            out = pd.to_datetime(s, format=fmt, errors="coerce")
            if not out.isna().all():
                break
    return out

def _localizar_y_cargar_min():
    if "df_cartera" in st.session_state and isinstance(st.session_state.df_cartera, pd.DataFrame):
        return st.session_state.df_cartera.copy(), st.session_state.get("path_cartera", "session_state")
    for ruta in RUTAS_CANDIDATAS:
        if os.path.exists(ruta):
            df = _leer_minimo(ruta, CANDIDATAS_MINIMAS)
            st.session_state.df_cartera = df
            st.session_state.path_cartera = ruta
            return df.copy(), ruta
    st.error("❌ No encontré `df_cartera` en sesión ni el parquet en rutas conocidas.")
    _listar_archivos_candidatos()
    return pd.DataFrame(), None

def _normalizar_y_reducir(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    renames = {}
    for raw, dst in ALIAS_RAW.items():
        if raw in df.columns and dst not in df.columns:
            renames[raw] = dst
    if renames:
        df = df.rename(columns=renames)

    if "fecha_dia" in df.columns:
        df["fecha_dia"] = _to_datetime_safe(df["fecha_dia"])
    if "valor_mercado" in df.columns:
        df["valor_mercado"] = pd.to_numeric(df["valor_mercado"], errors="coerce")
    if "nemotecnico" in df.columns:
        df["nemotecnico"] = df["nemotecnico"].astype(str)

    cols_presentes = [c for c in DEST_COLS if c in df.columns]
    return df[cols_presentes].copy()

# ----- UI helpers con “Seleccionar todo” + lógica -----
def _multiselect_con_todo(label: str, opciones: list):
    opciones_ui = ["(Seleccionar todo)"] + opciones
    return st.multiselect(label, opciones_ui, default=["(Seleccionar todo)"])

def _limpiar_seleccion(seleccion, universo):
    # Si está “Seleccionar todo” y hay algo más, ignoramos “Seleccionar todo”
    if "(Seleccionar todo)" in seleccion:
        if len(seleccion) == 1:
            return list(universo)  # solo “todo”
        else:
            return [x for x in seleccion if x != "(Seleccionar todo)"]
    return seleccion

# ---- Formato chileno ----
def formato_chileno_num(n, decimales=0):
    try:
        if pd.isna(n): return ""
        n = round(float(n), decimales)
        s = f"{n:,.{decimales}f}"
        # 1,234,567.89 -> 1.234.567,89
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return s
    except Exception:
        return str(n)

# ===============================
# 📥 Carga + normalización
# ===============================
df_raw, path_usado = _localizar_y_cargar_min()
if df_raw.empty: st.stop()
df = _normalizar_y_reducir(df_raw)

# Validaciones
if "fecha_dia" not in df.columns or pd.to_datetime(df["fecha_dia"], errors="coerce").dropna().empty:
    st.error("❌ No hay fecha válida en la cartera.")
    st.stop()
if "run_fm" not in df.columns:
    st.error("❌ Falta RUT de fondo (run_fondo/run_fm).")
    st.stop()

# ===============================
# 🔎 Filtros (con botón “Aplicar”)
# ===============================
fechas = (
    pd.to_datetime(df["fecha_dia"], errors="coerce")
    .dropna().dt.date.sort_values(ascending=False).unique().tolist()
)
fecha_sel_raw = st.selectbox("📅 Selecciona una fecha", fechas)

ruts = sorted(df["run_fm"].dropna().unique().tolist())
colA, colB = st.columns(2)
with colA:
    sel_f1_raw = _multiselect_con_todo("Fondo 1 (RUT)", ruts)
with colB:
    sel_f2_raw = _multiselect_con_todo("Fondo 2 (RUT)", ruts)

aplicar = st.button("✅ Aplicar filtros", use_container_width=True)

# Estado persistente
if aplicar:
    st.session_state.fecha_sel = fecha_sel_raw
    st.session_state.f1 = _limpiar_seleccion(sel_f1_raw, ruts)
    st.session_state.f2 = _limpiar_seleccion(sel_f2_raw, ruts)
elif "fecha_sel" not in st.session_state:
    st.session_state.fecha_sel = fecha_sel_raw
    st.session_state.f1 = ruts[:]  # todo
    st.session_state.f2 = ruts[:]  # todo

# Aviso de estado
if aplicar:
    st.success("✅ Filtros aplicados.")
else:
    st.info("ℹ️ Ajustá y presioná **Aplicar filtros** para actualizar.")

fecha_sel = st.session_state.fecha_sel
ruts_fondo1 = st.session_state.f1
ruts_fondo2 = st.session_state.f2

if not ruts_fondo1 and not ruts_fondo2:
    st.warning("Seleccioná al menos un conjunto (Fondo 1 o Fondo 2).")
    st.stop()

# ===============================
# 🎯 Filtrado por fecha del snapshot
# ===============================
df_day = df[pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date()].copy()
if df_day.empty:
    st.warning("⚠️ No hay datos para esa fecha.")
    st.stop()

for col, default in [("nemotecnico", None), ("tipo_instrumento", "N/D"), ("valor_mercado", 0.0)]:
    if col not in df_day.columns: df_day[col] = default
df_day["valor_mercado"] = pd.to_numeric(df_day["valor_mercado"], errors="coerce").fillna(0.0)
df_day["nemotecnico"] = df_day["nemotecnico"].astype(str)

# ===============================
# 🧮 Comparador + fila (Total)
# ===============================
def _agg_por_grupo(df_base: pd.DataFrame, ruts_sel: list, pref: str):
    if not ruts_sel:
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"]), 0.0
    tmp = df_base[df_base["run_fm"].isin(ruts_sel)]
    if tmp.empty:
        return pd.DataFrame(columns=["nemotecnico", f"{pref}_vm", f"{pref}_pct"]), 0.0
    g = tmp.groupby("nemotecnico", as_index=False)["valor_mercado"].sum()
    total = float(g["valor_mercado"].sum())
    g[f"{pref}_vm"] = g["valor_mercado"]
    g[f"{pref}_pct"] = (100.0 * g["valor_mercado"] / total) if total > 0 else 0.0
    g = g.drop(columns=["valor_mercado"])
    return g, total

g1, tot1 = _agg_por_grupo(df_day, ruts_fondo1, "F1")
g2, tot2 = _agg_por_grupo(df_day, ruts_fondo2, "F2")

tabla = pd.merge(g1, g2, on="nemotecnico", how="outer").fillna(0.0)

# Orden y fila total
if not tabla.empty:
    tabla["_orden"] = tabla[["F1_vm", "F2_vm"]].max(axis=1)
    tabla = tabla.sort_values("_orden", ascending=False).drop(columns=["_orden"])

fila_total = pd.DataFrame({
    "nemotecnico": ["(Total)"],
    "F1_vm": [tot1],
    "F1_pct": [100.0 if tot1 > 0 else 0.0],
    "F2_vm": [tot2],
    "F2_pct": [100.0 if tot2 > 0 else 0.0],
})
tabla = pd.concat([tabla, fila_total], ignore_index=True)

# ===============================
# 🖼️ Vista para UI (formato chileno + headers cortos)
# ===============================
tabla_ui = tabla.rename(columns={
    "nemotecnico": "Nemotécnico",
    "F1_vm": "F1 V. de Mercado",
    "F1_pct": "F1 % del Total",
    "F2_vm": "F2 V. de Mercado",
    "F2_pct": "F2 % del Total",
}).copy()

# guardar numéricos puros para exportar y ordenar si hiciera falta
tabla_ui["_F1_vm_num"] = pd.to_numeric(tabla_ui["F1 V. de Mercado"], errors="coerce")
tabla_ui["_F2_vm_num"] = pd.to_numeric(tabla_ui["F2 V. de Mercado"], errors="coerce")
tabla_ui["_F1_pct_num"] = pd.to_numeric(tabla_ui["F1 % del Total"], errors="coerce")
tabla_ui["_F2_pct_num"] = pd.to_numeric(tabla_ui["F2 % del Total"], errors="coerce")

# formateo chileno visible (cadenas)
tabla_ui["F1 V. de Mercado"] = tabla_ui["_F1_vm_num"].apply(lambda x: formato_chileno_num(x, 0))
tabla_ui["F2 V. de Mercado"] = tabla_ui["_F2_vm_num"].apply(lambda x: formato_chileno_num(x, 0))
tabla_ui["F1 % del Total"] = tabla_ui["_F1_pct_num"].apply(lambda x: formato_chileno_num(x, 2))
tabla_ui["F2 % del Total"] = tabla_ui["_F2_pct_num"].apply(lambda x: formato_chileno_num(x, 2))

st.dataframe(
    tabla_ui[["Nemotécnico","F1 V. de Mercado","F1 % del Total","F2 V. de Mercado","F2 % del Total"]],
    use_container_width=True,
    hide_index=True
)
st.caption(f"🔢 Filas: {len(tabla_ui):,}".replace(",", "."))

# ===============================
# ⬇️ Descargar **vista actual** a Excel
# ===============================
def _excel_bytes(tab: pd.DataFrame) -> bytes:
    # Usamos numéricos puros para que Excel conserve números
    df_x = pd.DataFrame({
        "Nemotecnico": tab["Nemotécnico"],
        "F1_V_de_Mercado": tab["_F1_vm_num"],
        "F1_pct_del_Total": tab["_F1_pct_num"]/100 if (tab["_F1_pct_num"].max() > 1.0) else tab["_F1_pct_num"],
        "F2_V_de_Mercado": tab["_F2_vm_num"],
        "F2_pct_del_Total": tab["_F2_pct_num"]/100 if (tab["_F2_pct_num"].max() > 1.0) else tab["_F2_pct_num"],
    })

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_x.to_excel(writer, index=False, sheet_name="Comparador")
        wb = writer.book
        ws = writer.sheets["Comparador"]

        # Formatos (Excel luego aplica locale del usuario)
        fmt_miles = wb.add_format({"num_format": "#,##0"})      # miles y enteros
        fmt_pct   = wb.add_format({"num_format": "0.00%"})      # 2 decimales en %

        # Anchos y formatos por columna
        ws.set_column("A:A", 26)
        ws.set_column("B:B", 18, fmt_miles)
        ws.set_column("C:C", 14, fmt_pct)
        ws.set_column("D:D", 18, fmt_miles)
        ws.set_column("E:E", 14, fmt_pct)

        # Header bold
        header_fmt = wb.add_format({"bold": True})
        for col_idx, _ in enumerate(df_x.columns):
            ws.write(0, col_idx, df_x.columns[col_idx], header_fmt)

    return output.getvalue()

excel_bytes = _excel_bytes(tabla_ui)

st.download_button(
    "📥 Descargar vista en Excel",
    data=excel_bytes,
    file_name=f"detalle_cartera_ACC_{pd.to_datetime(fecha_sel).date()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

# ===============================
# ⬇️ CSV del MES (todos los fondos)
# ===============================
fec = pd.to_datetime(fecha_sel)
anio, mes = int(fec.year), int(fec.month)
primer_dia = pd.Timestamp(anio, mes, 1)
ultimo_dia = pd.Timestamp(anio, mes, calendar.monthrange(anio, mes)[1])

df_month = df[(pd.to_datetime(df["fecha_dia"]) >= primer_dia) & (pd.to_datetime(df["fecha_dia"]) <= ultimo_dia)].copy()

for col, default in [("nemotecnico", None), ("tipo_instrumento", "N/D"), ("valor_mercado", 0.0)]:
    if col not in df_month.columns: df_month[col] = default
df_month["valor_mercado"] = pd.to_numeric(df_month["valor_mercado"], errors="coerce").fillna(0.0)
df_month["nemotecnico"] = df_month["nemotecnico"].astype(str)

@st.cache_data
def _csv_mes_bytes(df_out: pd.DataFrame) -> bytes:
    cols_csv = [c for c in ["fecha_dia","run_fm","nemotecnico","tipo_instrumento","valor_mercado"] if c in df_out.columns]
    df_csv = df_out[cols_csv].rename(columns={
        "fecha_dia":"Fecha","run_fm":"RUT","nemotecnico":"Nemotecnico",
        "tipo_instrumento":"TipoInstrumento","valor_mercado":"ValorMercadoCLP"
    })
    return df_csv.to_csv(index=False).encode("utf-8-sig")

csv_mes = _csv_mes_bytes(df_month)
st.download_button(
    label="⬇️ Bajar CSV — Todos los fondos del mes",
    data=csv_mes,
    file_name=f"cartera_mes_{anio}-{mes:02d}.csv",
    mime="text/csv",
    use_container_width=True
)

# ===============================
# 🔎 Verificación de duplicados (al final)
# ===============================
with st.expander("🔎 Verificación de duplicados"):
    st.markdown("**Nivel dataset completo**")
    total_registros = len(df)
    duplicados_exactos = df.duplicated().sum()
    st.write(f"📦 Total de registros: {total_registros:,}".replace(",", "."))
    st.write(f"🔁 Filas completamente duplicadas: {duplicados_exactos:,}".replace(",", "."))

    st.markdown("---")
    st.markdown(f"**Nivel snapshot {pd.to_datetime(fecha_sel).date()}**")
    df_snap = df[pd.to_datetime(df["fecha_dia"]).dt.date == pd.to_datetime(fecha_sel).date()]
    st.write(f"📦 Registros en snapshot: {len(df_snap):,}".replace(",", "."))
    st.write(f"🔁 Duplicados exactos en snapshot: {df_snap.duplicated().sum():,}".replace(",", "."))

    st.markdown("---")
    st.markdown("**Duplicados por clave `['fecha_dia','run_fm','nemotecnico']`**")
    for target, nombre in [(df, "dataset"), (df_snap, "snapshot")]:
        if all(c in target.columns for c in ["fecha_dia","run_fm","nemotecnico"]):
            dups = target.duplicated(subset=["fecha_dia","run_fm","nemotecnico"]).sum()
            st.write(f"🔁 En {nombre}: {dups:,}".replace(",", "."))
        else:
            faltan = [c for c in ["fecha_dia","run_fm","nemotecnico"] if c not in target.columns]
            st.warning(f"⚠️ No se puede verificar por clave en {nombre}. Faltan columnas: {faltan}")

# ===============================
# 📌 Marcas
# ===============================
st.markdown(f"📂 Usando parquet: `{st.session_state.get('path_cartera', '')}`")
st.markdown(f"🗓️ Fecha efectiva en vista: **{pd.to_datetime(fecha_sel).date()}**")
