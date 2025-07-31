# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
import random
import unicodedata
from datetime import date, timedelta
from openai import OpenAI, RateLimitError

# -------------------------------
# 🔐 Login
# -------------------------------
USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")

if "logueado" not in st.session_state:
    st.session_state.logueado = False
if "requiere_login" not in st.session_state:
    st.session_state.requiere_login = random.randint(1, 3) == 1

if st.session_state.requiere_login and not st.session_state.logueado:
    st.title("🔐 Acceso al Dashboard")
    usuario = st.text_input("Usuario")
    clave = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if usuario == USER and clave == PASS:
            st.session_state.logueado = True
            st.success("✅ Acceso concedido. Cargando dashboard...")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")
    st.stop()

# -------------------------------
# 🔑 Conexión a OpenAI
# -------------------------------
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    st.error("❌ No se encontró la variable OPENAI_API_KEY.")
    st.stop()

client = OpenAI(api_key=OPENAI_KEY)

# -------------------------------
# 📂 Leer Parquet original y normalizar columnas
# -------------------------------
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"

def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

def hacer_unicas(cols):
    seen = {}
    nuevas = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            nuevas.append(c)
        else:
            seen[c] += 1
            nuevas.append(f"{c}_{seen[c]}")
    return nuevas

@st.cache_data
def cargar_datos():
    if not os.path.exists(PARQUET_PATH):
        st.error(f"❌ No se encontró el archivo Parquet en {PARQUET_PATH}")
        st.stop()
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    df.columns = hacer_unicas(df.columns)
    columnas = [c for c in [
        "fecha_inf_date", "run_fm", "nombre_corto", "nom_adm",
        "patrimonio_neto_mm", "venta_neta_mm", "aportes_mm", "rescates_mm",
        "tipo_fm", "categoria", "serie"
    ] if c in df.columns]
    return df[columnas]

df = cargar_datos()

if df.empty:
    st.warning("No hay datos disponibles en el archivo Parquet.")
    st.stop()

# -------------------------------
# Preprocesamiento
# -------------------------------
df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])
df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# -------------------------------
# Título
# -------------------------------
st.markdown("""
<div style='display: flex; align-items: center; gap: 15px; padding-top: 10px;'>
    <img src='https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Owl_in_the_Moonlight.jpg/640px-Owl_in_the_Moonlight.jpg'
         width='60' style='border-radius: 50%; box-shadow: 0 2px 6px rgba(0,0,0,0.2);'/>
    <h1 style='margin: 0; font-size: 2.2em;'>Dashboard Fondos Mutuos</h1>
</div>
""", unsafe_allow_html=True)

# -------------------------------
# Rango de Fechas
# -------------------------------
st.markdown("### Rango de Fechas")

fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted(df["fecha_inf_date"].dt.year.unique())
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

df = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) &
        (df["fecha_inf_date"].dt.date <= fecha_fin)]

if df.empty:
    st.warning("No hay datos para el rango seleccionado.")
    st.stop()

if "rango_fechas" not in st.session_state:
    st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

# -------------------------------
# Multiselect con "Seleccionar todo"
# -------------------------------
def multiselect_con_todo(label, opciones, key):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)

    if key not in st.session_state:
        st.session_state[key] = ["(Seleccionar todo)"]

    seleccion = st.multiselect(
        label,
        opciones_mostradas,
        default=st.session_state[key],
        key=f"ms_{key}"
    )

    # ✅ desmarcar "(Seleccionar todo)" si hay otras opciones elegidas
    if "(Seleccionar todo)" in seleccion and len(seleccion) > 1:
        seleccion = [s for s in seleccion if s != "(Seleccionar todo)"]
        st.session_state[key] = seleccion
        st.rerun()

    # ✅ si no hay selección, volvemos a todo
    if not seleccion:
        st.session_state[key] = ["(Seleccionar todo)"]
        return list(opciones)

    # ✅ si sólo está "(Seleccionar todo)", devolvemos todas las opciones
    if seleccion == ["(Seleccionar todo)"]:
        return list(opciones)

    return seleccion

# -------------------------------
# Filtros principales (cacheando opciones)
# -------------------------------
@st.cache_data
def opciones_categoria(df):
    return sorted(df["categoria"].dropna().unique())

@st.cache_data
def opciones_adm(df, categorias):
    return sorted(df[df["categoria"].isin(categorias)]["nom_adm"].dropna().unique())

@st.cache_data
def opciones_fondo(df, adms):
    return sorted(df[df["nom_adm"].isin(adms)]["run_fm_nombrecorto"].dropna().unique())

categoria_opciones = opciones_categoria(df)
categoria_seleccionadas = multiselect_con_todo("Categoría", categoria_opciones, key="filtro_categoria")

adm_opciones = opciones_adm(df, categoria_seleccionadas)
adm_seleccionadas = multiselect_con_todo("Administradora(s)", adm_opciones, key="filtro_adm")

fondo_opciones = opciones_fondo(df, adm_seleccionadas)
fondo_seleccionados = multiselect_con_todo("Fondo(s)", fondo_opciones, key="filtro_fondo")

with st.expander("🔧 Filtros adicionales"):
    tipo_opciones = sorted(df["tipo_fm"].dropna().unique())
    tipo_seleccionados = multiselect_con_todo("Tipo de Fondo", tipo_opciones, key="filtro_tipo")

    serie_opciones = sorted(df[df["run_fm_nombrecorto"].isin(fondo_seleccionados)]["serie"].dropna().unique())
    serie_seleccionadas = multiselect_con_todo("Serie(s)", serie_opciones, key="filtro_serie")

    st.markdown("#### Ajuste fino de fechas")
    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    hoy_df = fecha_max_real
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=30), fecha_min_real), hoy_df)
    if col_b.button("3M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=90), fecha_min_real), hoy_df)
    if col_c.button("6M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=180), fecha_min_real), hoy_df)
    if col_d.button("MTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, hoy_df.month, 1), hoy_df)
    if col_e.button("YTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, 1, 1), hoy_df)

# -------------------------------
# Aplicar filtros al DataFrame
# -------------------------------
rango_fechas = st.session_state["rango_fechas"]

df_filtrado = df[df["tipo_fm"].isin(tipo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["categoria"].isin(categoria_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["nom_adm"].isin(adm_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["run_fm_nombrecorto"].isin(fondo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["serie"].isin(serie_seleccionadas)]
df_filtrado = df_filtrado[(df_filtrado["fecha_inf_date"].dt.date >= rango_fechas[0]) &
                          (df_filtrado["fecha_inf_date"].dt.date <= rango_fechas[1])]

if df_filtrado.empty:
    st.warning("No hay datos disponibles con los filtros seleccionados.")
    st.stop()

# -------------------------------
# Tabs
# -------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Patrimonio Neto Total (MM CLP)",
    "Venta Neta Acumulada (MM CLP)",
    "Listado de Fondos Mutuos",
    "💡 Insight IA"
])

with tab1:
    st.subheader("Evolución del Patrimonio Neto Total (en millones de CLP)")
    patrimonio_total = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["patrimonio_neto_mm"]
        .sum()
        .sort_index()
    )
    patrimonio_total.index = pd.to_datetime(patrimonio_total.index)
    st.bar_chart(patrimonio_total, height=300, use_container_width=True)

with tab2:
    st.subheader("Evolución acumulada de la Venta Neta (en millones de CLP)")
    venta_neta_acumulada = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["venta_neta_mm"]
        .sum()
        .cumsum()
        .sort_index()
    )
    venta_neta_acumulada.index = pd.to_datetime(venta_neta_acumulada.index)
    st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

    with st.expander("📊 Ver Aportes y Rescates acumulados"):
        st.markdown("#### Evolución acumulada de Aportes (en millones de CLP)")
        aportes_acumulados = (
            df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["aportes_mm"]
            .sum()
            .cumsum()
            .sort_index()
        )
        aportes_acumulados.index = pd.to_datetime(aportes_acumulados.index)
        st.bar_chart(aportes_acumulados, height=250, use_container_width=True)

        st.markdown("#### Evolución acumulada de Rescates (en millones de CLP)")
        rescates_acumulados = (
            df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["rescates_mm"]
            .sum()
            .cumsum()
            .sort_index()
        )
        rescates_acumulados.index = pd.to_datetime(rescates_acumulados.index)
        st.bar_chart(rescates_acumulados, height=250, use_container_width=True)

with tab3:
    ranking_ventas = (
        df_filtrado
        .groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
        .head(20)
        .copy()
    )

    total_fondos = df_filtrado[["run_fm", "nombre_corto", "nom_adm"]].drop_duplicates().shape[0]
    cantidad_ranking = ranking_ventas.shape[0]

    if total_fondos <= 20:
        titulo = f"Listado de Fondos Mutuos (total: {total_fondos})"
    else:
        titulo = f"Listado de Fondos Mutuos (top {cantidad_ranking} por Venta Neta de {total_fondos})"

    st.subheader(titulo)

    def generar_url_cmf(rut):
        return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI"

    ranking_ventas["URL CMF"] = ranking_ventas["run_fm"].astype(str).apply(generar_url_cmf)

    ranking_ventas = ranking_ventas.rename(columns={
        "run_fm": "RUT",
        "nombre_corto": "Nombre del Fondo",
        "nom_adm": "Administradora",
        "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
    })

    ranking_ventas["Venta Neta Acumulada (MM CLP)"] = ranking_ventas["Venta Neta Acumulada (MM CLP)"].apply(
        lambda x: f"{x:,.0f}".replace(",", ".")
    )

    ranking_ventas["URL CMF"] = ranking_ventas["URL CMF"].apply(lambda x: f'<a href="{x}" target="_blank">Ver en CMF</a>')

    st.markdown(ranking_ventas.to_html(index=False, escape=False), unsafe_allow_html=True)

    st.markdown("### Descargar datos filtrados")
    MAX_FILAS = 50_000
    st.caption(f"🔢 Total de filas: {df_filtrado.shape[0]:,}")

    if df_filtrado.shape[0] > MAX_FILAS:
        st.warning(f"⚠️ La descarga está limitada a {MAX_FILAS:,} filas. Aplicá más filtros para reducir el tamaño (actual: {df_filtrado.shape[0]:,} filas).")
    else:
        @st.cache_data(hash_funcs={pd.DataFrame: lambda _: None})
        def generar_csv(df):
            return df.to_csv(index=False).encode("utf-8-sig")

        csv_data = generar_csv(df_filtrado)

        st.download_button(
            label="⬇️ Descargar CSV",
            data=csv_data,
            file_name="ffmm_filtrado.csv",
            mime="text/csv"
        )

with tab4:
    st.subheader("💡 Insight IA basado en Top 20 Fondos")

    top_fondos = (
        df_filtrado
        .groupby(["run_fm", "nombre_corto", "nom_adm"])["venta_neta_mm"]
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .reset_index()
    )

    top_fondos["venta_neta_mm"] = top_fondos["venta_neta_mm"].apply(lambda x: f"{x:,.0f}".replace(",", "."))

    contexto = top_fondos.to_string(index=False)

    if st.button("🔍 Generar Insight IA"):
        try:
            prompt = f"""Analiza el top 20 de fondos mutuos basado en venta neta acumulada.
            Responde en español, completo pero breve (máximo 6 oraciones).
            Prioriza tendencias generales, riesgos y oportunidades clave.

            Datos:
            {contexto}
            """
            with st.spinner("Analizando con GPT-4o-mini..."):
                respuesta = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "Eres un analista financiero especializado en fondos mutuos en Chile."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=800
                )
            st.success(respuesta.choices[0].message.content)
        except RateLimitError:
            st.error("⚠️ No hay crédito disponible en la cuenta de OpenAI.")

    st.markdown("### 💬 Chat con IA usando el Top 20")
    if "chat_historial" not in st.session_state:
        st.session_state.chat_historial = []

    for msg in st.session_state.chat_historial:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    pregunta = st.chat_input("Escribí tu pregunta sobre los fondos")
    if pregunta:
        st.session_state.chat_historial.append({"role": "user", "content": pregunta})
        with st.chat_message("user"):
            st.markdown(pregunta)

        try:
            prompt_chat = f"""Usa estos datos de contexto:\n{contexto}\n\n
            Responde en español, completo pero breve (máximo 6 oraciones).
            Pregunta: {pregunta}"""
            with st.chat_message("assistant"):
                with st.spinner("Analizando..."):
                    respuesta_chat = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "Eres un analista financiero especializado en fondos mutuos en Chile."},
                            *[{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_historial],
                            {"role": "user", "content": prompt_chat}
                        ],
                        max_tokens=800
                    )
                    output = respuesta_chat.choices[0].message.content
                    st.markdown(output)
                    st.session_state.chat_historial.append({"role": "assistant", "content": output})
        except RateLimitError:
            st.error("⚠️ No hay crédito disponible en la cuenta de OpenAI.")

    with st.expander("📊 Ver Top 20 Fondos Mutuos"):
        st.dataframe(top_fondos.rename(columns={
            "run_fm": "RUT",
            "nombre_corto": "Nombre del Fondo",
            "nom_adm": "Administradora",
            "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
        }), use_container_width=True)

# -------------------------------
# Footer
# -------------------------------
st.markdown("<br><br><br><br>", unsafe_allow_html=True)

footer = """
<style>
.footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    background-color: #f0f2f6;
    color: #333;
    text-align: center;
    font-size: 12px;
    padding: 10px;
    border-top: 1px solid #ccc;
    z-index: 999;
}
</style>

<div class="footer">
    Autor: Nicolás Fernández Ponce, CFA | Este dashboard muestra la evolución del patrimonio y las ventas netas de fondos mutuos en Chile.  
    Datos provistos por la <a href="https://www.cmfchile.cl" target="_blank">CMF</a>
</div>
"""
st.markdown(footer, unsafe_allow_html=True)
