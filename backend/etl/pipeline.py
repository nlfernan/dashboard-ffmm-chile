# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
import random
import unicodedata
from datetime import date, timedelta
from openai import OpenAI, RateLimitError
import altair as alt

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
# 📂 Leer Parquet y normalizar columnas
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
if "fecha_inf_date" not in df.columns and "fecha_inf" in df.columns:
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf"])
else:
    df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])

df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# -------------------------------
# Mapeo Categoría AFM → Categoría agregada
# -------------------------------
mapeo_categorias = {
    "Accionario America Latina": "Accionario Internacional",
    "Accionario Asia Emergente": "Accionario Internacional",
    "Accionario Brasil": "Accionario Internacional",
    "Accionario Desarrollado": "Accionario Internacional",
    "Accionario EEUU": "Accionario Internacional",
    "Accionario Emergente": "Accionario Internacional",
    "Accionario Europa Desarrollado": "Accionario Internacional",
    "Accionario Europa Emergente": "Accionario Internacional",
    "Accionario Nacional Large CAP": "Accionario Nacional",
    "Accionario Nacional Otros": "Accionario Nacional",
    "Accionario Pais": "Accionario Internacional",
    "Accionario Países MILA": "Accionario Internacional",
    "Accionario Sectorial": "Accionario Otros",
    "Balanceado Agresivo": "Balanceado",
    "Balanceado Conservador": "Balanceado",
    "Balanceado Moderado": "Balanceado",
    "Estructurado Accionario Desarrollado": "Estructurado",
    "Estructurado No Accionario": "Estructurado",
    "Fondos de Deuda < 365 Dias Internacional": "Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Nacional en pesos": "Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Nacional en UF": "Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Orig. Flex": "Deuda Mediano Plazo",
    "Fondos de Deuda < 90 Dias Internacional Dolar": "Deuda Corto Plazo",
    "Fondos de Deuda < 90 Dias Nacional": "Deuda Corto Plazo",
    "Fondos de Deuda > 365 Dias Internacional Mercados Emergentes": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Internacional Mercados Internacionales": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversión en Pesos": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversión en UF < 3 años": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversion en UF > 5 años": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversion UF > 3 años y =<5": "Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Orig. Flex": "Deuda Largo Plazo",
    "Inversionistas Calificados Accionario Internacional": "Accionario Internacional",
    "Inversionistas Calificados Accionario Nacional": "Accionario Nacional",
    "Inversionistas Calificados Títulos de Deuda": "Deuda Otros",
    "S/C Fondos creados recientemente que aún no han sido clasificados": "Otros",
    "S/C Fondos que han variado su política efectiva de inversión durante el período de comparación": "Otros",
}
df["categoria_agregada"] = df["categoria"].map(mapeo_categorias).fillna("Otros")

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

# -------------------------------
# Multiselect con "Seleccionar todo"
# -------------------------------
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion or not seleccion:
        return list(opciones)
    else:
        return seleccion

# -------------------------------
# Filtros principales
# -------------------------------
categoria_opciones = sorted(df["categoria_agregada"].dropna().unique())
categoria_seleccionadas = multiselect_con_todo("Categoría", categoria_opciones)

adm_opciones = sorted(df[df["categoria_agregada"].isin(categoria_seleccionadas)]["nom_adm"].dropna().unique())
adm_seleccionadas = multiselect_con_todo("Administradora(s)", adm_opciones)

fondo_opciones = sorted(df[df["nom_adm"].isin(adm_seleccionadas)]["run_fm_nombrecorto"].dropna().unique())
fondo_seleccionados = multiselect_con_todo("Fondo(s)", fondo_opciones)

# -------------------------------
# Filtrar DataFrame
# -------------------------------
df_filtrado = df[df["categoria_agregada"].isin(categoria_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["nom_adm"].isin(adm_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["run_fm_nombrecorto"].isin(fondo_seleccionados)]

if df_filtrado.empty:
    st.warning("No hay datos disponibles con los filtros seleccionados.")
    st.stop()

# -------------------------------
# Tabs
# -------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Patrimonio Neto Total (MM CLP)",
    "Venta Neta Acumulada (MM CLP)",
    "Venta / Aportes / Rescates Diarios",
    "Listado de Fondos Mutuos",
    "💡 Insight IA"
])

with tab1:
    st.subheader("Evolución del Patrimonio Neto Total (en millones de CLP)")
    patrimonio_total = df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum()
    st.bar_chart(patrimonio_total, height=300, use_container_width=True)

with tab2:
    st.subheader("Evolución acumulada de la Venta Neta (en millones de CLP)")
    venta_neta_acumulada = df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum()
    st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

with tab3:
    st.subheader("Venta neta / Aportes / Rescates Diarios")
    diarios = df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date).agg({
        "venta_neta_mm": "sum",
        "aportes_mm": "sum",
        "rescates_mm": "sum"
    }).reset_index().rename(columns={"fecha_inf_date": "Fecha"})

    chart_venta = alt.Chart(diarios).mark_bar(color="#1f77b4").encode(x="Fecha:T", y="venta_neta_mm:Q")
    chart_aportes = alt.Chart(diarios).mark_bar(color="green").encode(x="Fecha:T", y="aportes_mm:Q")
    chart_rescates = alt.Chart(diarios).mark_bar(color="red").encode(x="Fecha:T", y="rescates_mm:Q")

    st.markdown("### Venta Neta Diaria")
    st.altair_chart(chart_venta, use_container_width=True)
    st.markdown("### Aportes Diarios")
    st.altair_chart(chart_aportes, use_container_width=True)
    st.markdown("### Rescates Diarios")
    st.altair_chart(chart_rescates, use_container_width=True)

    @st.cache_data
    def generar_csv(df):
        return df.to_csv(index=False).encode("utf-8-sig")

    csv_diarios = generar_csv(diarios)
    st.download_button(
        label="⬇️ Descargar CSV (Diarios)",
        data=csv_diarios,
        file_name="venta_aportes_rescates_diarios.csv",
        mime="text/csv"
    )

with tab4:
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
        def generar_csv_full(df):
            return df.to_csv(index=False).encode("utf-8-sig")

        csv_data = generar_csv_full(df_filtrado)

        st.download_button(
            label="⬇️ Descargar CSV",
            data=csv_data,
            file_name="ffmm_filtrado.csv",
            mime="text/csv"
        )

with tab5:
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
