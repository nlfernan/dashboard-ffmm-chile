# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os
import calendar
import random
from datetime import date, timedelta
from sqlalchemy import create_engine
from openai import OpenAI, RateLimitError

# -------------------------------
# 🔐 Login aleatorio 1 de cada 3 sesiones
# -------------------------------
USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")

if "requiere_login" not in st.session_state:
    st.session_state.requiere_login = random.randint(1, 3) == 1  # 1 de 3 sesiones pide login

if st.session_state.requiere_login and not st.session_state.get("logueado", False):
    st.sidebar.header("🔐 Login")
    usuario = st.sidebar.text_input("Usuario")
    clave = st.sidebar.text_input("Contraseña", type="password")

    if usuario == USER and clave == PASS:
        st.session_state.logueado = True
        st.success("✅ Acceso concedido")
    else:
        st.warning("Ingresá tus credenciales para continuar")
        st.stop()

# -------------------------------
# 🔑 Conexión a OpenAI
# -------------------------------
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    st.error("❌ No se encontró la variable OPENAI_API_KEY. Configurala en Railway > Environments.")
    st.stop()

client = OpenAI(api_key=OPENAI_KEY)

# -------------------------------
# Conexión a PostgreSQL
# -------------------------------
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    st.error("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")
    st.stop()

engine = create_engine(DB_URL)

# -------------------------------
# Cargar datos
# -------------------------------
@st.cache_data
def cargar_datos_db():
    query = """
    SELECT 
        fecha_inf_date, run_fm, nombre_corto, nom_adm, serie,
        patrimonio_neto_mm, venta_neta_mm, tipo_fm, categoria
    FROM fondos_mutuos;
    """
    return pd.read_sql(query, engine)

df = cargar_datos_db()
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
# Filtro de fechas con Mes/Año
# -------------------------------
fechas_disponibles = df["fecha_inf_date"].dropna()
fecha_min_real = fechas_disponibles.min().date()
fecha_max_real = fechas_disponibles.max().date()

rango_fechas = st.slider(
    "Seleccioná rango de fechas",
    min_value=fecha_min_real,
    max_value=fecha_max_real,
    value=(fecha_min_real, fecha_max_real),
    format="DD-MM-YYYY"
)

df = df[(df["fecha_inf_date"].dt.date >= rango_fechas[0]) &
        (df["fecha_inf_date"].dt.date <= rango_fechas[1])]

# -------------------------------
# Filtros principales
# -------------------------------
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion or not seleccion:
        return list(opciones)
    else:
        return seleccion

categoria_opciones = sorted(df["categoria"].dropna().unique())
categoria_seleccionadas = multiselect_con_todo("Categoría", categoria_opciones)

adm_opciones = sorted(df[df["categoria"].isin(categoria_seleccionadas)]["nom_adm"].dropna().unique())
adm_seleccionadas = multiselect_con_todo("Administradora(s)", adm_opciones)

fondo_opciones = sorted(df[df["nom_adm"].isin(adm_seleccionadas)]["run_fm_nombrecorto"].dropna().unique())
fondo_seleccionados = multiselect_con_todo("Fondo(s)", fondo_opciones)

with st.expander("🔧 Filtros adicionales"):
    tipo_opciones = sorted(df["tipo_fm"].dropna().unique())
    tipo_seleccionados = multiselect_con_todo("Tipo de Fondo", tipo_opciones)

    serie_opciones = sorted(df[df["run_fm_nombrecorto"].isin(fondo_seleccionados)]["serie"].dropna().unique())
    serie_seleccionadas = multiselect_con_todo("Serie(s)", serie_opciones)
else:
    tipo_seleccionados = df["tipo_fm"].dropna().unique()
    serie_seleccionadas = df["serie"].dropna().unique()

# -------------------------------
# Aplicar filtros
# -------------------------------
df_filtrado = df[df["tipo_fm"].isin(tipo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["categoria"].isin(categoria_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["nom_adm"].isin(adm_seleccionadas)]
df_filtrado = df_filtrado[df_filtrado["run_fm_nombrecorto"].isin(fondo_seleccionados)]
df_filtrado = df_filtrado[df_filtrado["serie"].isin(serie_seleccionadas)]

# -------------------------------
# Tabs
# -------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Patrimonio Neto Total",
    "Venta Neta Acumulada",
    "Listado Fondos",
    "💡 Insight IA"
])

with tab1:
    st.subheader("Evolución del Patrimonio Neto Total (en millones de CLP)")
    patrimonio_total = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["patrimonio_neto_mm"]
        .sum()
        .sort_index()
    )
    st.bar_chart(patrimonio_total, height=300, use_container_width=True)

with tab2:
    st.subheader("Evolución acumulada de la Venta Neta (en millones de CLP)")
    venta_neta_acumulada = (
        df_filtrado.groupby(df_filtrado["fecha_inf_date"].dt.date)["venta_neta_mm"]
        .sum()
        .cumsum()
        .sort_index()
    )
    st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

with tab3:
    ranking_ventas = (
        df_filtrado
        .groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
        .head(20)
        .copy()
    )

    st.subheader("Listado de Fondos Mutuos (Top 20 por Venta Neta)")
    st.dataframe(ranking_ventas)

    # Descargar CSV limitado a 50.000
    st.markdown("### Descargar datos filtrados")
    MAX_FILAS = 50_000
    st.caption(f"🔢 Total de filas: {df_filtrado.shape[0]:,}")

    if df_filtrado.shape[0] > MAX_FILAS:
        st.warning(f"⚠️ La descarga está limitada a {MAX_FILAS:,} filas. Aplicá más filtros para reducir el tamaño (actual: {df_filtrado.shape[0]:,} filas).")
    else:
        @st.cache_data
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

    # Chat IA
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

    # Tabla Top 20 al final
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
