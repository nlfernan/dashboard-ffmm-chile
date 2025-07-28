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
# 🔑 Conexión a OpenAI usando variable de entorno
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
st.markdown("<h1>Dashboard Fondos Mutuos</h1>", unsafe_allow_html=True)

# -------------------------------
# Rango de fechas
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
# Tabs
# -------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Patrimonio Neto Total",
    "Venta Neta Acumulada",
    "Listado Fondos",
    "💡 Insight IA"
])

# --- Tab 4: Insight IA ---
with tab4:
    st.subheader("💡 Insight IA basado en Top 20 Fondos")

    top_fondos = (
        df.groupby(["run_fm", "nombre_corto", "nom_adm"])["venta_neta_mm"]
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .reset_index()
    )

    # Formatear números con miles
    top_fondos["venta_neta_mm"] = top_fondos["venta_neta_mm"].apply(lambda x: f"{x:,.0f}".replace(",", "."))

    contexto = top_fondos.to_string(index=False)

    # Botón Insight IA
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

    # Tabla Top 20 al final en expander
    with st.expander("📊 Ver Top 20 Fondos Mutuos"):
        st.dataframe(top_fondos.rename(columns={
            "run_fm": "RUT",
            "nombre_corto": "Nombre del Fondo",
            "nom_adm": "Administradora",
            "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
        }), use_container_width=True)
