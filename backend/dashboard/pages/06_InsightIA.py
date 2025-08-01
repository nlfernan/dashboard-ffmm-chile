# -*- coding: utf-8 -*-
import streamlit as st
from openai import OpenAI, RateLimitError
import os

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("💡 Insight IA")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)

# ===============================
# 📌 Top 20 fondos
# ===============================
top_fondos = (
    df.groupby(["run_fm", "nombre_corto", "nom_adm"])["venta_neta_mm"]
    .sum()
    .sort_values(ascending=False)
    .head(20)
    .reset_index()
)

contexto = top_fondos.to_string(index=False)

# ===============================
# 🔑 API Key híbrida (local o Railway)
# ===============================
try:
    OPENAI_KEY = st.secrets["OPENAI_API_KEY"]  # Local con secrets.toml
except Exception:
    OPENAI_KEY = os.getenv("OPENAI_API_KEY")   # Producción con variable de entorno

if not OPENAI_KEY:
    st.error("❌ No se encontró OPENAI_API_KEY en secrets.toml ni en variables de entorno.")
    st.stop()

client = OpenAI(api_key=OPENAI_KEY)

# ===============================
# 🔍 Generar insight automático
# ===============================
if st.button("Generar Insight IA"):
    try:
        prompt = f"""
        Analiza el top 20 de fondos mutuos basado en venta neta acumulada.
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

# ===============================
# 💬 Chat con IA
# ===============================
st.markdown("### 💬 Chat con IA sobre el Top 20")

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
        prompt_chat = f"Usa estos datos de contexto:\n{contexto}\n\nPregunta: {pregunta}"
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

# ===============================
# 📊 Expandible abajo del chat
# ===============================
with st.expander("📊 Ver Top 20 Fondos Mutuos", expanded=False):
    st.dataframe(top_fondos.rename(columns={
        "run_fm": "RUT",
        "nombre_corto": "Nombre del Fondo",
        "nom_adm": "Administradora",
        "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
    }), use_container_width=True)
