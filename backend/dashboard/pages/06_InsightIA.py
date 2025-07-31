import streamlit as st
from openai import OpenAI, RateLimitError
if "df_filtrado" not in st.session_state:
    st.warning("Volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("💡 Insight IA")
    top = df.groupby(["run_fm","nombre_corto","nom_adm"])["venta_neta_mm"].sum().sort_values(ascending=False).head(20).reset_index()
    contexto = top.to_string(index=False)
    if st.button("Generar Insight IA"):
        try:
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            prompt = f"Analiza el top 20 de fondos:
{contexto}"
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"system","content":"Eres un analista financiero en Chile."},{"role":"user","content":prompt}]
            )
            st.success(resp.choices[0].message.content)
        except RateLimitError:
            st.error("No hay crédito OpenAI.")
    if "chat_historial" not in st.session_state:
        st.session_state.chat_historial = []
    pregunta = st.chat_input("Pregunta sobre los fondos")
    if pregunta:
        st.session_state.chat_historial.append({"role":"user","content":pregunta})
        st.write(pregunta)
