import streamlit as st
from openai import OpenAI

st.title('💡 Insight IA')


df = st.session_state.get("df_filtrado", st.session_state.df)

top_fondos = df.groupby(["run_fm", "nombre_corto", "nom_adm"])["venta_neta_mm"].sum().sort_values(ascending=False).head(20).reset_index()
st.dataframe(top_fondos)

if st.button("🔍 Generar Insight IA"):
    client = OpenAI(api_key=st.secrets['OPENAI_API_KEY'])
    contexto = top_fondos.to_string(index=False)
    prompt = f"Analiza el top 20 de fondos:
{contexto}"
    st.write("➡️ (Aquí se llama a GPT-4o-mini con el prompt)")
