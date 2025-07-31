
import streamlit as st

st.title("💡 Insight IA")

df = st.session_state.df_filtrado if "df_filtrado" in st.session_state else st.session_state.df
top_fondos = df.groupby(["run_fm", "nombre_corto", "nom_adm"])["venta_neta_mm"].sum().sort_values(ascending=False).head(20)
st.dataframe(top_fondos)
