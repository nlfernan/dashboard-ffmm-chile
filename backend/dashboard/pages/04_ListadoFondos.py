
import streamlit as st

st.title("📜 Listado de Fondos Mutuos")

df = st.session_state.df_filtrado if "df_filtrado" in st.session_state else st.session_state.df
st.dataframe(df.head(50))
