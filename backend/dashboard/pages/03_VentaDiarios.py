import streamlit as st

st.header("Venta / Aportes / Rescates Diarios")

if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos filtrados, volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.write(df.head())
