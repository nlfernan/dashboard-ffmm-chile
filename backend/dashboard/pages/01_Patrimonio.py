
import streamlit as st

if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos. Volvé a Principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Evolución del Patrimonio Neto Total (MM CLP)")
    patrimonio = df.groupby(df["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum().sort_index()
    st.bar_chart(patrimonio)
