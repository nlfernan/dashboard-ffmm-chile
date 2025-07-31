
import streamlit as st
if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos, volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Patrimonio Neto Total")
    patrimonio_total = df.groupby(df["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum()
    st.bar_chart(patrimonio_total)
