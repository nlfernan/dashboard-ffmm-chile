
import streamlit as st

st.title("📈 Patrimonio Neto Total")

df = st.session_state.df_filtrado if "df_filtrado" in st.session_state else st.session_state.df
patrimonio_total = df.groupby(df["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum().sort_index()
st.bar_chart(patrimonio_total, height=300, use_container_width=True)
