
import streamlit as st

st.title("💵 Venta Neta Acumulada")

df = st.session_state.df_filtrado if "df_filtrado" in st.session_state else st.session_state.df
venta_neta_acumulada = df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum().sort_index()
st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)
