import streamlit as st
if "df_filtrado" not in st.session_state:
    st.warning("Volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Venta Neta Acumulada")
    venta = df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum()
    st.bar_chart(venta)
