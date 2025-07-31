
import streamlit as st
if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos, volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Venta Neta Acumulada")
    venta_neta_acumulada = df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum()
    st.bar_chart(venta_neta_acumulada)
    st.subheader("Aportes Acumulados")
    aportes = df.groupby(df["fecha_inf_date"].dt.date)["aportes_mm"].sum().cumsum()
    st.bar_chart(aportes)
    st.subheader("Rescates Acumulados")
    rescates = df.groupby(df["fecha_inf_date"].dt.date)["rescates_mm"].sum().cumsum()
    st.bar_chart(rescates)
