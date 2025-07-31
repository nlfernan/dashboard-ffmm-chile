
import streamlit as st

if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos. Volvé a Principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Venta Neta Acumulada (MM CLP)")
    venta = df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum()
    st.bar_chart(venta)
    st.subheader("Aportes Acumulados (MM CLP)")
    aportes = df.groupby(df["fecha_inf_date"].dt.date)["aportes_mm"].sum().cumsum()
    st.bar_chart(aportes)
    st.subheader("Rescates Acumulados (MM CLP)")
    rescates = df.groupby(df["fecha_inf_date"].dt.date)["rescates_mm"].sum().cumsum()
    st.bar_chart(rescates)
