
import streamlit as st

if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos. Volvé a Principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Venta / Aportes / Rescates Diarios (MM CLP)")
    diarios = df.groupby(df["fecha_inf_date"].dt.date).agg({
        "venta_neta_mm":"sum","aportes_mm":"sum","rescates_mm":"sum"
    }).sort_index()
    st.line_chart(diarios)
