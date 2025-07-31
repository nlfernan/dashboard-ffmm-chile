
import streamlit as st, altair as alt
if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos, volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Venta/Aportes/Rescates Diarios")
    diarios = df.groupby(df["fecha_inf_date"].dt.date).agg({
        "venta_neta_mm": "sum",
        "aportes_mm": "sum",
        "rescates_mm": "sum"
    }).reset_index().rename(columns={"fecha_inf_date": "Fecha"})
    st.altair_chart(alt.Chart(diarios).mark_bar(color='#1f77b4').encode(x='Fecha:T', y='venta_neta_mm:Q'), use_container_width=True)
    st.altair_chart(alt.Chart(diarios).mark_bar(color='green').encode(x='Fecha:T', y='aportes_mm:Q'), use_container_width=True)
    st.altair_chart(alt.Chart(diarios).mark_bar(color='red').encode(x='Fecha:T', y='rescates_mm:Q'), use_container_width=True)
