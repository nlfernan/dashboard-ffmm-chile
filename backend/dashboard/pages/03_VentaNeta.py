import streamlit as st

st.title('💵 Venta Neta Acumulada (MM CLP)')


df = st.session_state.get("df_filtrado", st.session_state.df)

venta_neta_acumulada = df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"].sum().cumsum().sort_index()
st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

st.subheader("📊 Aportes y Rescates acumulados")
aportes_acumulados = df.groupby(df["fecha_inf_date"].dt.date)["aportes_mm"].sum().cumsum().sort_index()
rescates_acumulados = df.groupby(df["fecha_inf_date"].dt.date)["rescates_mm"].sum().cumsum().sort_index()

st.bar_chart(aportes_acumulados, height=250, use_container_width=True)
st.bar_chart(rescates_acumulados, height=250, use_container_width=True)
