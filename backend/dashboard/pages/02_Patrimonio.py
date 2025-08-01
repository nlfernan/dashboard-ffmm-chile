import streamlit as st

# 🚦 Evitar que se ejecute si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title('📈 Patrimonio Neto Total (MM CLP)')

# Tomar DataFrame filtrado si existe, sino el completo
df = st.session_state.get("df_filtrado", st.session_state.df)

# Agrupar y graficar
patrimonio_total = df.groupby(df["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum().sort_index()
st.bar_chart(patrimonio_total, height=300, use_container_width=True)
