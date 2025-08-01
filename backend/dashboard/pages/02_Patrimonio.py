import streamlit as st
import pandas as pd

# 🚦 Evitar que se ejecute si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title('📈 Patrimonio Neto Total (MM CLP)')

# Tomar DataFrame filtrado si existe, sino el completo
df = st.session_state.get("df_filtrado", st.session_state.df)

# ✅ Función cacheada para agrupar patrimonio
@st.cache_data
def calcular_patrimonio(df: pd.DataFrame):
    return df.groupby("fecha_dia")["patrimonio_neto_mm"].sum().sort_index()

patrimonio_total = calcular_patrimonio(df)

st.bar_chart(patrimonio_total, height=300, use_container_width=True)