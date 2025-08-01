import streamlit as st
import pandas as pd

if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title('📈 Patrimonio Neto Total (MM CLP)')

df = st.session_state.get("df_filtrado", st.session_state.df)

@st.cache_data
def calcular_patrimonio(df: pd.DataFrame):
    return df.groupby("fecha_dia")["patrimonio_neto_mm"].sum().sort_index()

patrimonio_total = calcular_patrimonio(df)

# ✅ Formatear: redondear y mostrar en millones con separador de miles
patrimonio_total = patrimonio_total.round(0)

# Mostrar como dataframe con formato bonito
st.dataframe(
    patrimonio_total.apply(lambda x: f"{x:,.0f}"), 
    use_container_width=True
)

# Graficar
st.bar_chart(patrimonio_total, height=300, use_container_width=True)
