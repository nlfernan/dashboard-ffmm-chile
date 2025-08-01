import streamlit as st
import pandas as pd
import altair as alt

if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title('📈 Patrimonio Neto Total (MM CLP)')

df = st.session_state.get("df_filtrado", st.session_state.df)

@st.cache_data
def calcular_patrimonio(df: pd.DataFrame):
    return df.groupby("fecha_dia")["patrimonio_neto_mm"].sum().sort_index().reset_index()

patrimonio_total = calcular_patrimonio(df)
patrimonio_total["patrimonio_neto_mm"] = patrimonio_total["patrimonio_neto_mm"].round(0)

# ✅ Formato chileno para tooltip
def formato_chileno(x):
    return f"{x:,.0f}".replace(",", ".")  # separador de miles con punto

patrimonio_total["tooltip_valor"] = patrimonio_total["patrimonio_neto_mm"].apply(formato_chileno)

# ✅ Crear gráfico con Altair
chart = alt.Chart(patrimonio_total).mark_bar(color="#0066cc").encode(
    x="fecha_dia:T",
    y="patrimonio_neto_mm:Q",
    tooltip=[
        alt.Tooltip("fecha_dia:T", title="Fecha"),
        alt.Tooltip("tooltip_valor:N", title="Patrimonio Neto (MM CLP)")
    ]
).properties(
    height=300,
    width="container"
)

st.altair_chart(chart, use_container_width=True)



