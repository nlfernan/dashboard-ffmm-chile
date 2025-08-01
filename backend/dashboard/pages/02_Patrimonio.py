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
    data = df.groupby("fecha_dia")["patrimonio_neto_mm"].sum().sort_index().reset_index()
    data["fecha_dia"] = pd.to_datetime(data["fecha_dia"])  # ✅ asegurar tipo datetime
    return data

patrimonio_total = calcular_patrimonio(df)
patrimonio_total["patrimonio_neto_mm"] = patrimonio_total["patrimonio_neto_mm"].round(0)

# ✅ Formato chileno para tooltip
def formato_chileno(x):
    return f"{x:,.0f}".replace(",", ".")

patrimonio_total["tooltip_valor"] = patrimonio_total["patrimonio_neto_mm"].apply(formato_chileno)
patrimonio_total["fecha_label"] = patrimonio_total["fecha_dia"].dt.strftime("%d-%m-%Y")  # ✅ formato eje X

if patrimonio_total.empty:
    st.warning("⚠️ No hay datos para los filtros seleccionados.")
else:
    chart = alt.Chart(patrimonio_total).mark_bar(color="#0066cc").encode(
        x=alt.X("fecha_label:N", title=None),  # ✅ eje X como texto para mostrar DD-MM-YYYY
        y=alt.Y("patrimonio_neto_mm:Q",
                title=None,
                axis=alt.Axis(labelExpr="format(datum.value, ',').replace(',', '.')")),  # ✅ eje Y chileno
        tooltip=[
            alt.Tooltip("fecha_label:N", title="Fecha"),
            alt.Tooltip("tooltip_valor:N", title="Patrimonio Neto (MM CLP)")
        ]
    ).properties(
        height=300,
        width="container"
    )

    st.altair_chart(chart, use_container_width=True)


