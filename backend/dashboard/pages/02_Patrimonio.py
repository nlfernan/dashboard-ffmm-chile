import streamlit as st

st.title('📈 Patrimonio Neto Total (MM CLP)')


df = st.session_state.get("df_filtrado", st.session_state.df)
patrimonio_total = df.groupby(df["fecha_inf_date"].dt.date)["patrimonio_neto_mm"].sum().sort_index()
st.bar_chart(patrimonio_total, height=300, use_container_width=True)
