import streamlit as st
import pandas as pd

st.title('📜 Listado de Fondos Mutuos')


df = st.session_state.get("df_filtrado", st.session_state.df)

ranking = df.groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"].sum().sort_values(by="venta_neta_mm", ascending=False).head(20)
st.dataframe(ranking)
