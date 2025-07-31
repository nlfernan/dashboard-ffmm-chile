import streamlit as st
if "df_filtrado" not in st.session_state:
    st.warning("Volvé a la página principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Listado de Fondos Mutuos")
    ranking = df.groupby(["run_fm","nombre_corto","nom_adm"], as_index=False)["venta_neta_mm"].sum().sort_values(by="venta_neta_mm", ascending=False).head(20)
    st.dataframe(ranking)
