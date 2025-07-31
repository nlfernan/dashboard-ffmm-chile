
import streamlit as st
import pandas as pd

if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos. Volvé a Principal.")
else:
    df = st.session_state["df_filtrado"]
    st.header("Listado Top 20 Fondos (Venta Neta)")
    ranking = df.groupby(["run_fm","nombre_corto","nom_adm"], as_index=False)["venta_neta_mm"].sum()
    ranking = ranking.sort_values(by="venta_neta_mm", ascending=False).head(20).copy()

    def url_cmf(rut):
        return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI"
    ranking["URL CMF"] = ranking["run_fm"].astype(str).apply(url_cmf)

    st.dataframe(ranking)

    @st.cache_data
    def generar_csv(df):
        return df.to_csv(index=False).encode("utf-8-sig")

    csv_data = generar_csv(df)
    st.download_button("⬇️ Descargar CSV filtrado", data=csv_data, file_name="ffmm_filtrado.csv", mime="text/csv")
