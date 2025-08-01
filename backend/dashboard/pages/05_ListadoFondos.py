# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("📜 Listado de Fondos Mutuos")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)

if df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# ===============================
# 📊 Ranking por venta neta (cacheado)
# ===============================
@st.cache_data
def calcular_ranking(df_filtrado: pd.DataFrame):
    print("🔄 Recalculando ranking de fondos...")  # Debug log
    columnas = ["run_fm", "nombre_corto", "nom_adm", "venta_neta_mm"]
    df_reducido = df_filtrado[columnas]

    ranking = (
        df_reducido.groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
    )
    return ranking

ranking = calcular_ranking(df)

# Determinar si mostrar top 20 o todo
total_fondos = ranking.shape[0]
if total_fondos > 20:
    ranking = ranking.nlargest(20, "venta_neta_mm")
    titulo = f"Top 20 Fondos por Venta Neta de {total_fondos} totales"
else:
    titulo = f"Listado de Fondos Mutuos (total: {total_fondos})"

st.subheader(titulo)

# ===============================
# 🌐 Agregar URL CMF
# ===============================
def generar_url_cmf(rut):
    return f"https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={rut}&tipoentidad=RGFMU&vig=VI"

ranking["URL CMF"] = ranking["run_fm"].astype(str).apply(generar_url_cmf)

# Formatear columnas
ranking = ranking.rename(columns={
    "run_fm": "RUT",
    "nombre_corto": "Nombre del Fondo",
    "nom_adm": "Administradora",
    "venta_neta_mm": "Venta Neta (MM CLP)"
})

ranking["Venta Neta (MM CLP)"] = ranking["Venta Neta (MM CLP)"].apply(lambda x: f"{x:,.0f}".replace(",", "."))

# Convertir URL a link HTML
ranking["URL CMF"] = ranking["URL CMF"].apply(lambda x: f'<a href="{x}" target="_blank">Ver en CMF</a>')

# ===============================
# 🖥️ Mostrar tabla como HTML
# ===============================
st.markdown(ranking.to_html(index=False, escape=False), unsafe_allow_html=True)

# ===============================
# 📥 Descargar CSV
# ===============================
MAX_FILAS = 50_000
st.markdown("### ⬇️ Descargar datos filtrados")

st.caption(f"🔢 Total de filas disponibles: {df.shape[0]:,}")

if df.shape[0] > MAX_FILAS:
    st.warning(f"⚠️ La descarga está limitada a {MAX_FILAS:,} filas. Aplica más filtros para reducir el tamaño (actual: {df.shape[0]:,} filas).")
else:
    @st.cache_data(hash_funcs={pd.DataFrame: lambda _: None})
    def generar_csv(df):
        return df.to_csv(index=False).encode("utf-8-sig")

    csv_data = generar_csv(df)
    st.download_button(
        label="⬇️ Descargar CSV",
        data=csv_data,
        file_name="ffmm_filtrado.csv",
        mime="text/csv"
    )
