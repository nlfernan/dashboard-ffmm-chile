# -*- coding: utf-8 -*-
import streamlit as st

st.title("📅 Venta Neta Diaria (MM CLP)")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)

if df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# ===============================
# 📈 Venta neta diaria (sin acumulación)
# ===============================
venta_neta_diaria = (
    df.groupby(df["fecha_inf_date"].dt.date)["venta_neta_mm"]
    .sum()
    .sort_index()
)
st.bar_chart(venta_neta_diaria, height=300, use_container_width=True)

# ===============================
# 📊 Aportes y rescates diarios ocultos
# ===============================
with st.expander("📊 Ver Aportes y Rescates diarios", expanded=False):
    st.markdown("#### Evolución diaria de Aportes (en millones de CLP)")
    aportes_diarios = (
        df.groupby(df["fecha_inf_date"].dt.date)["aportes_mm"]
        .sum()
        .sort_index()
    )
    st.bar_chart(aportes_diarios, height=250, use_container_width=True)

    st.markdown("#### Evolución diaria de Rescates (en millones de CLP)")
    rescates_diarios = (
        df.groupby(df["fecha_inf_date"].dt.date)["rescates_mm"]
        .sum()
        .sort_index()
    )
    st.bar_chart(rescates_diarios, height=250, use_container_width=True)
