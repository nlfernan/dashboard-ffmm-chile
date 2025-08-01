# -*- coding: utf-8 -*-
import streamlit as st

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("💵 Venta Neta Acumulada (MM CLP)")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)

if df.empty:
    st.warning("⚠️ No hay datos disponibles con los filtros actuales.")
    st.stop()

# ===============================
# 📈 Venta neta acumulada
# ===============================
venta_neta_acumulada = (
    df.groupby("fecha_dia")["venta_neta_mm"]
    .sum()
    .cumsum()
    .sort_index()
)
st.bar_chart(venta_neta_acumulada, height=300, use_container_width=True)

# ===============================
# 📊 Aportes y rescates ocultos
# ===============================
with st.expander("📊 Ver Aportes y Rescates acumulados", expanded=False):
    st.markdown("#### Evolución acumulada de Aportes (en millones de CLP)")
    aportes_acumulados = (
        df.groupby("fecha_dia")["aportes_mm"]
        .sum()
        .cumsum()
        .sort_index()
    )
    st.bar_chart(aportes_acumulados, height=250, use_container_width=True)

    st.markdown("#### Evolución acumulada de Rescates (en millones de CLP)")
    rescates_acumulados = (
        df.groupby("fecha_dia")["rescates_mm"]
        .sum()
        .cumsum()
        .sort_index()
    )
    st.bar_chart(rescates_acumulados, height=250, use_container_width=True)

