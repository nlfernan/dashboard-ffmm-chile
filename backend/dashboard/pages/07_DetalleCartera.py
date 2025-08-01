# -*- coding: utf-8 -*-
import streamlit as st

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("📑 Detalle Cartera")

st.info("🔧 Esta sección estará disponible próximamente. Aquí podrás ver la composición detallada de la cartera.")
