import streamlit as st
if not st.session_state.get("logueado", False):
    st.warning("Debes iniciar sesión primero en la página principal.")
    st.stop()
if "df_filtrado" not in st.session_state:
    st.warning("⚠️ No hay datos filtrados. Ve a la pestaña 'Filtros'.")
    st.stop()

df = st.session_state["df_filtrado"]

import streamlit as st
st.header('Detalle Carteras')
st.info('👷‍♂️ Hombres trabajando...')