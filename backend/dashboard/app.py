
import streamlit as st
import os

USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")

st.set_page_config(page_title="Dashboard FFMM", page_icon="📊", layout="wide")

if "logueado" not in st.session_state:
    st.session_state.logueado = False

if not st.session_state.logueado:
    st.title("🔐 Acceso al Dashboard")
    usuario = st.text_input("Usuario")
    clave = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if usuario == USER and clave == PASS:
            st.session_state.logueado = True
            st.success("✅ Acceso concedido. Redirigiendo a filtros...")
            st.switch_page("pages/00_Filtros.py")
        else:
            st.error("Usuario o contraseña incorrectos")
    st.stop()
else:
    st.success("Ya has iniciado sesión. Ve a la pestaña 'Filtros'.")
