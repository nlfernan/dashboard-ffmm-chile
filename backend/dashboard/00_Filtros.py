import streamlit as st
if not st.session_state.get("logueado", False):
    st.warning("Debes iniciar sesión primero en la página principal.")
    st.stop()


# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import os, unicodedata, calendar
from datetime import date, timedelta

# 🔐 Login
USER = os.getenv("DASHBOARD_USER")
PASS = os.getenv("DASHBOARD_PASS")
if "logueado" not in st.session_state:
    st.session_state.logueado = False
if "requiere_login" not in st.session_state:
    import random
    st.session_state.requiere_login = random.randint(1, 3) == 1

if st.session_state.requiere_login and not st.session_state.logueado:
    st.title("🔐 Acceso al Dashboard")
    usuario = st.text_input("Usuario")
    clave = st.text_input("Contraseña", type="password")
    if st.button("Ingresar"):
        if usuario == USER and clave == PASS:
            st.session_state.logueado = True
            st.success("✅ Acceso concedido. Cargando dashboard...")
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos")
    st.stop()

# 📂 Cargar parquet
PARQUET_PATH = "/app/data_fuentes/ffmm_merged.parquet"
def limpiar_nombre(col):
    col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    col = ''.join(c if c.isalnum() else '_' for c in col)
    return col.lower()

@st.cache_data
def cargar_datos():
    df = pd.read_parquet(PARQUET_PATH)
    df.columns = [limpiar_nombre(c) for c in df.columns]
    return df

df = cargar_datos()
df["fecha_inf_date"] = pd.to_datetime(df["fecha_inf_date"])

# 🗂 Mapping AFM -> Categoría agregada
mapping = {
    "Accionario America Latina":"Accionario Internacional",
    "Accionario Asia Emergente":"Accionario Internacional",
    "Accionario Brasil":"Accionario Internacional",
    "Accionario Desarrollado":"Accionario Internacional",
    "Accionario EEUU":"Accionario Internacional",
    "Accionario Emergente":"Accionario Internacional",
    "Accionario Europa Desarrollado":"Accionario Internacional",
    "Accionario Europa Emergente":"Accionario Internacional",
    "Accionario Nacional Large CAP":"Accionario Nacional",
    "Accionario Nacional Otros":"Accionario Nacional",
    "Accionario Pais":"Accionario Internacional",
    "Accionario Países MILA":"Accionario Internacional",
    "Accionario Sectorial":"Accionario Otros",
    "Balanceado Agresivo":"Balanceado",
    "Balanceado Conservador":"Balanceado",
    "Balanceado Moderado":"Balanceado",
    "Estructurado Accionario Desarrollado":"Estructurado",
    "Estructurado No Accionario":"Estructurado",
    "Fondos de Deuda < 365 Dias Internacional":"Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Nacional en pesos":"Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Nacional en UF":"Deuda Mediano Plazo",
    "Fondos de Deuda < 365 Dias Orig. Flex":"Deuda Mediano Plazo",
    "Fondos de Deuda < 90 Dias Internacional Dolar":"Deuda Corto Plazo",
    "Fondos de Deuda < 90 Dias Nacional":"Deuda Corto Plazo",
    "Fondos de Deuda > 365 Dias Internacional Mercados Emergentes":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Internacional Mercados Internacionales":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversión en Pesos":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversión en UF < 3 años":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversion en UF > 5 años":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Nacional Inversion UF > 3 años y =<5":"Deuda Largo Plazo",
    "Fondos de Deuda > 365 Dias Orig. Flex":"Deuda Largo Plazo",
    "Inversionistas Calificados Accionario Internacional":"Accionario Internacional",
    "Inversionistas Calificados Accionario Nacional":"Accionario Nacional",
    "Inversionistas Calificados Títulos de Deuda":"Deuda Otros",
    "S/C Fondos creados recientemente que aún no han sido clasificados":"Otros",
    "S/C Fondos que han variado su política efectiva de inversión durante el período de comparación":"Otros"
}

if "categoria" in df.columns:
    df["categoria_agregada"] = df["categoria"].map(mapping).fillna("Otros")

if "nombre_corto" in df.columns and "run_fm" in df.columns:
    df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

st.title("Principal - Filtros")

# 📅 Filtros de fechas
fechas_unicas = sorted(df["fecha_inf_date"].dt.date.unique())
fecha_min_real = fechas_unicas[0]
fecha_max_real = fechas_unicas[-1]

años_disponibles = sorted(df["fecha_inf_date"].dt.year.unique())
meses_disponibles = list(calendar.month_name)[1:]

col1, col2 = st.columns(2)
col3, col4 = st.columns(2)

año_inicio = col1.selectbox("Año inicio", años_disponibles, index=0)
mes_inicio = col2.selectbox("Mes inicio", meses_disponibles, index=0)
año_fin = col3.selectbox("Año fin", años_disponibles, index=len(años_disponibles)-1)
mes_fin = col4.selectbox("Mes fin", meses_disponibles, index=len(meses_disponibles)-1)

fecha_inicio = date(año_inicio, meses_disponibles.index(mes_inicio)+1, 1)
ultimo_dia_mes_fin = calendar.monthrange(año_fin, meses_disponibles.index(mes_fin)+1)[1]
fecha_fin = date(año_fin, meses_disponibles.index(mes_fin)+1, ultimo_dia_mes_fin)

df = df[(df["fecha_inf_date"].dt.date >= fecha_inicio) & (df["fecha_inf_date"].dt.date <= fecha_fin)]

# ✅ Función multiselect
def multiselect_con_todo(label, opciones):
    opciones_mostradas = ["(Seleccionar todo)"] + list(opciones)
    seleccion = st.multiselect(label, opciones_mostradas, default=["(Seleccionar todo)"])
    if "(Seleccionar todo)" in seleccion or not seleccion:
        return list(opciones)
    else:
        return seleccion

# ✅ Filtros principales
categoria_agregada_sel = multiselect_con_todo("Categoría agregada", sorted(df["categoria_agregada"].dropna().unique()))
df = df[df["categoria_agregada"].isin(categoria_agregada_sel)]

categoria_sel = multiselect_con_todo("Categoría", sorted(df["categoria"].dropna().unique()))
df = df[df["categoria"].isin(categoria_sel)]

adm_sel = multiselect_con_todo("Administradora(s)", sorted(df["nom_adm"].dropna().unique()))
df = df[df["nom_adm"].isin(adm_sel)]

fondo_sel = multiselect_con_todo("Fondo(s)", sorted(df["run_fm_nombrecorto"].dropna().unique()))
df = df[df["run_fm_nombrecorto"].isin(fondo_sel)]

# ✅ Filtros adicionales
with st.expander("🔧 Filtros adicionales"):
    tipo_sel = multiselect_con_todo("Tipo de Fondo", sorted(df["tipo_fm"].dropna().unique()))
    df = df[df["tipo_fm"].isin(tipo_sel)]
    serie_sel = multiselect_con_todo("Serie(s)", sorted(df["serie"].dropna().unique()))
    df = df[df["serie"].isin(serie_sel)]

    if "rango_fechas" not in st.session_state:
        st.session_state["rango_fechas"] = (fecha_inicio, fecha_fin)

    st.session_state["rango_fechas"] = st.slider(
        "Rango exacto",
        min_value=fecha_min_real,
        max_value=fecha_max_real,
        value=st.session_state["rango_fechas"],
        format="DD-MM-YYYY"
    )

    hoy_df = fecha_max_real
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    if col_a.button("1M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=30), fecha_min_real), hoy_df)
    if col_b.button("3M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=90), fecha_min_real), hoy_df)
    if col_c.button("6M"):
        st.session_state["rango_fechas"] = (max(hoy_df - timedelta(days=180), fecha_min_real), hoy_df)
    if col_d.button("MTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, hoy_df.month, 1), hoy_df)
    if col_e.button("YTD"):
        st.session_state["rango_fechas"] = (date(hoy_df.year, 1, 1), hoy_df)

# Guardar DF filtrado
st.session_state["df_filtrado"] = df

st.success(f"Datos filtrados: {df.shape[0]} filas")

# ✅ Footer HTML
st.markdown("<br><br><br><br>", unsafe_allow_html=True)
footer = '''
<style>
.footer {
    position: fixed;
    left: 0;
    bottom: 0;
    width: 100%;
    background-color: #f0f2f6;
    color: #333;
    text-align: center;
    font-size: 12px;
    padding: 10px;
    border-top: 1px solid #ccc;
    z-index: 999;
}
</style>
<div class="footer">
    Autor: Nicolás Fernández Ponce, CFA | Dashboard Fondos Mutuos Chile · Datos CMF
</div>
'''
st.markdown(footer, unsafe_allow_html=True)
