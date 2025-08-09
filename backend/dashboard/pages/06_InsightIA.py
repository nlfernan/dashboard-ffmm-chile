# -*- coding: utf-8 -*-
import streamlit as st
from openai import OpenAI, RateLimitError, APIError
import os
import pandas as pd
import numpy as np
import re

# 🚦 Bloquear si los datos no están listos
if not st.session_state.get("datos_cargados", False):
    st.warning("⏳ Los datos aún se están cargando. Vuelve cuando termine de aplicar filtros.")
    st.stop()

st.title("💡 Insight IA")

# ===============================
# 📂 Tomar datos filtrados
# ===============================
df = st.session_state.get("df_filtrado", st.session_state.df)
if df is None or df.empty:
    st.warning("⚠️ No hay datos filtrados disponibles para generar insights.")
    st.stop()

# ===============================
# 🧹 Normalización mínima
# ===============================
df = df.copy()
df.columns = df.columns.str.lower().str.strip()

def _alias(_df, target, candidates):
    if target in _df.columns and _df[target].notna().any():
        return
    for c in candidates:
        if c in _df.columns and _df[c].notna().any():
            _df[target] = _df[c]
            return

# nombre_corto desde run_fm_nombrecorto (split tolerante) o aliases
if "nombre_corto" not in df.columns or df["nombre_corto"].isna().all():
    if "run_fm_nombrecorto" in df.columns:
        parts = df["run_fm_nombrecorto"].astype(str).str.split(r"\s*-\s*", n=1, regex=True, expand=True)
        if parts.shape[1] == 2:
            if "run_fm" not in df.columns or df["run_fm"].isna().all():
                df["run_fm"] = parts[0]
            df["nombre_corto"] = parts[1]
        else:
            _alias(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"])
    else:
        _alias(df, "nombre_corto", ["nombre_fondo", "nombre", "fondo"])

# run_fm si falta
if "run_fm" not in df.columns or df["run_fm"].isna().all():
    if "run_fm_nombrecorto" in df.columns:
        df["run_fm"] = df["run_fm_nombrecorto"].astype(str).str.split(r"\s*-\s*", n=1, regex=True, expand=True)[0]
    else:
        _alias(df, "run_fm", ["run", "rut_fm", "rut_fondo", "id_fondo"])
df["run_fm"] = df.get("run_fm", "").astype(str)

# nom_adm limpieza
_alias(df, "nom_adm", ["administradora", "adm", "nombre_adm", "nomadm", "nom__adm"])
if "nom_adm" in df.columns:
    df["nom_adm"] = (
        df["nom_adm"].astype(str)
        .str.replace("  ", " ", regex=False)
        .str.replace("ADMINISTRADORA GENERAL DE FONDOS", "", regex=False)
        .str.replace("S.A.", "", regex=False)
        .str.replace("ASSET MANAGEMENT", "AM", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .replace({"": np.nan})
    )

# venta_neta_mm si falta
if "venta_neta_mm" not in df.columns:
    if {"aportes_mm", "rescates_mm"}.issubset(df.columns):
        df["venta_neta_mm"] = pd.to_numeric(df["aportes_mm"], errors="coerce").fillna(0) + \
                              pd.to_numeric(df["rescates_mm"], errors="coerce").fillna(0)
    else:
        df["venta_neta_mm"] = 0.0
df["venta_neta_mm"] = pd.to_numeric(df["venta_neta_mm"], errors="coerce").fillna(0)

# Guardrails mínimos
req_cols = ["run_fm", "venta_neta_mm"]
faltan = [c for c in req_cols if c not in df.columns]
if faltan:
    st.error(f"❌ Faltan columnas para el insight: {faltan}")
    st.stop()

# ===============================
# 📌 Top 20 fondos (cacheado) por RUT
# ===============================
@st.cache_data
def calcular_top20(tab: pd.DataFrame) -> pd.DataFrame:
    base = tab[["run_fm", "venta_neta_mm", "nombre_corto", "nom_adm"]].copy()
    base["venta_neta_mm"] = pd.to_numeric(base["venta_neta_mm"], errors="coerce").fillna(0)

    # Suma por RUT
    suma = (
        base.groupby("run_fm", as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values("venta_neta_mm", ascending=False)
        .head(20)
    )

    # Nombre/Admin representativos por RUT: modo o primero no nulo
    def _representativo(s: pd.Series):
        s = s.dropna()
        if s.empty:
            return np.nan
        m = s.mode(dropna=True)
        return (m.iat[0] if not m.empty else s.iloc[0])

    nombres = base.dropna(subset=["nombre_corto"]).groupby("run_fm")["nombre_corto"].agg(_representativo)
    admins  = base.dropna(subset=["nom_adm"]).groupby("run_fm")["nom_adm"].agg(_representativo)

    out = suma.merge(nombres, on="run_fm", how="left").merge(admins, on="run_fm", how="left")
    out["nombre_corto"] = out["nombre_corto"].fillna("(Sin nombre)")
    out["nom_adm"] = out["nom_adm"].fillna("(Sin adm)")
    return out

top_fondos = calcular_top20(df)
if top_fondos.empty:
    st.warning("No hay Top 20 disponible con los filtros actuales.")
    st.stop()

# Contexto compacto para el prompt (CSV corto)
ctx_cols = ["run_fm", "nombre_corto", "nom_adm", "venta_neta_mm"]
contexto = top_fondos[ctx_cols].rename(columns={
    "run_fm": "RUT", "nombre_corto": "Fondo", "nom_adm": "Adm", "venta_neta_mm": "Venta_MM"
}).to_csv(index=False)

# ===============================
# 🔑 API Key híbrida (local o Railway)
# ===============================
OPENAI_KEY = None
try:
    OPENAI_KEY = st.secrets["OPENAI_API_KEY"]
except Exception:
    OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_KEY:
    st.error("❌ No se encontró OPENAI_API_KEY en secrets.toml ni en variables de entorno.")
    st.stop()

client = OpenAI(api_key=OPENAI_KEY)

# ===============================
# ⚙️ Parámetros IA
# ===============================
colm1, colm2 = st.columns(2)
modelo = colm1.selectbox("Modelo", ["gpt-4o-mini", "gpt-4o", "gpt-4.1-mini"], index=0)
temp = colm2.slider("Creatividad (temperature)", 0.0, 1.0, 0.2, 0.1)

# ===============================
# 🔍 Generar insight automático
# ===============================
if st.button("Generar Insight IA", use_container_width=True):
    try:
        prompt = (
            "Analiza el top 20 de fondos mutuos en Chile, por venta neta acumulada en MM CLP. "
            "Sé concreto (máx. 6 oraciones). Menciona: tendencia general, 1–2 administradoras destacadas, "
            "presencia de categorías/tipos, y posibles riesgos/opciones tácticas.\n\n"
            f"Contexto CSV (RUT,Fondo,Adm,Venta_MM):\n{contexto}"
        )
        with st.spinner("Analizando…"):
            resp = client.chat.completions.create(
                model=modelo,
                messages=[
                    {"role": "system", "content": "Eres un analista financiero experto en fondos mutuos chilenos. Sé preciso y ejecutivo."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=temp,
            )
        st.success(resp.choices[0].message.content)
    except RateLimitError:
        st.error("⚠️ Límite de uso alcanzado en OpenAI.")
    except APIError as e:
        st.error(f"⚠️ Error de API: {e}")
    except Exception as e:
        st.error(f"⚠️ Error inesperado: {e}")

# ===============================
# 💬 Chat con IA
# ===============================
st.markdown("### 💬 Chat con IA sobre el Top 20")
if "chat_historial" not in st.session_state:
    st.session_state.chat_historial = []

for msg in st.session_state.chat_historial:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

pregunta = st.chat_input("Escribe tu pregunta sobre los fondos")
if pregunta:
    st.session_state.chat_historial.append({"role": "user", "content": pregunta})
    with st.chat_message("user"):
        st.markdown(pregunta)

    try:
        prompt_chat = (
            "Usa estrictamente este contexto en CSV (RUT,Fondo,Adm,Venta_MM) para responder brevemente: \n"
            f"{contexto}\n\nPregunta: {pregunta}"
        )
        with st.chat_message("assistant"):
            with st.spinner("Analizando..."):
                resp_chat = client.chat.completions.create(
                    model=modelo,
                    messages=[
                        {"role": "system", "content": "Eres un analista financiero experto en fondos mutuos chilenos. Sé preciso y ejecutivo."},
                        *[{"role": m["role"], "content": m["content"]} for m in st.session_state.chat_historial],
                        {"role": "user", "content": prompt_chat}
                    ],
                    max_tokens=500,
                    temperature=temp,
                )
                salida = resp_chat.choices[0].message.content
                st.markdown(salida)
                st.session_state.chat_historial.append({"role": "assistant", "content": salida})
    except RateLimitError:
        st.error("⚠️ Límite de uso alcanzado en OpenAI.")
    except APIError as e:
        st.error(f"⚠️ Error de API: {e}")
    except Exception as e:
        st.error(f"⚠️ Error inesperado: {e}")

# ===============================
# 📊 Expandible abajo del chat
# ===============================
with st.expander("📊 Ver Top 20 Fondos Mutuos", expanded=False):
    st.dataframe(
        top_fondos.rename(columns={
            "run_fm": "RUT",
            "nombre_corto": "Nombre del Fondo",
            "nom_adm": "Administradora",
            "venta_neta_mm": "Venta Neta Acumulada (MM CLP)"
        }),
        use_container_width=True
    )
