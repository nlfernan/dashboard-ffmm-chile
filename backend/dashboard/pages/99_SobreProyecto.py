# -*- coding: utf-8 -*-
import streamlit as st

st.title("👤 Sobre mí y el proyecto")

st.markdown("""
### 📌 Sobre mí
Soy **Nicolás Fernández Ponce, CFA**, Ingeniero Comercial UC. Trabajé en banca y asset management, con foco en **gestión de capital regulatorio (CRR/CRD, Basilea III/IV, CMF)**, automatización de reportes y **analytics con Python/SQL**. Lideré iniciativas con impacto material en solvencia y reporting ejecutivo, integrando riesgo, finanzas y negocio.

### 📌 Sobre el proyecto
Este dashboard convierte **datos complejos** en **insights accionables** para la industria chilena:

- **Cobertura de datos:** **Fondos Mutuos y AFPs**.
    - FFMM: flujos diarios y acumulados (aportes, rescates, venta neta), patrimonio, ranking por categorías y administradoras, y *insights* con IA.
    - **AFPs:** series y tableros para **multifondos**, **patrimonio/afiliados**, **flujos**, **rentabilidades** y **comisiones** (cuando aplique en tu dataset).
- **Stack técnico:** Python + Streamlit, backend **PostgreSQL**, despliegue en **Railway**.
- **Objetivo:** entregar **visibilidad comparativa** y **alertas/explicaciones** (IA ligera) para decisiones comerciales, de producto y control de gestión.

### 🚀 Roadmap breve
- Integración total AFPs: normalización por multifondo, métricas homogéneas y comparables.
- Alertas IA: anomalías en flujos/rentabilidades, explicaciones ejecutivas.
- Exportables limpios (CSV/Excel) por pestaña y *deep links* con filtros.
""")

# Botón de LinkedIn
st.link_button("🔗 Mi perfil en LinkedIn", "https://www.linkedin.com/in/nicolas-fernandez-ponce-cfa/")
