import panel as pn
import pandas as pd
import os
import time

pn.extension('tabulator', design='material')

# === Cargar datos una sola vez con solo columnas necesarias ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

print("📂 Leyendo parquet (columnas mínimas)...")
cols_necesarias = ["FECHA_INF_DATE", "NOM_ADM", "SERIE", "PATRIMONIO_NETO_MM"]
df = pd.read_parquet(PARQUET_PATH, columns=cols_necesarias, engine="pyarrow")
total_registros = len(df)
print(f"✅ Datos cargados: {total_registros:,} filas")

# === Pantalla inicial rápida ===
loading = pn.Column(
    "## 🚀 Cargando Dashboard de Fondos Mutuos...",
    pn.indicators.Number(name="Registros cargados", value=total_registros, format="{value:,}"),
    pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='primary'),
    align='center'
)

template = pn.template.MaterialTemplate(title="Dashboard FFMM Chile")
template.main.append(loading)
template.servable()

# Pequeño delay solo para mostrar el splash
time.sleep(1)

# === Filtros ===
fecha_min, fecha_max = df["FECHA_INF_DATE"].min(), df["FECHA_INF_DATE"].max()
admins = sorted(df["NOM_ADM"].dropna().unique())
series = sorted(df["SERIE"].dropna().unique())

fecha_slider = pn.widgets.DateRangeSlider(name="Rango de Fechas",
                                          start=fecha_min, end=fecha_max,
                                          value=(fecha_min, fecha_max))
admin_multi = pn.widgets.MultiChoice(name="Administradora", options=admins)
serie_multi = pn.widgets.MultiChoice(name="Serie", options=series)

# === Callback para gráfico ===
@pn.depends(fecha_slider, admin_multi, serie_multi)
def grafico_patrimonio(fechas, admins_sel, series_sel):
    data = df[(df["FECHA_INF_DATE"] >= fechas[0]) & (df["FECHA_INF_DATE"] <= fechas[1])]
    if admins_sel:
        data = data[data["NOM_ADM"].isin(admins_sel)]
    if series_sel:
        data = data[data["SERIE"].isin(series_sel)]
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    grouped = data.groupby("FECHA_INF_DATE")["PATRIMONIO_NETO_MM"].sum().reset_index()
    return grouped.hvplot.line(x="FECHA_INF_DATE", y="PATRIMONIO_NETO_MM",
                               title="Patrimonio Neto Total (MM)",
                               xlabel="Fecha", ylabel="Patrimonio (MM)")

# === Reemplazar pantalla de carga por el dashboard ===
template.main[:] = []  # limpia el contenido inicial
template.sidebar.append(pn.pane.Markdown(f"### ℹ️ Datos cargados: **{total_registros:,}** registros"))
template.sidebar.append(fecha_slider)
template.sidebar.append(admin_multi)
template.sidebar.append(serie_multi)

template.main.append(pn.Tabs(
    ("📊 Patrimonio", grafico_patrimonio),
    ("💰 Ventas", pn.pane.Markdown("*Placeholder Ventas*")),
    ("🏆 Ranking", pn.pane.Markdown("*Placeholder Ranking*")),
))
