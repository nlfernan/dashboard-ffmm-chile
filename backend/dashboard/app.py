import panel as pn
import pandas as pd
import os

pn.extension('tabulator', design='material')  # Activa Material Design

# === Cargar datos ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")
df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")

# === Filtros dinámicos ===
fecha_min, fecha_max = df["FECHA_INF_DATE"].min(), df["FECHA_INF_DATE"].max()
admins = sorted(df["NOM_ADM"].dropna().unique())
series = sorted(df["SERIE"].dropna().unique())

fecha_slider = pn.widgets.DateRangeSlider(name="Rango de Fechas",
                                          start=fecha_min, end=fecha_max,
                                          value=(fecha_min, fecha_max))
admin_multi = pn.widgets.MultiChoice(name="Administradora", options=admins)
serie_multi = pn.widgets.MultiChoice(name="Serie", options=series)

# === Callback para filtrar ===
@pn.depends(fecha_slider, admin_multi, serie_multi)
def grafico_patrimonio(fechas, admins_sel, series_sel):
    data = df.copy()
    data = data[(data["FECHA_INF_DATE"] >= fechas[0]) & (data["FECHA_INF_DATE"] <= fechas[1])]
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

# === Layout con MaterialTemplate ===
template = pn.template.MaterialTemplate(title="Dashboard FFMM Chile")
template.sidebar.append(pn.pane.Markdown("### Filtros"))
template.sidebar.append(fecha_slider)
template.sidebar.append(admin_multi)
template.sidebar.append(serie_multi)

template.main.append(pn.Tabs(
    ("📊 Patrimonio", grafico_patrimonio),
    ("💰 Ventas", pn.pane.Markdown("*Placeholder Ventas*")),
    ("🏆 Ranking", pn.pane.Markdown("*Placeholder Ranking*")),
))

template.servable()
