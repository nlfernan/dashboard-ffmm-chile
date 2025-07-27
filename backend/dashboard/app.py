
import pandas as pd
import panel as pn
import hvplot.pandas  # Para gráficos interactivos
import requests
import os

pn.extension('tabulator', 'plotly')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = os.path.join(BASE_DIR, "../data_fuentes/ffmm_merged.parquet")

df = pd.read_parquet(PARQUET_PATH)
df["FECHA_INF"] = pd.to_datetime(df["FECHA_INF"], format="%Y%m%d", errors="coerce")

fechas = pn.widgets.DateRangeSlider(
    name="Rango de Fechas",
    start=df["FECHA_INF"].min(),
    end=df["FECHA_INF"].max(),
    value=(df["FECHA_INF"].min(), df["FECHA_INF"].max())
)

adm = pn.widgets.MultiSelect(
    name="Administradora",
    options=sorted(df["NOM_ADM"].dropna().unique().tolist()),
    size=6
)

tipos = pn.widgets.MultiSelect(
    name="Tipo de Fondo",
    options=sorted(df["SERIE"].dropna().unique().tolist()),
    size=6
)

def filtrar_data():
    data = df.copy()
    data = data[(data["FECHA_INF"] >= fechas.value[0]) & (data["FECHA_INF"] <= fechas.value[1])]
    if adm.value:
        data = data[data["NOM_ADM"].isin(adm.value)]
    if tipos.value:
        data = data[data["SERIE"].isin(tipos.value)]
    return data

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def tab_patrimonio():
    data = filtrar_data()
    plot = data.groupby("FECHA_INF")["PATRIMONIO_NETO"].sum().hvplot.line(
        title="Patrimonio Neto Total",
        ylabel="Patrimonio",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def tab_ventas():
    data = filtrar_data()
    data["VENTA_NETA"] = data["CUOTAS_APORTADAS"] - data["CUOTAS_RESCATADAS"]
    plot = data.groupby("FECHA_INF")["VENTA_NETA"].sum().hvplot.bar(
        title="Ventas Netas",
        ylabel="Ventas",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def tab_ranking():
    data = filtrar_data()
    ranking = (
        data.groupby("NOM_ADM")["PATRIMONIO_NETO"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .reset_index()
    )
    return ranking.hvplot.barh(x="NOM_ADM", y="PATRIMONIO_NETO", title="Top 15 Administradoras")

def tab_insights():
    try:
        r = requests.get("http://localhost:8000/ia/insights")
        if r.status_code == 200:
            return pn.pane.Markdown(f"### 🤖 Insights IA\n\n{r.json().get('insight', 'Sin datos')}")
        else:
            return pn.pane.Markdown("❌ No se pudo conectar al endpoint IA")
    except:
        return pn.pane.Markdown("⚠️ Endpoint IA no disponible")

filtros = pn.Column("## Filtros", fechas, adm, tipos)

tabs = pn.Tabs(
    ("📈 Patrimonio", tab_patrimonio),
    ("💰 Ventas", tab_ventas),
    ("🏆 Ranking", tab_ranking),
    ("🤖 Insights IA", tab_insights),
)

dashboard = pn.Row(filtros, tabs)

dashboard.servable()
