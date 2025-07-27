import panel as pn
import pandas as pd
import hvplot.pandas
from sqlalchemy import create_engine
import os
import requests

pn.extension('tabulator', 'plotly')

# ------------------------
# Configuración DB
# ------------------------
DATABASE_URL = os.getenv("DATABASE_URL")  # Seteada en Railway
engine = create_engine(DATABASE_URL)

# ------------------------
# Función de carga desde Postgres
# ------------------------
@pn.cache
def cargar_datos():
    query = """
        SELECT 
            FECHA_INF, NOM_ADM, SERIE, 
            CUOTAS_APORTADAS, CUOTAS_RESCATADAS,
            CUOTAS_EN_CIRCULACION, PATRIMONIO_NETO,
            RUN_FM, RUN_ADM
        FROM fondos_mutuos;
    """
    df = pd.read_sql(query, engine)
    df["FECHA_INF"] = pd.to_datetime(df["FECHA_INF"])
    return df

df = cargar_datos()

# ------------------------
# Widgets de filtros
# ------------------------
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
    name="Serie",
    options=sorted(df["SERIE"].dropna().unique().tolist()),
    size=6
)

def filtrar_data():
    data = df[(df["FECHA_INF"] >= fechas.value[0]) & (df["FECHA_INF"] <= fechas.value[1])]
    if adm.value:
        data = data[data["NOM_ADM"].isin(adm.value)]
    if tipos.value:
        data = data[data["SERIE"].isin(tipos.value)]
    return data

# ------------------------
# Vistas (igual que Streamlit)
# ------------------------
@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def vista_patrimonio():
    data = filtrar_data()
    plot = data.groupby("FECHA_INF")["PATRIMONIO_NETO"].sum().hvplot.line(
        title="Patrimonio Neto Total",
        ylabel="Patrimonio",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def vista_ventas():
    data = filtrar_data()
    data["VENTA_NETA"] = (data["CUOTAS_APORTADAS"] - data["CUOTAS_RESCATADAS"]) * (
        data["PATRIMONIO_NETO"] / data["CUOTAS_EN_CIRCULACION"]
    )
    plot = data.groupby("FECHA_INF")["VENTA_NETA"].sum().hvplot.bar(
        title="Ventas Netas",
        ylabel="Ventas",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def vista_ranking():
    data = filtrar_data()
    ranking = (
        data.groupby("NOM_ADM")["PATRIMONIO_NETO"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .reset_index()
    )
    return ranking.hvplot.barh(x="NOM_ADM", y="PATRIMONIO_NETO", title="Top 15 Administradoras")

@pn.depends(fechas.param.value, adm.param.value, tipos.param.value)
def vista_fondos():
    data = filtrar_data()[["RUN_FM", "NOM_ADM", "SERIE", "PATRIMONIO_NETO"]].copy()
    data["URL_CMF"] = data["RUN_FM"].apply(lambda x: f"https://www.cmfchile.cl/entidad.php?rut={x}")
    return pn.widgets.Tabulator(data, pagination='remote', page_size=20, width=900)

def vista_insights():
    try:
        r = requests.get("http://localhost:8000/ia/insights")
        if r.status_code == 200:
            return pn.pane.Markdown(f"### 🤖 Insights IA\n\n{r.json().get('insight', 'Sin datos')}")
        else:
            return pn.pane.Markdown("❌ No se pudo conectar al endpoint IA")
    except:
        return pn.pane.Markdown("⚠️ Endpoint IA no disponible")

# ------------------------
# Layout con Tabs
# ------------------------
filtros = pn.Column("## Filtros", fechas, adm, tipos)

tabs = pn.Tabs(
    ("📈 Patrimonio", vista_patrimonio),
    ("💰 Ventas", vista_ventas),
    ("🏆 Ranking", vista_ranking),
    ("📜 Fondos", vista_fondos),
    ("🤖 Insights IA", vista_insights),
)

dashboard = pn.Row(filtros, tabs)

dashboard.servable()
