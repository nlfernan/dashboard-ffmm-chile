import panel as pn
import pandas as pd
import hvplot.pandas
from sqlalchemy import create_engine
import os

pn.extension('tabulator', 'plotly')

# ------------------------
# Configuración DB
# ------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# ------------------------
# Cargar datos desde PostgreSQL
# ------------------------
@pn.cache
def cargar_datos():
    query = """
        SELECT 
            fecha_inf, nom_adm, serie,
            cuotas_aportadas, cuotas_rescatadas,
            cuotas_en_circulacion, patrimonio_neto,
            run_fm, nombre_fondo
        FROM fondos_mutuos;
    """
    df = pd.read_sql(query, engine)
    df["fecha_inf"] = pd.to_datetime(df["fecha_inf"])
    return df

df = cargar_datos()

# ------------------------
# Widgets de filtros
# ------------------------
fechas = pn.widgets.DateRangeSlider(
    name="Rango de Fechas",
    start=df["fecha_inf"].min(),
    end=df["fecha_inf"].max(),
    value=(df["fecha_inf"].min(), df["fecha_inf"].max())
)

adm = pn.widgets.MultiSelect(
    name="Administradora",
    options=sorted(df["nom_adm"].dropna().unique().tolist()),
    size=6
)

serie = pn.widgets.MultiSelect(
    name="Serie",
    options=sorted(df["serie"].dropna().unique().tolist()),
    size=6
)

def filtrar_data():
    data = df[(df["fecha_inf"] >= fechas.value[0]) & (df["fecha_inf"] <= fechas.value[1])]
    if adm.value:
        data = data[data["nom_adm"].isin(adm.value)]
    if serie.value:
        data = data[data["serie"].isin(serie.value)]
    return data

# ------------------------
# Vistas
# ------------------------
@pn.depends(fechas.param.value, adm.param.value, serie.param.value)
def vista_patrimonio():
    data = filtrar_data()
    plot = data.groupby("fecha_inf")["patrimonio_neto"].sum().hvplot.line(
        title="Patrimonio Neto Total",
        ylabel="Patrimonio",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, serie.param.value)
def vista_ventas():
    data = filtrar_data()
    # Ventas netas aproximadas
    data["venta_neta"] = (data["cuotas_aportadas"] - data["cuotas_rescatadas"]) * (
        data["patrimonio_neto"] / data["cuotas_en_circulacion"]
    )
    plot = data.groupby("fecha_inf")["venta_neta"].sum().hvplot.bar(
        title="Ventas Netas",
        ylabel="Ventas",
        xlabel="Fecha"
    )
    return plot

@pn.depends(fechas.param.value, adm.param.value, serie.param.value)
def vista_ranking():
    data = filtrar_data()
    ranking = (
        data.groupby("nom_adm")["patrimonio_neto"]
        .sum()
        .sort_values(ascending=False)
        .head(15)
        .reset_index()
    )
    return ranking.hvplot.barh(x="nom_adm", y="patrimonio_neto", title="Top 15 Administradoras")

@pn.depends(fechas.param.value, adm.param.value, serie.param.value)
def vista_fondos():
    data = filtrar_data()[["run_fm", "nombre_fondo", "nom_adm", "serie", "patrimonio_neto"]].copy()
    data["url_cmf"] = data["run_fm"].apply(lambda x: f"https://www.cmfchile.cl/entidad.php?rut={x}")
    return pn.widgets.Tabulator(data, pagination='remote', page_size=20, width=900)

# ------------------------
# Layout
# ------------------------
filtros = pn.Column("## Filtros", fechas, adm, serie)

tabs = pn.Tabs(
    ("📈 Patrimonio", vista_patrimonio),
    ("💰 Ventas", vista_ventas),
    ("🏆 Ranking", vista_ranking),
    ("📜 Fondos", vista_fondos),
)

dashboard = pn.Row(filtros, tabs)

dashboard.servable()
