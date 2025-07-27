import panel as pn
import pandas as pd
import hvplot.pandas
from sqlalchemy import create_engine
import os

pn.extension('tabulator', 'plotly')

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# ------------------------
# Cargar datos desde Postgres
# ------------------------
query = """
SELECT fecha_inf, fecha_inf_date, nom_adm, serie,
       cuotas_aportadas, cuotas_rescatadas,
       cuotas_en_circulacion, patrimonio_neto,
       run_fm, nombre_fondo
FROM fondos_mutuos;
"""
df = pd.read_sql(query, engine)

# ------------------------
# Asegurar columna fecha_inf y parseo correcto
# ------------------------
if "fecha_inf" in df.columns:
    df["fecha_inf"] = pd.to_datetime(df["fecha_inf"].astype(str), format="%Y%m%d", errors="coerce")

# Si fecha_inf está vacío pero existe fecha_inf_date, usarla
if df["fecha_inf"].dropna().empty and "fecha_inf_date" in df.columns:
    df["fecha_inf"] = pd.to_datetime(df["fecha_inf_date"], errors="coerce")

print("🧪 Rango de fechas detectado:", df["fecha_inf"].min(), "→", df["fecha_inf"].max())

# ------------------------
# Control si no hay fechas válidas
# ------------------------
if df.empty or df["fecha_inf"].dropna().empty:
    contenido = pn.Column(
        "# 🚀 Dashboard FFMM Chile",
        "⏳ Esperando que los datos terminen de cargarse o no hay fechas válidas en la tabla..."
    )
    contenido.servable()
else:
    # ------------------------
    # Widgets de filtros
    # ------------------------
    fecha_min = df["fecha_inf"].min()
    fecha_max = df["fecha_inf"].max()

    fechas = pn.widgets.DateRangeSlider(
        name="Rango de Fechas",
        start=fecha_min,
        end=fecha_max,
        value=(fecha_min, fecha_max)
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

    def filtrar_data(rango_fechas, adm_value, serie_value):
        data = df[(df["fecha_inf"] >= rango_fechas[0]) & (df["fecha_inf"] <= rango_fechas[1])]
        if adm_value:
            data = data[data["nom_adm"].isin(adm_value)]
        if serie_value:
            data = data[data["serie"].isin(serie_value)]
        return data

    @pn.depends(fechas.param.value, adm.param.value, serie.param.value)
    def vista_patrimonio(rango_fechas, adm_value, serie_value):
        data = filtrar_data(rango_fechas, adm_value, serie_value)
        return data.groupby("fecha_inf")["patrimonio_neto"].sum().hvplot.line(
            title="Patrimonio Neto Total", ylabel="Patrimonio", xlabel="Fecha"
        )

    @pn.depends(fechas.param.value, adm.param.value, serie.param.value)
    def vista_ventas(rango_fechas, adm_value, serie_value):
        data = filtrar_data(rango_fechas, adm_value, serie_value)
        data["venta_neta"] = (data["cuotas_aportadas"] - data["cuotas_rescatadas"]) * (
            data["patrimonio_neto"] / data["cuotas_en_circulacion"]
        )
        return data.groupby("fecha_inf")["venta_neta"].sum().hvplot.bar(
            title="Ventas Netas", ylabel="Ventas", xlabel="Fecha"
        )

    @pn.depends(fechas.param.value, adm.param.value, serie.param.value)
    def vista_ranking(rango_fechas, adm_value, serie_value):
        data = filtrar_data(rango_fechas, adm_value, serie_value)
        ranking = (
            data.groupby("nom_adm")["patrimonio_neto"]
            .sum()
            .sort_values(ascending=False)
            .head(15)
            .reset_index()
        )
        return ranking.hvplot.barh(x="nom_adm", y="patrimonio_neto", title="Top 15 Administradoras")

    filtros = pn.Column("## Filtros", fechas, adm, serie)
    tabs = pn.Tabs(
        ("📈 Patrimonio", vista_patrimonio),
        ("💰 Ventas", vista_ventas),
        ("🏆 Ranking", vista_ranking)
    )

    dashboard = pn.Row(filtros, tabs)
    dashboard.servable()
