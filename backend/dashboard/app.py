import panel as pn
import pandas as pd
import hvplot.pandas
from sqlalchemy import create_engine
import os
import threading
import time

pn.extension('tabulator', 'plotly')

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# ------------------------
# Estado inicial con spinner y contador de filas
# ------------------------
contador = pn.indicators.Number(name="Filas cargadas", value=0, format="{value:,}")
estado = pn.indicators.LoadingSpinner(value=True, width=50, height=50)
mensaje_estado = pn.pane.Markdown("### ⏳ Cargando datos desde PostgreSQL...")

contenido = pn.Column(
    "# 🚀 Dashboard FFMM Chile",
    estado,
    mensaje_estado,
    contador
)

df = pd.DataFrame()

# ------------------------
# Función que carga los datos y actualiza contador
# ------------------------
def cargar_datos():
    global df, contenido
    try:
        query = """
            SELECT fecha_inf, nom_adm, serie,
                   cuotas_aportadas, cuotas_rescatadas,
                   cuotas_en_circulacion, patrimonio_neto,
                   run_fm, nombre_fondo
            FROM fondos_mutuos;
        """
        chunks = pd.read_sql(query, engine, chunksize=50000)
        data_list = []
        total = 0
        for chunk in chunks:
            chunk["fecha_inf"] = pd.to_datetime(chunk["fecha_inf"])
            data_list.append(chunk)
            total += len(chunk)
            contador.value = total
            time.sleep(0.1)  # Pequeño delay para que se vea la animación
        df_final = pd.concat(data_list, ignore_index=True)
        contenido[:] = [crear_dashboard(df_final)]
    except Exception as e:
        contenido[:] = [pn.pane.Markdown(f"❌ Error cargando datos: {e}")]

# ------------------------
# Función que construye el dashboard final
# ------------------------
def crear_dashboard(df):
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

    @pn.depends(fechas.param.value, adm.param.value, serie.param.value)
    def vista_fondos(rango_fechas, adm_value, serie_value):
        data = filtrar_data(rango_fechas, adm_value, serie_value)[
            ["run_fm", "nombre_fondo", "nom_adm", "serie", "patrimonio_neto"]
        ].copy()
        data["url_cmf"] = data["run_fm"].apply(lambda x: f"https://www.cmfchile.cl/entidad.php?rut={x}")
        return pn.widgets.Tabulator(data, pagination='remote', page_size=20, width=900)

    filtros = pn.Column("## Filtros", fechas, adm, serie)
    tabs = pn.Tabs(
        ("📈 Patrimonio", vista_patrimonio),
        ("💰 Ventas", vista_ventas),
        ("🏆 Ranking", vista_ranking),
        ("📜 Fondos", vista_fondos),
    )
    return pn.Row(filtros, tabs)

# ------------------------
# Lanzar carga de datos en background al iniciar
# ------------------------
threading.Thread(target=cargar_datos, daemon=True).start()

# ------------------------
# Servir el dashboard
# ------------------------
contenido.servable()
