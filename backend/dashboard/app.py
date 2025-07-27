import panel as pn
import pandas as pd
import os
from sqlalchemy import create_engine
import time

pn.extension('tabulator', design='material')

# === Conexión a PostgreSQL ===
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")

engine = create_engine(DB_URL)
print(f"🔗 Conectando a DB: {DB_URL}")

# === Query de columnas necesarias ===
query = """
SELECT 
    "FECHA_INF_DATE", "FECHA_INF", "RUN_FM", "Nombre_Corto", "NOM_ADM", "SERIE",
    "PATRIMONIO_NETO_MM", "VENTA_NETA_MM", "TIPO_FM", "Categoría"
FROM fondos_mutuos;
"""

print("📂 Leyendo datos desde PostgreSQL...")
df = pd.read_sql(query, engine)
total_registros = len(df)
df["RUN_FM_NOMBRECORTO"] = df["RUN_FM"].astype(str) + " - " + df["Nombre_Corto"].astype(str)
print(f"✅ Datos cargados: {total_registros:,} filas")

# === Splash ===
loading = pn.Column(
    "## 🚀 Cargando Dashboard de Fondos Mutuos...",
    pn.indicators.Number(name="Registros cargados", value=total_registros, format="{value:,}"),
    pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='primary'),
    align='center'
)
template = pn.template.MaterialTemplate(title="Dashboard Fondos Mutuos")
template.main.append(loading)
template.servable()
time.sleep(1)

# === Filtros ===
fecha_min, fecha_max = df["FECHA_INF_DATE"].min(), df["FECHA_INF_DATE"].max()
categorias = sorted(df["Categoría"].dropna().unique())
admins = sorted(df["NOM_ADM"].dropna().unique())
fondos = sorted(df["RUN_FM_NOMBRECORTO"].dropna().unique())
tipos = sorted(df["TIPO_FM"].dropna().unique())
series = sorted(df["SERIE"].dropna().unique())

categoria_multi = pn.widgets.MultiChoice(name="Categoría", options=categorias)
admin_multi = pn.widgets.MultiChoice(name="Administradora(s)", options=admins)
fondo_multi = pn.widgets.MultiChoice(name="Fondo(s)", options=fondos)
tipo_multi = pn.widgets.MultiChoice(name="Tipo de Fondo", options=tipos)
serie_multi = pn.widgets.MultiChoice(name="Serie(s)", options=series)

fecha_slider = pn.widgets.DateRangeSlider(name="Rango de Fechas",
                                          start=fecha_min, end=fecha_max,
                                          value=(fecha_min, fecha_max))

# === Función de filtrado ===
def filtrar_df():
    data = df.copy()
    if categoria_multi.value:
        data = data[data["Categoría"].isin(categoria_multi.value)]
    if admin_multi.value:
        data = data[data["NOM_ADM"].isin(admin_multi.value)]
    if fondo_multi.value:
        data = data[data["RUN_FM_NOMBRECORTO"].isin(fondo_multi.value)]
    if tipo_multi.value:
        data = data[data["TIPO_FM"].isin(tipo_multi.value)]
    if serie_multi.value:
        data = data[data["SERIE"].isin(serie_multi.value)]
    fechas = fecha_slider.value
    data = data[(data["FECHA_INF_DATE"] >= fechas[0]) & (data["FECHA_INF_DATE"] <= fechas[1])]
    return data

# === Tabs ===
@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_patrimonio(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    grouped = data.groupby("FECHA_INF_DATE")["PATRIMONIO_NETO_MM"].sum().reset_index()
    return grouped.hvplot.line(x="FECHA_INF_DATE", y="PATRIMONIO_NETO_MM",
                               title="Patrimonio Neto Total (MM CLP)")

@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_ventas(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    ventas = data.groupby("FECHA_INF_DATE")["VENTA_NETA_MM"].sum().cumsum().reset_index()
    return ventas.hvplot.line(x="FECHA_INF_DATE", y="VENTA_NETA_MM",
                              title="Venta Neta Acumulada (MM CLP)")

@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_listado(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    ranking = (
        data.groupby(["RUN_FM", "Nombre_Corto", "NOM_ADM"], as_index=False)["VENTA_NETA_MM"]
        .sum()
        .sort_values(by="VENTA_NETA_MM", ascending=False)
        .head(20)
        .copy()
    )
    ranking["URL CMF"] = ranking["RUN_FM"].astype(str).apply(
        lambda r: f'<a href="https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={r}&tipoentidad=RGFMU&vig=VI" target="_blank">Ver</a>'
    )
    return pn.widgets.Tabulator(ranking, formatters={"URL CMF": "html"}, pagination='remote', page_size=20)

# === Sidebar y layout ===
template.main[:] = []
template.sidebar.append(pn.pane.Markdown(f"### ℹ️ Datos cargados: **{total_registros:,}** registros"))
template.sidebar.append(categoria_multi)
template.sidebar.append(admin_multi)
template.sidebar.append(fondo_multi)
template.sidebar.append(tipo_multi)
template.sidebar.append(serie_multi)
template.sidebar.append(fecha_slider)

tabs = pn.Tabs(
    ("📊 Patrimonio Neto Total", vista_patrimonio),
    ("💰 Venta Neta Acumulada", vista_ventas),
    ("🏆 Listado de Fondos Mutuos", vista_listado),
)
template.main.append(tabs)

footer = pn.pane.HTML("""
<div style='text-align:center; font-size:12px; padding:10px; border-top:1px solid #ccc;'>
    Autor: Nicolás Fernández Ponce, CFA | Datos provistos por la <a href="https://www.cmfchile.cl" target="_blank">CMF</a>
</div>
""", sizing_mode="stretch_width")
template.main.append(footer)
