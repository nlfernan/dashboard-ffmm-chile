import panel as pn
import pandas as pd
import os
import time
from sqlalchemy import create_engine

pn.extension('tabulator', design='material')

# === Conexión a PostgreSQL ===
DB_URL = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
if not DB_URL:
    raise RuntimeError("❌ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL.")

engine = create_engine(DB_URL)
print(f"🔗 Conectando a DB: {DB_URL}")

# === Cargar solo columnas necesarias ===
query = """
SELECT 
    fecha_inf_date, run_fm, nombre_corto, nom_adm, serie,
    patrimonio_neto_mm, venta_neta_mm, tipo_fm, categoria
FROM fondos_mutuos;
"""
print("📂 Leyendo datos desde PostgreSQL...")
df = pd.read_sql(query, engine)
total_registros = len(df)
print(f"✅ Datos cargados: {total_registros:,} filas")

# === Columna combinada para filtro de fondos ===
df["run_fm_nombrecorto"] = df["run_fm"].astype(str) + " - " + df["nombre_corto"].astype(str)

# === Splash screen ===
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

# === Filtros dinámicos ===
fecha_min, fecha_max = df["fecha_inf_date"].min(), df["fecha_inf_date"].max()
categorias = sorted(df["categoria"].dropna().unique())
admins = sorted(df["nom_adm"].dropna().unique())
fondos = sorted(df["run_fm_nombrecorto"].dropna().unique())
tipos = sorted(df["tipo_fm"].dropna().unique())
series = sorted(df["serie"].dropna().unique())

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
        data = data[data["categoria"].isin(categoria_multi.value)]
    if admin_multi.value:
        data = data[data["nom_adm"].isin(admin_multi.value)]
    if fondo_multi.value:
        data = data[data["run_fm_nombrecorto"].isin(fondo_multi.value)]
    if tipo_multi.value:
        data = data[data["tipo_fm"].isin(tipo_multi.value)]
    if serie_multi.value:
        data = data[data["serie"].isin(serie_multi.value)]
    fechas = fecha_slider.value
    data = data[(data["fecha_inf_date"] >= fechas[0]) & (data["fecha_inf_date"] <= fechas[1])]
    return data

# === Tab Patrimonio ===
@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_patrimonio(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    grouped = data.groupby("fecha_inf_date")["patrimonio_neto_mm"].sum().reset_index()
    return grouped.hvplot.line(x="fecha_inf_date", y="patrimonio_neto_mm",
                               title="Patrimonio Neto Total (MM CLP)",
                               xlabel="Fecha", ylabel="Patrimonio (MM)")

# === Tab Venta Neta ===
@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_ventas(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    ventas = data.groupby("fecha_inf_date")["venta_neta_mm"].sum().cumsum().reset_index()
    return ventas.hvplot.line(x="fecha_inf_date", y="venta_neta_mm",
                              title="Venta Neta Acumulada (MM CLP)",
                              xlabel="Fecha", ylabel="Venta Neta (MM)")

# === Tab Listado ===
@pn.depends(categoria_multi, admin_multi, fondo_multi, tipo_multi, serie_multi, fecha_slider)
def vista_listado(*_):
    data = filtrar_df()
    if data.empty:
        return pn.pane.Markdown("⚠️ *No hay datos para los filtros seleccionados*")
    ranking = (
        data.groupby(["run_fm", "nombre_corto", "nom_adm"], as_index=False)["venta_neta_mm"]
        .sum()
        .sort_values(by="venta_neta_mm", ascending=False)
        .head(20)
        .copy()
    )
    ranking["URL CMF"] = ranking["run_fm"].astype(str).apply(
        lambda r: f'<a href="https://www.cmfchile.cl/institucional/mercados/entidad.php?auth=&send=&mercado=V&rut={r}&tipoentidad=RGFMU&vig=VI" target="_blank">Ver</a>'
    )
    return pn.widgets.Tabulator(ranking, formatters={"URL CMF": "html"}, pagination='remote', page_size=20)

# === Sidebar ===
template.main[:] = []
template.sidebar.append(pn.pane.Markdown(f"### ℹ️ Datos cargados: **{total_registros:,}** registros"))
template.sidebar.append(categoria_multi)
template.sidebar.append(admin_multi)
template.sidebar.append(fondo_multi)
template.sidebar.append(tipo_multi)
template.sidebar.append(serie_multi)
template.sidebar.append(fecha_slider)

# === Tabs principales ===
tabs = pn.Tabs(
    ("📊 Patrimonio Neto Total", vista_patrimonio),
    ("💰 Venta Neta Acumulada", vista_ventas),
    ("🏆 Listado de Fondos Mutuos", vista_listado),
)
template.main.append(tabs)

# === Footer ===
footer = pn.pane.HTML("""
<div style='text-align:center; font-size:12px; padding:10px; border-top:1px solid #ccc;'>
    Autor: Nicolás Fernández Ponce, CFA | Datos provistos por la <a href="https://www.cmfchile.cl" target="_blank">CMF</a>
</div>
""", sizing_mode="stretch_width")
template.main.append(footer)
