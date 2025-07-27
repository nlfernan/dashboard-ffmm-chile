import panel as pn

# 🔍 Debug: confirmar que el archivo se está ejecutando
print("✅ [DEBUG] dashboard/app.py: El archivo se está cargando correctamente.")

# Inicializar extensión de Panel
pn.extension()

# 🔍 Debug: confirmar que Panel.extension() no da error
print("✅ [DEBUG] Panel.extension inicializada.")

# Componente de prueba mínimo
mensaje = pn.pane.Markdown("""
# 🚀 Dashboard FFMM Chile
✅ Panel está funcionando y ejecutó el archivo app.py correctamente.
""", width=600)

# Layout básico
dashboard = pn.Column(
    "## Debug",
    "Si ves este texto en Railway, Panel está sirviendo el dashboard.",
    mensaje
)

# 🔍 Debug: confirmar que se montó el layout
print("✅ [DEBUG] Layout del dashboard creado, listo para servirse.")

# Servir el dashboard
dashboard.servable()

# 🔍 Debug final
print("✅ [DEBUG] dashboard.app.servable() ejecutado, esperando que Bokeh arranque.")
