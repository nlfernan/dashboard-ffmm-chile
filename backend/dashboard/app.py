import panel as pn

pn.extension()

# Componente de prueba
mensaje = pn.pane.Markdown("""
# 🚀 Dashboard FFMM Chile
✅ Panel está funcionando correctamente en Railway.
""", width=600)

# Layout mínimo
dashboard = pn.Column(mensaje)

dashboard.servable()

