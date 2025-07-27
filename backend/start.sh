
#!/bin/bash
panel serve dashboard/app.py \
    --address 0.0.0.0 \
    --port $PORT \
    --allow-websocket-origin="*" \
    --prefix "" 
