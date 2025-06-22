#!/bin/bash
# kill.sh - Simula la caída de un nodo.

# --- VALIDACIÓN DE ARGUMENTO ---
if [ -z "$1" ]; then
    echo "Uso: $0 <ID_DEL_NODO>"
    echo "Ejemplo: $0 3"
    exit 1
fi

NODE_ID=$1

echo "[INFO] Intentando detener el Nodo ${NODE_ID}..."

if [ -f "node.pid" ]; then
    PID=$(cat "node.pid")
    echo "[INFO] Deteniendo proceso del Nodo ${NODE_ID} (PID ${PID}) con kill -9..."
    kill -9 ${PID}
    rm "node.pid"
    echo "[OK] Proceso detenido."
else
    echo "[WARN] No se encontró el archivo node.pid. El proceso podría no estar en ejecución."
fi