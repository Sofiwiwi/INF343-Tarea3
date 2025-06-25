#!/bin/bash
# start.sh - Inicia un nodo de forma limpia.

# --- VALIDACIÓN DE ARGUMENTO ---
if [ -z "$1" ]; then
    echo "Uso: $0 <ID_DEL_NODO>"
    echo "Ejemplo: $0 1"
    exit 1
fi

NODE_ID=$1
PROJECT_PATH=$(pwd) # Asume que se ejecuta desde la carpeta del proyecto

echo "[INFO] Preparando para iniciar Nodo ${NODE_ID}..."

# --- LÓGICA DE LA SOLUCIÓN ---
# Borramos el archivo de estado anterior para asegurar un inicio como secundario.
# Esto es crucial para una correcta reintegración después de una caída.
if [ -f "node_${NODE_ID}.json" ]; then
    echo "[INFO] Limpiando estado de líder anterior (borrando node_${NODE_ID}.json)..."
    rm "node_${NODE_ID}.json"
fi

# Verificamos si ya está corriendo
if [ -f "node.pid" ]; then
    echo "[ERROR] El proceso ya está en ejecución (PID $(cat node.pid)). Usa ./crash_node.sh ${NODE_ID} para detenerlo primero."
    exit 1
fi

# --- INICIO DEL NODO ---
echo "[INFO] Iniciando proceso del Nodo ${NODE_ID} en segundo plano..."
nohup go run main.go ${NODE_ID} > "node${NODE_ID}.log" 2>&1 &
echo "[OK] Nodo ${NODE_ID} iniciado. Log en node${NODE_ID}.log"