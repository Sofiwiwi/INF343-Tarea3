#!/bin/bash

# --- CONFIGURACIÓN ---
# Asegúrate de que esta ruta sea correcta.
PROJECT_PATH="/home/ubuntu/INF343-Tarea3"
SIMULATION_LOG_FILE="simulation.log" # Nombre del archivo para los logs de este script

IP_NODO_1="10.10.28.17"
IP_NODO_2="10.10.28.18"
IP_NODO_3="10.10.28.19"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "${SIMULATION_LOG_FILE}"
}

start_go_process() {
    if [ -f "node.pid" ]; then
        log "El proceso del nodo ya parece estar en ejecución. Saliendo."
        exit 1
    fi
    log "Iniciando proceso del Nodo ${NODE_ID}..."
    cd ${PROJECT_PATH}
    nohup go run main.go ${NODE_ID} > "node${NODE_ID}.log" 2>&1 &
    echo $! > "node.pid"
    log "Proceso del Nodo ${NODE_ID} iniciado con PID $(cat node.pid). Log en node${NODE_ID}.log"
}

# Detiene el proceso 'go run' usando el archivo PID
stop_go_process() {
    if [ -f "node.pid" ]; then
        log "Deteniendo proceso del Nodo ${NODE_ID} (PID $(cat node.pid))."
        kill $(cat "node.pid")
        rm "node.pid"
        log "Proceso detenido."
    else
        log "No se encontró el archivo node.pid. El proceso podría no estar en ejecución."
    fi
}


# --- LÓGICA PRINCIPAL ---

# 1. IDENTIFICACIÓN DEL NODO
LOCAL_IP=$(hostname -I | awk '{print $1}')
NODE_ID=0

case ${LOCAL_IP} in
    "${IP_NODO_1}")
        NODE_ID=1
        ;;
    "${IP_NODO_2}")
        NODE_ID=2
        ;;
    "${IP_NODO_3}")
        NODE_ID=3
        ;;
    *)
        echo "ERROR: La IP local (${LOCAL_IP}) no coincide con ninguna IP configurada en el script."
        exit 1
        ;;
esac

# 2. MANEJO DE ARGUMENTOS (EJ: ./local_simulation.sh cleanup)
if [ "$1" == "cleanup" ]; then
    # Limpiamos el archivo de log para la ejecución de la limpieza.
    > "${SIMULATION_LOG_FILE}"
    log "Ejecutando limpieza para el Nodo ${NODE_ID}..."
    stop_go_process
    log "Limpieza completada."
    exit 0
fi


# 3. INICIO DE SIMULACIÓN Y LOGS
# Limpiamos el archivo de log anterior para empezar de cero en esta ejecución.
> "${SIMULATION_LOG_FILE}"
log "Script ejecutado en una máquina identificada como Nodo ${NODE_ID}."
log "Los logs de esta simulación se guardarán en '${SIMULATION_LOG_FILE}'."


# 4. LÓGICA DE SIMULACIÓN BASADA EN EL ID DEL NODO
if [ ${NODE_ID} -eq 3 ]; then
    # --- Comportamiento del Líder Inicial (Nodo 3) ---
    log "Rol de simulación: LÍDER INICIAL."
    start_go_process
    log "El nodo estará activo por 45 segundos y luego simulará una caída."
    log "Observa los logs de los otros nodos para ver la elección del nuevo líder."
    sleep 45

    log "SIMULANDO CAÍDA DEL LÍDER."
    stop_go_process
    echo "" | tee -a "${SIMULATION_LOG_FILE}" # Añade una línea en blanco para legibilidad
    log "El nodo ha sido detenido."
    log "Para simular la REINTEGRACIÓN, simplemente vuelve a ejecutar este script ('./local_simulation_file_logs.sh') en esta misma máquina."

else
    # --- Comportamiento de los Secundarios (Nodos 1 y 2) ---
    log "Rol de simulación: SECUNDARIO."
    start_go_process
    log "Este nodo permanecerá activo, esperando la caída del líder y participando en la nueva elección."
    log "La simulación se completará en este nodo. Para detener, ejecuta: ./local_simulation_file_logs.sh cleanup"
fi