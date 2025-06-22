#!/bin/bash

USER_1="ubuntu"
PASS_1="W8h7F78St81R8L1x" 
IP_1="10.10.28.17"

USER_2="ubuntu"
PASS_2="8qS0YRcKfoS9CY5e" 
IP_2="10.10.28.18"        

USER_3="ubuntu"
PASS_3="zK2D335HRiGYl7Zp" 
IP_3="10.10.28.19"      

PROJECT_PATH="/home/ubuntu/INF343-Tarea3"

# --- FUNCIONES AUXILIARES ---

# Función para imprimir logs con fecha y hora
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Función para iniciar un nodo en una VM remota
start_node() {
    local node_id=$1
    local user=$2
    local pass=$3
    local ip=$4

    log "Iniciando Nodo $node_id en ${ip}..."
    # Usamos sshpass para pasar la contraseña al comando ssh
    sshpass -p "${pass}" ssh -o StrictHostKeyChecking=no ${user}@${ip} "cd ${PROJECT_PATH} && nohup go run main.go ${node_id} > node${node_id}.log 2>&1 & echo \$! > node.pid"
    log "Nodo $node_id iniciado."
}

# Función para detener un nodo en una VM remota
stop_node() {
    local node_id=$1
    local user=$2
    local pass=$3
    local ip=$4

    log "Deteniendo Nodo $node_id en ${ip}..."
    sshpass -p "${pass}" ssh -o StrictHostKeyChecking=no ${user}@${ip} "if [ -f ${PROJECT_PATH}/node.pid ]; then kill \$(cat ${PROJECT_PATH}/node.pid) && rm ${PROJECT_PATH}/node.pid; echo 'Nodo detenido.'; else echo 'PID no encontrado, el nodo podría estar ya detenido.'; fi"
    log "Comando de detención enviado al Nodo $node_id."
}


# --- LÓGICA DE LA SIMULACIÓN ---

# Limpieza: si se ejecuta el script con "cleanup", detiene todos los nodos.
if [ "$1" == "cleanup" ]; then
    log "Iniciando limpieza de todos los nodos..."
    stop_node 1 $USER_1 $PASS_1 $IP_1
    stop_node 2 $USER_2 $PASS_2 $IP_2
    stop_node 3 $USER_3 $PASS_3 $IP_3
    log "Limpieza completada."
    exit 0
fi


log "========= INICIO DE LA SIMULACIÓN ========="

log "PASO 1: Iniciando los tres nodos del clúster."
start_node 1 $USER_1 $PASS_1 $IP_1
start_node 2 $USER_2 $PASS_2 $IP_2
start_node 3 $USER_3 $PASS_3 $IP_3
echo ""
log "Nodos iniciados. Dando 15 segundos para la elección del líder inicial..."
log "Puedes revisar los logs en cada VM ('tail -f nodeX.log') para ver el proceso."
sleep 15


echo ""
log "PASO 2: Simulando la caída del líder (Nodo 3)."
stop_node 3 $USER_3 $PASS_3 $IP_3
echo ""
log "Líder (Nodo 3) detenido. Dando 15 segundos para que los nodos 1 y 2 detecten la falla y elijan un nuevo líder..."
sleep 15

echo ""
log "PASO 3: Reintegrando al líder anterior (Nodo 3)."
start_node 3 $USER_3 $PASS_3 $IP_3
echo ""
log "Nodo 3 reiniciado. Se reintegrará al clúster como secundario y se sincronizará con el nuevo líder (Nodo 2)."
log "Dando 20 segundos para observar el comportamiento normal con el nuevo líder..."
sleep 20

echo ""
log "PASO 4: Simulación de eventos."
log "El sistema ahora está estable con el Nodo 2 como primario y los Nodos 1 y 3 como secundarios."
log "La simulación de eventos (mensajes automáticos) continúa en el primario activo, y se replica a los secundarios."
log "Puedes verificar los logs para confirmar la consistencia del número de secuencia."
echo ""
log "========= SIMULACIÓN COMPLETADA ========="
log "Para detener todos los nodos, ejecuta: ./simulation_with_password.sh cleanup"