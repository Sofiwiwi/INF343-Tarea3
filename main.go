package main

import (
	"INF343-Tarea3/servernode" // Asegúrate de que esta ruta sea correcta para tu proyecto
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

// ServerNodeWrapper simula una instancia de un ServerNode que contiene los módulos.
type ServerNodeWrapper struct {
	NodeID          int
	CoordinatorMod  *servernode.CoordinatorModule
	MonitorMod      *servernode.MonitorModule
	SyncMod         *servernode.SynchronizationModule
	PersistenceMod  *servernode.PersistenceModule // Añadir referencia al módulo de persistencia
	NodeState       *servernode.Nodo              // Utiliza servernode.Nodo
	CurrentState    *servernode.Estado            // Estado replicado
	InboundMessages chan servernode.Message
	StopChan        chan struct{}
	IsStopped       bool // Para simular la detención de un nodo
}

// NewServerNodeWrapper crea un nuevo nodo simulado
func NewServerNodeWrapper(id int, allNodeIDs []int) *ServerNodeWrapper {
	// Cargar estado persistente del propio nodo (NodeState)
	nodeState, err := servernode.LoadNodeStateFromFile(id)
	if err != nil {
		fmt.Printf("Error cargando estado del nodo %d: %v. Creando nuevo estado.\\n", id, err)
		// Si no se puede cargar, crea un nuevo estado de nodo
		nodeState = &servernode.Nodo{ID: id, IsPrimary: false, LastMessage: time.Now().Format(time.RFC3339)}
	}

	// Inicializar el módulo de persistencia para este nodo
	persistenceMod := servernode.NewPersistenceModule(id)

	// Cargar el estado replicado del log de eventos
	currentState, err := persistenceMod.LoadState()
	if err != nil {
		fmt.Printf("Error cargando estado replicado para el nodo %d: %v. Inicializando estado vacío.\\n", id, err)
		currentState = &servernode.Estado{SequenceNumber: 0, EventLog: []servernode.Evento{}}
	}

	coordinatorMod := servernode.NewCoordinatorModule(id, allNodeIDs, nodeState)
	// MonitorMod necesita el CoordinatorMod, y el PrimaryID se establece dinámicamente
	monitorMod := servernode.NewMonitorModule(id, -1, coordinatorMod) // PrimaryID inicial -1
	syncMod := servernode.NewSynchronizationModule(id, coordinatorMod, currentState, nodeState, persistenceMod)

	return &ServerNodeWrapper{
		NodeID:          id,
		CoordinatorMod:  coordinatorMod,
		MonitorMod:      monitorMod,
		SyncMod:         syncMod,
		PersistenceMod:  persistenceMod,
		NodeState:       nodeState,
		CurrentState:    currentState,
		InboundMessages: make(chan servernode.Message, 100), // Buffer para mensajes entrantes
		StopChan:        make(chan struct{}),
		IsStopped:       false,
	}
}

// Start inicia los módulos del nodo y el procesamiento de mensajes entrantes.
func (snw *ServerNodeWrapper) Start() {
	fmt.Printf("Nodo %d: Iniciando módulos...\n", snw.NodeID)

	// Iniciar monitoreo en una goroutine
	go snw.MonitorMod.Start()

	// Goroutine para procesar mensajes entrantes
	go func() {
		for {
			select {
			case msg := <-snw.InboundMessages:
				fmt.Printf("Nodo %d: Mensaje recibido: %+v\n", snw.NodeID, msg)
				snw.processMessage(msg)
			case <-snw.StopChan:
				fmt.Printf("Nodo %d: Deteniendo procesamiento de mensajes.\n", snw.NodeID)
				return
			}
		}
	}()

	// Iniciar la primera elección al arrancar si es el nodo con el ID más alto
	// Esto es una simplificación; en un sistema real, se usaría algún mecanismo de descubrimiento de nodos.
	// O el nodo de mayor ID siempre inicia la primera elección.
	if snw.NodeID == max(snw.CoordinatorMod.Nodes...) {
		fmt.Printf("Nodo %d: Soy el nodo con el ID más alto (%d), iniciando primera elección.\n", snw.NodeID, snw.NodeID)
		snw.CoordinatorMod.StartElection()
	}
}

// Stop detiene el nodo simulado.
func (snw *ServerNodeWrapper) Stop() {
	fmt.Printf("Nodo %d: Deteniendo nodo...\n", snw.NodeID)
	snw.IsStopped = true
	snw.MonitorMod.Stop()
	close(snw.StopChan)
	// Guardar estado al detener
	snw.NodeState.SaveNodeStateToFile()
	snw.PersistenceMod.SaveState(snw.CurrentState)
}

// processMessage maneja los mensajes entrantes y los dirige al módulo apropiado
func (snw *ServerNodeWrapper) processMessage(msg servernode.Message) {
	switch msg.MessageType {
	case servernode.MessageTypeElection:
		fmt.Printf("Nodo %d: Recibió mensaje ELECTION de %d\n", snw.NodeID, msg.SenderID)
		snw.CoordinatorMod.HandleElectionMessage(msg.SenderID)
	case servernode.MessageTypeOK:
		fmt.Printf("Nodo %d: Recibió mensaje OK de %d\n", snw.NodeID, msg.SenderID)
		snw.CoordinatorMod.HandleOKMessage(msg.SenderID)
	case servernode.MessageTypeCoordinator:
		fmt.Printf("Nodo %d: Recibió mensaje COORDINATOR de %d\n", snw.NodeID, msg.SenderID)
		var payloadData struct {
			CoordinatorID int `json:"coordinator_id"`
		}
		if err := json.Unmarshal([]byte(msg.Payload), &payloadData); err != nil {
			fmt.Printf("Nodo %d: Error al deserializar payload COORDINATOR: %v\n", snw.NodeID, err)
			return
		}
		snw.CoordinatorMod.HandleCoordinatorMessage(payloadData.CoordinatorID)
	case servernode.MessageTypeAreYouAlive:
		fmt.Printf("Nodo %d: Recibió mensaje ARE_YOU_ALIVE de %d. Respondiendo OK.\n", snw.NodeID, msg.SenderID)
		snw.CoordinatorMod.SendOKMessage(msg.SenderID) // Responde directamente con OK
	case servernode.MessageTypeRequestState:
		fmt.Printf("Nodo %d: Recibió mensaje REQUEST_STATE de %d. Enviando estado.\n", snw.NodeID, msg.SenderID)
		// El nodo primario debe enviar su estado completo al solicitante
		if snw.CoordinatorMod.IsPrimary {
			snw.SyncMod.SendStateMessage(msg.SenderID, *snw.CurrentState)
		} else {
			// Si no es primario, se asume que el solicitante quiere el estado del primario
			// o el solicitante puede reintentar con el primario conocido.
			fmt.Printf("Nodo %d: Solicitud de estado recibida de %d, pero no soy primario.\n", snw.NodeID, msg.SenderID)
		}
	case servernode.MessageTypeSendState:
		fmt.Printf("Nodo %d: Recibió mensaje SEND_STATE de %d. Actualizando estado.\n", snw.NodeID, msg.SenderID)
		var receivedState servernode.Estado
		if err := json.Unmarshal([]byte(msg.Payload), &receivedState); err != nil {
			fmt.Printf("Nodo %d: Error al deserializar payload SEND_STATE: %v\n", snw.NodeID, err)
			return
		}
		snw.SyncMod.UpdateState(&receivedState)
	case servernode.MessageTypeSendLog:
		fmt.Printf("Nodo %d: Recibió mensaje SEND_LOG de %d. Añadiendo entradas de log.\n", snw.NodeID, msg.SenderID)
		var payloadData struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}
		if err := json.Unmarshal([]byte(msg.Payload), &payloadData); err != nil {
			fmt.Printf("Nodo %d: Error al deserializar payload SEND_LOG: %v\n", snw.NodeID, err)
			return
		}
		snw.SyncMod.AddLogEntries(payloadData.Entries, payloadData.SequenceNumber)
	default:
		fmt.Printf("Nodo %d: Mensaje de tipo desconocido: %s\n", snw.NodeID, msg.MessageType)
	}
}

// Función auxiliar para obtener el máximo de una lista de enteros
func max(nums ...int) int {
	if len(nums) == 0 {
		return 0 // O error, dependiendo del caso de uso
	}
	m := nums[0]
	for _, v := range nums {
		if v > m {
			m = v
		}
	}
	return m
}

// SendMessageOverNetwork envía un mensaje a través de la red a un nodo objetivo.
func SendMessageOverNetwork(senderID, targetID int, messageType, payload string, nodeAddresses map[int]string) {
	targetAddr := nodeAddresses[targetID]
	if targetAddr == "" {
		fmt.Printf("Error: No se encontró la dirección para el nodo %d\n", targetID)
		return
	}

	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		fmt.Printf("Error al conectar con el nodo %d (%s): %v\n", targetID, targetAddr, err)
		return
	}
	defer conn.Close()

	msg := servernode.Message{
		SenderID:    senderID,
		TargetID:    targetID,
		MessageType: messageType,
		Payload:     payload,
	}

	jsonMsg, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("Error al serializar el mensaje: %v\n", err)
		return
	}

	// Asegúrate de que el mensaje termine con un salto de línea para que ReadBytes('\n') funcione
	_, err = conn.Write(append(jsonMsg, '\n'))
	if err != nil {
		fmt.Printf("Error al enviar el mensaje al nodo %d: %v\n", targetID, err)
	}
}

// ListenForMessages debe ser una goroutine que escuche mensajes entrantes.
func ListenForMessages(node *ServerNodeWrapper, listenAddr string) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Printf("Error al iniciar el listener en %s: %v\n", listenAddr, err)
		return
	}
	defer listener.Close()
	fmt.Printf("Nodo %d escuchando en %s\n", node.NodeID, listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Manejar el error de conexión si el listener se cierra o hay problemas de red
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				fmt.Printf("Nodo %d: Error de timeout al aceptar conexión: %v\n", node.NodeID, err)
				continue
			}
			if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" && opErr.Net == "tcp" && opErr.Err.Error() == "use of closed network connection" {
				// El listener fue cerrado intencionalmente
				fmt.Printf("Nodo %d: Listener cerrado.\n", node.NodeID)
				return
			}
			fmt.Printf("Nodo %d: Error al aceptar conexión: %v\n", node.NodeID, err)
			continue
		}
		go handleConnection(conn, node)
	}
}

func handleConnection(conn net.Conn, node *ServerNodeWrapper) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Leer hasta el delimitador de nueva línea
	messageBytes, err := reader.ReadBytes('\n')
	if err != nil {
		fmt.Printf("Nodo %d: Error al leer del cliente %s: %v\n", node.NodeID, conn.RemoteAddr(), err)
		return
	}

	var msg servernode.Message
	err = json.Unmarshal(messageBytes, &msg)
	if err != nil {
		fmt.Printf("Nodo %d: Error al deserializar el mensaje de %s (%s): %v\n", node.NodeID, conn.RemoteAddr(), string(messageBytes), err)
		return
	}

	// Enviar el mensaje al canal de entrada del nodo para procesamiento asíncrono
	select {
	case node.InboundMessages <- msg:
		// Mensaje enviado al canal
	default:
		fmt.Printf("Nodo %d: Canal de mensajes lleno, descartando mensaje de %s\n", node.NodeID, msg.SenderID)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <node_id>")
		fmt.Println(" o: ./server_node <node_id>")
		return
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("El ID del nodo debe ser un número entero.")
		return
	}

	// Definimos las IPs y puertos de todos los nodos.
	// Esto se debe mantener consistente en todas las instancias de main.go
	nodeAddresses := map[int]string{
		1: "10.10.28.17:8001",
		2: "10.10.28.18:8001",
		3: "10.10.28.19:8001",
	}

	nodeIDs := []int{1, 2, 3} // Todos los IDs de los nodos.

	// Verifica que el nodeID proporcionado sea válido
	if _, ok := nodeAddresses[nodeID]; !ok {
		fmt.Printf("Node ID %d no es válido. Debe ser 1, 2 o 3.\n", nodeID)
		return
	}

	// Inicialización del nodo actual
	node := NewServerNodeWrapper(nodeID, nodeIDs)

	// Redefinir las funciones de envío de mensajes de los módulos para usar la red real
	node.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		fmt.Printf("Nodo %d: Enviando ELECTION a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeElection, "", nodeAddresses)
		// Para una elección real, necesitarías esperar una respuesta 'OK' en un tiempo límite.
		// Aquí, para simplificar, asumimos que el envío es "exitoso" para el propósito del algoritmo.
		// El módulo de coordinación manejará los timeouts si no recibe OK.
		return true // Asume que el mensaje fue intentado enviar
	}

	node.CoordinatorMod.SendOKMessage = func(targetID int) {
		fmt.Printf("Nodo %d: Enviando OK a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeOK, "", nodeAddresses)
	}

	node.CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
		payloadData := struct {
			CoordinatorID int `json:"coordinator_id"`
		}{
			CoordinatorID: coordinatorID,
		}
		payloadJSON, _ := json.Marshal(payloadData)
		fmt.Printf("Nodo %d: Enviando COORDINATOR (nuevo primario %d) a Nodo %d\n", node.NodeID, coordinatorID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeCoordinator, string(payloadJSON), nodeAddresses)
	}

	node.SyncMod.SendRequestStateMessage = func(targetID int, payload string) {
		fmt.Printf("Nodo %d: Enviando REQUEST_STATE a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeRequestState, payload, nodeAddresses)
	}

	node.SyncMod.SendStateMessage = func(targetID int, state servernode.Estado) {
		stateJSON, _ := json.Marshal(state)
		fmt.Printf("Nodo %d: Enviando SEND_STATE a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeSendState, string(stateJSON), nodeAddresses)
	}

	node.SyncMod.SendLogEntriesMessage = func(targetID int, entries []servernode.Evento, newSequenceNumber int) {
		payloadData := struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}{
			Entries:        entries,
			SequenceNumber: newSequenceNumber,
		}
		payloadJSON, _ := json.Marshal(payloadData)
		fmt.Printf("Nodo %d: Enviando SEND_LOG a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeSendLog, string(payloadJSON), nodeAddresses)
	}

	node.MonitorMod.SendHeartbeat = func(targetID int) bool {
		fmt.Printf("Nodo %d: Enviando ARE_YOU_ALIVE a Nodo %d\n", node.NodeID, targetID)
		SendMessageOverNetwork(node.NodeID, targetID, servernode.MessageTypeAreYouAlive, "", nodeAddresses)
		// Para un heartbeat real, esperarías una respuesta "OK" en un tiempo límite.
		// La lógica de `MonitorModule` ya espera esta respuesta indirectamente (si no hay OK, asume caído).
		return true // Asume que el intento de envío es exitoso.
	}

	// Iniciar el listener de mensajes para este nodo en su dirección IP y puerto
	go ListenForMessages(node, nodeAddresses[nodeID])

	// Iniciar los módulos del nodo
	node.Start()

	// --- Lógica de la simulación principal (para el nodo primario) ---
	// Esta parte de la lógica es solo para simular la adición de eventos en el primario.
	// En un sistema real, un cliente externo o una aplicación dispararía estas acciones.
	if nodeID == 1 { // Suponemos que el nodo 1 es el primario inicial y el que añade eventos.
		fmt.Println("\n--- Nodo 1 (Primario Inicial): Esperando a que el sistema se estabilice para añadir eventos ---")
		time.Sleep(10 * time.Second) // Dar tiempo para que los nodos arranquen y elijan primario

		fmt.Println("\n--- Nodo 1 (Posible Primario): Agregando eventos ---")
		node.SyncMod.AddEvent(servernode.Evento{ID: 1, Value: "primer evento"})
		time.Sleep(1 * time.Second)
		node.SyncMod.AddEvent(servernode.Evento{ID: 2, Value: "segundo evento"})
		time.Sleep(1 * time.Second)
		node.SyncMod.AddEvent(servernode.Evento{ID: 3, Value: "tercer evento"})
	}

	// Mantener el nodo corriendo indefinidamente
	select {}
}
