package main

import (
	"INF343-Tarea3/servernode"
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
	"sync"
)

// ServerNodeWrapper ahora representará un único nodo que se ejecuta en una MV
type ServerNodeWrapper struct {
	NodeID          int
	CoordinatorMod  *servernode.CoordinatorModule
	MonitorMod      *servernode.MonitorModule
	SyncMod         *servernode.SynchronizationModule
	PersistenceMod  *servernode.PersistenceModule
	NodeState       *servernode.Nodo
	CurrentState    *servernode.Estado
	InboundMessages chan servernode.Message // Canal para mensajes entrantes desde la red
	StopChan        chan struct{}
	IsStopped       bool
	NodeAddresses   map[int]string // Nuevo: Mapa de ID de nodo a IP:Puerto
	MonitorStarted  bool           // Para controlar el inicio del monitor
	// Canal para recibir respuestas de heartbeat de forma síncrona o con timeout
	HeartbeatResponse map[int]chan bool // Mapa para almacenar canales de respuesta de heartbeat por targetID
	hrMutex           sync.Mutex        // Mutex para proteger HeartbeatResponse map
}

// NewServerNodeWrapper inicializa un único nodo.
func NewServerNodeWrapper(id int, allNodeIDs []int, nodeAddresses map[int]string) *ServerNodeWrapper {
	nodeState, err := servernode.LoadNodeStateFromFile(id)
	if err != nil {
		fmt.Printf("Nodo %d: Error cargando estado del nodo: %v. Creando nuevo estado.\n", id, err)
		nodeState = &servernode.Nodo{ID: id, IsPrimary: false, LastMessage: time.Now().Format(time.RFC3339)}
	}

	persistenceMod := servernode.NewPersistenceModule(id)
	currentState, err := persistenceMod.LoadState()
	if err != nil {
		fmt.Printf("Nodo %d: Error cargando estado replicado: %v. Inicializando vacío.\n", id, err)
		currentState = &servernode.Estado{SequenceNumber: 0, EventLog: []servernode.Evento{}}
	}

	sn := &ServerNodeWrapper{
		NodeID:          id,
		NodeState:       nodeState,
		CurrentState:    currentState,
		StopChan:        make(chan struct{}),
		IsStopped:       false,
		PersistenceMod:  persistenceMod,
		InboundMessages: make(chan servernode.Message, 100), // Buffer para mensajes entrantes
		NodeAddresses:   nodeAddresses, // Asigna el mapa de direcciones
		HeartbeatResponse: make(map[int]chan bool), // Inicializa el mapa de canales
	}

	sn.CoordinatorMod = servernode.NewCoordinatorModule(id, allNodeIDs, sn.NodeState)
	sn.MonitorMod = servernode.NewMonitorModule(id, -1, sn.CoordinatorMod)
	sn.SyncMod = servernode.NewSynchronizationModule(id, sn.CoordinatorMod, sn.CurrentState, sn.NodeState, sn.PersistenceMod)

	// *** AQUÍ ASIGNAMOS LAS FUNCIONES DE ENVÍO REALES ***
	// Se define una función auxiliar para enviar mensajes TCP para no repetir código.
	sendMessageFunc := func(targetID int, msgType string, payload string) bool {
		targetAddr, ok := sn.NodeAddresses[targetID]
		if !ok {
			fmt.Printf("Nodo %d: Error: Dirección desconocida para el nodo %d\n", sn.NodeID, targetID)
			return false
		}
		msg := servernode.Message{
			SenderID:    sn.NodeID,
			TargetID:    targetID,
			MessageType: msgType,
			Payload:     payload,
		}
		// Llamada a la función TCP real
		err := SendMessageTCP(targetAddr, msg)
		if err != nil {
			fmt.Printf("Nodo %d: Error al enviar %s a %d (%s): %v\n", sn.NodeID, msgType, targetID, targetAddr, err)
			return false
		}
		return true
	}

	sn.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		return sendMessageFunc(targetID, servernode.MessageTypeElection, "")
	}
	sn.CoordinatorMod.SendOKMessage = func(targetID int) {
		sendMessageFunc(targetID, servernode.MessageTypeOK, "")
	}
	sn.CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
		payloadData := struct {
			CoordinatorID int `json:"coordinator_id"`
		}{CoordinatorID: coordinatorID}
		payloadJSON, _ := json.Marshal(payloadData)
		sendMessageFunc(targetID, servernode.MessageTypeCoordinator, string(payloadJSON))
	}
	sn.SyncMod.SendRequestStateMessage = func(targetID int, payload string) {
		sendMessageFunc(targetID, servernode.MessageTypeRequestState, payload)
	}
	sn.SyncMod.SendStateMessage = func(targetID int, state servernode.Estado) {
		payloadJSON, _ := json.Marshal(state)
		sendMessageFunc(targetID, servernode.MessageTypeSendState, string(payloadJSON))
	}
	sn.SyncMod.SendLogEntriesMessage = func(targetID int, entries []servernode.Evento, newSequenceNumber int) {
		payloadData := struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}{
			Entries:        entries,
			SequenceNumber: newSequenceNumber,
		}
		payloadJSON, _ := json.Marshal(payloadData)
		sendMessageFunc(targetID, servernode.MessageTypeSendLog, string(payloadJSON))
	}

	// Lógica de Heartbeat más robusta:
	sn.MonitorMod.SendHeartbeat = func(targetID int) bool {
		// Crear un canal de respuesta específico para este heartbeat
		sn.hrMutex.Lock()
		responseChan := make(chan bool, 1) // Buffer 1 para evitar bloqueos si la respuesta llega antes del select
		sn.HeartbeatResponse[targetID] = responseChan
		sn.hrMutex.Unlock()

		// Intentar enviar el mensaje ARE_YOU_ALIVE
		sent := sendMessageFunc(targetID, servernode.MessageTypeAreYouAlive, "")
		if !sent {
			// Si no se pudo enviar (ej. conexión rechazada), asumimos que el nodo no está vivo.
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID) // Limpiar el canal si el envío inicial falla
			sn.hrMutex.Unlock()
			return false
		}

		// Esperar por una respuesta OK con un timeout
		select {
		case alive := <-responseChan:
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID) // Limpiar el canal después de recibir la respuesta
			sn.hrMutex.Unlock()
			return alive
		case <-time.After(servernode.HeartbeatTimeout): // Usar el timeout definido en MonitorModule
			fmt.Printf("Nodo %d: Timeout esperando respuesta de heartbeat de %d.\n", sn.NodeID, targetID)
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID) // Limpiar el canal si hay timeout
			sn.hrMutex.Unlock()
			return false
		}
	}

	return sn
}

// Start ahora es el bucle principal de un único nodo
func (sn *ServerNodeWrapper) Start() {
	fmt.Printf("Nodo %d: Iniciando...\n", sn.NodeID)
	sn.IsStopped = false

	// Iniciar el listener de mensajes para este nodo en su dirección IP y puerto
	go ListenForMessages(sn, sn.NodeAddresses[sn.NodeID])

	// Procesar mensajes entrantes desde el canal
	go sn.processInboundMessages()

	// Si el nodo es el nodo 1 y no es primario al inicio, inicia la elección por defecto.
	// Esto es para asegurar que siempre haya un primario al inicio si no hay uno persistido.
	if sn.NodeID == 1 && !sn.NodeState.IsPrimary {
		fmt.Printf("Nodo %d: No es primario al inicio y es el Nodo 1. Iniciando elección para convertirse en primario inicial.\n", sn.NodeID)
		sn.CoordinatorMod.StartElection()
		// Espera un momento para que la elección se asiente
		time.Sleep(3 * time.Second)
	}

	// Goroutine para manejar el inicio/parada del monitor
	go func() {
		for {
			select {
			case <-sn.StopChan:
				return
			case <-time.After(2 * time.Second): // Revisa el estado cada 2 segundos
				// Bloquear para acceder al estado de isPrimary de forma segura
				sn.CoordinatorMod.ElectionLock.Lock()
				isPrimary := sn.NodeState.IsPrimary
				sn.CoordinatorMod.ElectionLock.Unlock()

				if !isPrimary && !sn.MonitorStarted {
					fmt.Printf("Nodo %d: No es primario y el monitor no está activo. Iniciando monitoreo.\n", sn.NodeID)
					sn.MonitorMod.Start()
					sn.MonitorStarted = true
				} else if isPrimary && sn.MonitorStarted {
					fmt.Printf("Nodo %d: Es primario y el monitor está activo. Deteniendo monitoreo.\n", sn.NodeID)
					sn.MonitorMod.Stop()
					sn.MonitorStarted = false
				}
			}
		}
	}()

	// Goroutine para la generación de eventos (solo si es primario)
	go func() {
		for {
			select {
			case <-sn.StopChan:
				return
			case <-time.After(1 * time.Second): // Revisa cada segundo si debe generar eventos
				// Bloquear para acceder al estado de isPrimary de forma segura
				sn.CoordinatorMod.ElectionLock.Lock()
				isPrimary := sn.NodeState.IsPrimary
				sn.CoordinatorMod.ElectionLock.Unlock()

				if isPrimary {
					fmt.Printf("Nodo %d: Es primario, comenzando o continuando la adición de eventos.\n", sn.NodeID)
					ticker := time.NewTicker(4 * time.Second) // Genera eventos cada 4 segundos
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							// Bloquear para acceder al estado de isPrimary de forma segura
							sn.CoordinatorMod.ElectionLock.Lock()
							currentIsPrimary := sn.NodeState.IsPrimary
							sn.CoordinatorMod.ElectionLock.Unlock()

							if currentIsPrimary { // Volver a verificar por si dejó de ser primario
								// El ID del evento ahora se generará dentro de AddEvent en SynchronizationModule
								newEvent := servernode.Evento{Value: fmt.Sprintf("Mensaje_de_Nodo%d", sn.NodeID)}
								sn.SyncMod.AddEvent(newEvent) // Llama al método AddEvent
							} else {
								fmt.Printf("Nodo %d: Ya no soy primario, deteniendo generación de eventos.\n", sn.NodeID)
								return // Salir de este bucle interno
							}
						case <-sn.StopChan:
							return
						}
					}
				}
			}
		}
	}()

	// Mantener el nodo en ejecución hasta que se detenga
	<-sn.StopChan
	fmt.Printf("Nodo %d: Detenido.\n", sn.NodeID)
	sn.IsStopped = true
	// Asegúrate de detener el monitor si estaba activo al detener el nodo
	if sn.MonitorMod != nil && sn.MonitorStarted {
		sn.MonitorMod.Stop()
	}
	sn.NodeState.SaveNodeStateToFile()
	sn.PersistenceMod.SaveState(sn.CurrentState)
}

// processInboundMessages consume mensajes del canal InboundMessages y los despacha a los módulos.
func (sn *ServerNodeWrapper) processInboundMessages() {
	for {
		select {
		case msg := <-sn.InboundMessages:
			fmt.Printf("Nodo %d: Recibido de %d, Tipo: %s (Payload: %s)\n", sn.NodeID, msg.SenderID, msg.MessageType, msg.Payload)
			switch msg.MessageType {
			case servernode.MessageTypeElection:
				sn.CoordinatorMod.HandleElectionMessage(msg.SenderID)
			case servernode.MessageTypeOK:
				// Si un nodo recibe un OK y espera un heartbeat, envía la confirmación.
				sn.hrMutex.Lock()
				if responseChan, ok := sn.HeartbeatResponse[msg.SenderID]; ok {
					select {
					case responseChan <- true: // Intentar enviar true al canal de respuesta
					default:
						// Canal ya cerrado o no esperando. No hay problema.
						// Esto puede ocurrir si el timeout ya se disparó y el canal fue cerrado.
					}
				}
				sn.hrMutex.Unlock()
				fmt.Printf("Nodo %d: Recibido OK de Nodo %d.\n", sn.NodeID, msg.SenderID)
			case servernode.MessageTypeCoordinator:
				var payloadData struct {
					CoordinatorID int `json:"coordinator_id"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &payloadData); err != nil {
					fmt.Printf("Nodo %d: Error al deserializar payload COORDINATOR: %v\n", sn.NodeID, err)
					continue
				}
				sn.CoordinatorMod.HandleCoordinatorMessage(payloadData.CoordinatorID)
			case servernode.MessageTypeAreYouAlive:
				// Si un nodo recibe un ARE_YOU_ALIVE, responde con un OK.
				fmt.Printf("Nodo %d: Recibido ARE_YOU_ALIVE de %d. Respondiendo OK.\n", sn.NodeID, msg.SenderID)
				okMsg := servernode.Message{
					SenderID:    sn.NodeID,
					TargetID:    msg.SenderID,
					MessageType: servernode.MessageTypeOK, // Usamos MessageTypeOK como respuesta de heartbeat
					Payload:     "",
				}
				targetAddr, ok := sn.NodeAddresses[msg.SenderID]
				if !ok {
					fmt.Printf("Nodo %d: Error: Dirección desconocida para el nodo %d\n", sn.NodeID, msg.SenderID)
					continue
				}
				if err := SendMessageTCP(targetAddr, okMsg); err != nil {
					fmt.Printf("Nodo %d: Error al responder OK a %d: %v\n", sn.NodeID, msg.SenderID, err)
				}
			case servernode.MessageTypeRequestState:
				// Llamada corregida para que coincida con la firma de HandleRequestStateMessage
				sn.SyncMod.HandleRequestStateMessage(msg.SenderID)
			case servernode.MessageTypeSendState:
				var newState servernode.Estado
				if err := json.Unmarshal([]byte(msg.Payload), &newState); err != nil {
					fmt.Printf("Nodo %d: Error al deserializar payload SEND_STATE: %v\n", sn.NodeID, err)
					continue
				}
				// Llamada corregida para usar UpdateState
				sn.SyncMod.UpdateState(&newState)
			case servernode.MessageTypeSendLog:
				var payloadData struct {
					Entries        []servernode.Evento `json:"entries"`
					SequenceNumber int                 `json:"sequence_number"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &payloadData); err != nil {
					fmt.Printf("Nodo %d: Error al deserializar payload SEND_LOG: %v\n", sn.NodeID, err)
					continue
				}
				sn.SyncMod.AddLogEntries(payloadData.Entries, payloadData.SequenceNumber)
			default:
				fmt.Printf("Nodo %d: Tipo de mensaje desconocido: %s\n", sn.NodeID, msg.MessageType)
			}
		case <-sn.StopChan:
			return
		}
	}
}

// Stop detiene el nodo
func (sn *ServerNodeWrapper) Stop() {
	close(sn.StopChan)
}

// SendMessageTCP envía un mensaje serializado a una dirección de red específica.
func SendMessageTCP(targetAddress string, msg servernode.Message) error {
	conn, err := net.Dial("tcp", targetAddress)
	if err != nil {
		return fmt.Errorf("fallo al conectar a %s: %w", targetAddress, err)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("fallo al codificar y enviar mensaje a %s: %w", targetAddress, err)
	}
	return nil
}

// ListenForMessages inicia un servidor TCP para escuchar mensajes entrantes.
func ListenForMessages(node *ServerNodeWrapper, listenAddress string) {
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		fmt.Printf("Nodo %d: Error al iniciar listener TCP en %s: %v\n", node.NodeID, listenAddress, err)
		return
	}
	defer listener.Close()
	fmt.Printf("Nodo %d: Escuchando mensajes TCP en %s\n", node.NodeID, listenAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-node.StopChan:
				fmt.Printf("Nodo %d: Listener detenido.\n", node.NodeID)
				return
			default:
				// Es posible que ocurran errores temporales, como conexiones canceladas.
				continue
			}
		}
		go handleConnection(conn, node.InboundMessages)
	}
}

// handleConnection lee un mensaje JSON desde una conexión y lo envía al canal de mensajes entrantes.
func handleConnection(conn net.Conn, inboundMessages chan<- servernode.Message) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)

	var msg servernode.Message
	if err := decoder.Decode(&msg); err != nil {
		// Loguear errores de lectura o decodificación, pero no terminar el programa.
		return
	}

	inboundMessages <- msg
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <nodeID>")
		os.Exit(1)
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Error: %s no es un número válido para nodeID\n", os.Args[1])
		os.Exit(1)
	}

	nodeAddresses := map[int]string{
		1: "10.10.28.17:8001", // Reemplaza con la IP real de tu MV1
		2: "10.10.28.18:8001", // Reemplaza con la IP real de tu MV2
		3: "10.10.28.19:8001", // Reemplaza con la IP real de tu MV3
	}

	allNodeIDs := []int{1, 2, 3}

	node := NewServerNodeWrapper(nodeID, allNodeIDs, nodeAddresses)
	node.Start() // Bloquea hasta que el nodo se detiene
}