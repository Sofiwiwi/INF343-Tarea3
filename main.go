package main

import (
	"INF343-Tarea3/servernode"
	"fmt"
	//"os"
	"strconv"
	"time"
	"encoding/json"
)

// ServerNodeWrapper simula una instancia de un ServerNode que contiene los módulos.
type ServerNodeWrapper struct {
	NodeID          int
	CoordinatorMod  *servernode.CoordinatorModule
	MonitorMod      *servernode.MonitorModule
	SyncMod         *servernode.SynchronizationModule
	PersistenceMod  *servernode.PersistenceModule // Añadir referencia al módulo de persistencia
	NodeState       *servernode.Nodo             // Utiliza servernode.Nodo
	CurrentState    *servernode.Estado           // Estado replicado
	InboundMessages chan servernode.Message
	StopChan        chan struct{}
	IsStopped       bool // Para simular la detención de un nodo
}

// NewServerNodeWrapper crea un nuevo nodo simulado
func NewServerNodeWrapper(id int, allNodeIDs []int) *ServerNodeWrapper {
	// Cargar estado persistente del propio nodo (NodeState)
	// Esta función está en coordinacion.go
	nodeState, err := servernode.LoadNodeStateFromFile(id)
	if err != nil {
		fmt.Printf("Error cargando estado del nodo %d: %v. Creando nuevo estado.\n", id, err)
		nodeState = &servernode.Nodo{ID: id, IsPrimary: false, LastMessage: time.Now().Format(time.RFC3339)}
	}

	// Inicializar el módulo de persistencia para este nodo
	persistenceMod := servernode.NewPersistenceModule(id)

	// Cargar estado replicado (o inicializarlo si no existe) usando el PersistenceModule
	currentState, err := persistenceMod.LoadState()
	if err != nil {
		fmt.Printf("Error cargando estado replicado para nodo %d: %v. Inicializando vacío.\n", id, err)
		// Si LoadState ya maneja os.IsNotExist y retorna un estado vacío, esta rama podría ser redundante.
		// Asegúrate de que LoadState siempre retorne un *Estado válido, incluso si es nuevo.
		currentState = &servernode.Estado{SequenceNumber: 0, EventLog: []servernode.Evento{}}
	}


	sn := &ServerNodeWrapper{
		NodeID:          id,
		NodeState:       nodeState,
		CurrentState:    currentState, // Usar el estado cargado/inicializado por PersistenceMod
		InboundMessages: make(chan servernode.Message, 100),
		StopChan:        make(chan struct{}),
		IsStopped:       false,
		PersistenceMod:  persistenceMod, // Asignar el módulo de persistencia
	}

	// Crear módulos
	sn.CoordinatorMod = servernode.NewCoordinatorModule(id, allNodeIDs, sn.NodeState)
	// El PrimaryID inicial para el monitoreo debería venir del coordinador si ya lo sabe, o ser -1.
	// Aquí asumimos que el coordinador aún no sabe quién es el primario al inicio, lo determinará la elección.
	sn.MonitorMod = servernode.NewMonitorModule(id, -1, sn.CoordinatorMod)

	// CORRECCIÓN del ERROR 1: Pasar los 5 argumentos en el orden correcto a NewSynchronizationModule
	sn.SyncMod = servernode.NewSynchronizationModule(id, sn.CoordinatorMod, sn.CurrentState, sn.NodeState, sn.PersistenceMod)

	return sn
}

// Start simula el bucle principal de un nodo
func (sn *ServerNodeWrapper) Start(nodesMap map[int]*ServerNodeWrapper) {
	fmt.Printf("Nodo %d: Iniciando...\n", sn.NodeID)
	sn.IsStopped = false

	// Iniciar monitoreo si no es el primario inicial
	// El primario no se monitorea a sí mismo. Los secundarios sí.
	if !sn.NodeState.IsPrimary {
		sn.MonitorMod.Start()
	}


	go func() {
		for {
			select {
			case msg := <-sn.InboundMessages:
				if sn.IsStopped { // Si el nodo está detenido, no procesa mensajes
					continue
				}
				fmt.Printf("Nodo %d: Recibido mensaje tipo '%s' de Nodo %d\n", sn.NodeID, msg.MessageType, msg.SenderID)
				switch msg.MessageType {
				case servernode.MessageTypeElection:
					sn.CoordinatorMod.HandleElectionMessage(msg.SenderID)
				case servernode.MessageTypeOK:
					// Este OK podría ser para una elección o un heartbeat.
					// Aquí, lo manejamos genéricamente para elecciones o simplemente se loguea.
					fmt.Printf("Nodo %d: Recibido OK de Nodo %d.\n", sn.NodeID, msg.SenderID)
				case servernode.MessageTypeCoordinator:
					coordinatorID, _ := strconv.Atoi(msg.Payload)
					sn.CoordinatorMod.HandleCoordinatorMessage(coordinatorID)
					// Una vez que se sabe quién es el primario, si yo no soy el primario, solicito sincronización.
					if sn.NodeID != sn.CoordinatorMod.PrimaryID { // Si no soy el primario asignado
						// Espera un poco para que todos los nodos actualicen su PrimaryID
						time.Sleep(500 * time.Millisecond) // Simular latencia de propagación de mensaje
						sn.SyncMod.RequestStateFromPrimary()
					}

				case servernode.MessageTypeAreYouAlive: // Mensaje de heartbeat
					// El módulo de monitoreo del nodo receptor debería manejar esto y responder OK
					// Para la simulación, respondemos directamente aquí.
					go func() {
						if nodesMap[msg.SenderID] != nil && !nodesMap[msg.SenderID].IsStopped {
							SimulateSendMessage(sn.NodeID, msg.SenderID, servernode.MessageTypeOK, "", nodesMap)
						}
					}()
				case servernode.MessageTypeRequestState: // Solicitud de estado
					requesterSequence, _ := strconv.Atoi(msg.Payload)
					sn.SyncMod.HandleRequestStateMessage(msg.SenderID, requesterSequence)
				case servernode.MessageTypeSendState: // Envío de estado completo
					var newState servernode.Estado
					if err := json.Unmarshal([]byte(msg.Payload), &newState); err != nil {
						fmt.Printf("Nodo %d: Error al deserializar estado recibido: %v\n", sn.NodeID, err)
					} else {
						sn.SyncMod.HandleReceiveState(newState)
					}
				case servernode.MessageTypeSendLog: // Envío de log incremental (BONUS)
					var payloadData struct {
						Entries        []servernode.Evento `json:"entries"`
						SequenceNumber int                 `json:"sequence_number"`
					}
					if err := json.Unmarshal([]byte(msg.Payload), &payloadData); err != nil {
						fmt.Printf("Nodo %d: Error al deserializar entradas de log recibidas: %v\n", sn.NodeID, err)
					} else {
						sn.SyncMod.HandleReceiveLogEntries(payloadData.Entries, payloadData.SequenceNumber)
					}
				}
			case <-sn.StopChan:
				fmt.Printf("Nodo %d: Detenido.\n", sn.NodeID)
				sn.IsStopped = true
				if sn.MonitorMod != nil {
					sn.MonitorMod.Stop() // Detener el monitoreo al detener el nodo
				}
				// Persistir el estado del propio nodo (NodeState) y el estado replicado (CurrentState) antes de detenerse
				sn.NodeState.SaveNodeStateToFile() // Guarda el 'Node' struct
				sn.PersistenceMod.SaveState(sn.CurrentState) // Guarda el 'Estado' replicado
				return
			}
		}
	}()

	// Simulación de un primario inicial
	// Si el nodo 1 es el primario inicial, no inicia el monitoreo.
	if sn.NodeID == 1 {
		sn.CoordinatorMod.IsPrimary = true
		sn.CoordinatorMod.PrimaryID = sn.NodeID
		sn.NodeState.IsPrimary = true
		fmt.Printf("Nodo %d: Inicialmente es primario.\n", sn.NodeID)
		sn.MonitorMod.Stop() // El primario no necesita monitorearse a sí mismo
	}

	// Simulación de adición de eventos al primario
	if sn.NodeState.IsPrimary {
		go func() {
			ticker := time.NewTicker(4 * time.Second)
			defer ticker.Stop()
			// El contador de eventos debe comenzar desde el último SequenceNumber cargado/existente
			eventCounter := sn.CurrentState.SequenceNumber
			for {
				select {
				case <-ticker.C:
					// Solo si este nodo es realmente el primario en este momento
					if sn.NodeState.IsPrimary {
						eventCounter++
						newEvent := servernode.Evento{ID: eventCounter, Value: fmt.Sprintf("Mensaje_%d_de_Nodo%d", eventCounter, sn.NodeID)}
						// CORRECCIÓN del ERROR 2: Usar UpdateStateFromEvent
						sn.SyncMod.UpdateStateFromEvent(newEvent) // El primario actualiza su estado y lo replica (y persiste a través de SyncMod)
					}
				case <-sn.StopChan:
					return
				}
			}
		}()
	}

	// Esto es solo para que el goroutine principal no termine
	<-sn.StopChan
}

// Stop detiene el nodo
func (sn *ServerNodeWrapper) Stop() {
	close(sn.StopChan)
	sn.IsStopped = true
}

// SimulateSendMessage es una función para simular el envío de mensajes entre nodos.
// Es una abstracción para el main para dirigir los mensajes.
func SimulateSendMessage(senderID int, targetID int, messageType string, payload string, nodes map[int]*ServerNodeWrapper) {
	msg := servernode.Message{ // Asegúrate de que Message se defina en servernode/coordinacion.go o servernode/sincronizacion.go
		SenderID:   senderID,
		TargetID:   targetID,
		MessageType: messageType,
		Payload:    payload,
	}

	if targetID == 0 { // Broadcast (considerar si es necesario para tu tarea)
		for _, node := range nodes {
			if node.NodeID != senderID && !node.IsStopped {
				node.InboundMessages <- msg
			}
		}
	} else if node, ok := nodes[targetID]; ok {
		if !node.IsStopped {
			node.InboundMessages <- msg
		} else {
			fmt.Printf("Simulación: Mensaje de %d a %d (tipo %s) no entregado, nodo destino está detenido.\n", senderID, targetID, messageType)
		}
	} else {
		fmt.Printf("Error: Nodo destino %d no encontrado para el mensaje de %d.\n", targetID, senderID)
	}
}

func main() {
	// Para ejecutar, usar: go run main.go
	// No se necesita <nodeID> como argumento de línea de comandos en esta simulación centralizada.
	// Los nodos se inicializan en el main.
	nodeIDs := []int{1, 2, 3}
	nodes := make(map[int]*ServerNodeWrapper)

	// Inicializar y iniciar todos los nodos
	for _, id := range nodeIDs {
		nodes[id] = NewServerNodeWrapper(id, nodeIDs)
		go nodes[id].Start(nodes)
	}

	// Sobrescribir los métodos de envío simulados en los módulos de cada nodo
	for _, node := range nodes {
		node.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeElection, "", nodes)
			time.Sleep(50 * time.Millisecond) // Pequeño delay para simular latencia
			return true
		}

		node.CoordinatorMod.SendOKMessage = func(targetID int) {
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeOK, "", nodes)
		}

		node.CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeCoordinator, strconv.Itoa(coordinatorID), nodes)
		}

		// Sobrescribir métodos de sincronización
		node.SyncMod.SendRequestStateMessage = func(targetID int, payload string) {
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeRequestState, payload, nodes)
		}
		node.SyncMod.SendStateMessage = func(targetID int, state servernode.Estado) {
			stateJSON, _ := json.Marshal(state)
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeSendState, string(stateJSON), nodes)
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
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeSendLog, string(payloadJSON), nodes)
		}

		// Sobrescribir métodos de monitoreo (SendHeartbeat)
		node.MonitorMod.SendHeartbeat = func(targetID int) bool {
			SimulateSendMessage(node.NodeID, targetID, servernode.MessageTypeAreYouAlive, "", nodes)
			time.Sleep(100 * time.Millisecond) // Dar tiempo para que el mensaje se procese
			if targetNode, ok := nodes[targetID]; ok && !targetNode.IsStopped {
				return true // Simula que el primario está vivo
			}
			return false // Simula que el primario ha caído
		}
	}

	time.Sleep(2 * time.Second)
	fmt.Println("-------------------------------------------")
	fmt.Printf("Estado inicial: Nodo 1 es el primario.\n")
	fmt.Println("-------------------------------------------")

	time.Sleep(5 * time.Second) // Dar tiempo para que el primario genere eventos y los replique

	// Simular la caída del primario (Nodo 1)
	fmt.Println("\n--- Simulando caída del Nodo 1 (primario) ---")
	nodes[1].Stop() // Detener el nodo 1
	time.Sleep(1 * time.Second)

	fmt.Println("\n--- Nodo 2 debería detectar la caída e iniciar elección ---")
	time.Sleep(10 * time.Second) // Dar tiempo para que la elección y sincronización se completen

	fmt.Println("\n--- Estado final de los nodos activos ---")
	for _, id := range nodeIDs {
		if node, ok := nodes[id]; ok && !node.IsStopped {
			fmt.Printf("Nodo %d: Es primario = %t, Primario conocido = %d, Secuencia: %d, Eventos: %d\n",
				node.NodeID, node.NodeState.IsPrimary, node.CoordinatorMod.PrimaryID, node.CurrentState.SequenceNumber, len(node.CurrentState.EventLog))
		}
	}

	// Simular la reintegración del Nodo 1
	fmt.Println("\n--- Simulando reintegración del Nodo 1 ---")
	nodes[1] = NewServerNodeWrapper(1, nodeIDs) // Crear una nueva instancia para simular re-inicio
	go nodes[1].Start(nodes)

	// Volver a sobrescribir los métodos de envío para el nodo 1 recién reintegrado
	nodes[1].CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeElection, "", nodes)
		time.Sleep(50 * time.Millisecond)
		return true
	}
	nodes[1].CoordinatorMod.SendOKMessage = func(targetID int) {
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeOK, "", nodes)
	}
	nodes[1].CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeCoordinator, strconv.Itoa(coordinatorID), nodes)
	}
	nodes[1].SyncMod.SendRequestStateMessage = func(targetID int, payload string) {
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeRequestState, payload, nodes)
	}
	nodes[1].SyncMod.SendStateMessage = func(targetID int, state servernode.Estado) {
		stateJSON, _ := json.Marshal(state)
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeSendState, string(stateJSON), nodes)
	}
	nodes[1].SyncMod.SendLogEntriesMessage = func(targetID int, entries []servernode.Evento, newSequenceNumber int) {
		payloadData := struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}{
			Entries:        entries,
			SequenceNumber: newSequenceNumber,
		}
		payloadJSON, _ := json.Marshal(payloadData)
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeSendLog, string(payloadJSON), nodes)
	}
	nodes[1].MonitorMod.SendHeartbeat = func(targetID int) bool {
		SimulateSendMessage(nodes[1].NodeID, targetID, servernode.MessageTypeAreYouAlive, "", nodes)
		time.Sleep(100 * time.Millisecond)
		if targetNode, ok := nodes[targetID]; ok && !targetNode.IsStopped {
			return true
		}
		return false
	}


	time.Sleep(5 * time.Second) // Dar tiempo para que el nodo 1 se reintegre y sincronice

	fmt.Println("\n--- Estado de los nodos después de la reintegración del Nodo 1 ---")
	for _, id := range nodeIDs {
		if node, ok := nodes[id]; ok && !node.IsStopped {
			fmt.Printf("Nodo %d: Es primario = %t, Primario conocido = %d, Secuencia: %d, Eventos: %d\n",
				node.NodeID, node.NodeState.IsPrimary, node.CoordinatorMod.PrimaryID, node.CurrentState.SequenceNumber, len(node.CurrentState.EventLog))
		}
	}

	time.Sleep(5 * time.Second) // Mantener la aplicación en ejecución para ver logs

	// Detener todos los nodos al final
	fmt.Println("\n--- Deteniendo todos los nodos ---")
	for _, node := range nodes {
		node.Stop()
	}
}