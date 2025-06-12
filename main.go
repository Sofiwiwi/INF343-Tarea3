package main

import (
	"INF343-Tarea3/servernode"
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type ServerNodeWrapper struct {
	NodeID            int
	CoordinatorMod    *servernode.CoordinatorModule
	MonitorMod        *servernode.MonitorModule
	SyncMod           *servernode.SynchronizationModule
	PersistenceMod    *servernode.PersistenceModule
	NodeState         *servernode.Nodo
	CurrentState      *servernode.Estado
	InboundMessages   chan servernode.Message
	StopChan          chan struct{}
	IsStopped         bool
	NodeAddresses     map[int]string
	MonitorStarted    bool
	HeartbeatResponse map[int]chan bool
	hrMutex sync.Mutex
}

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
		NodeID:            id,
		NodeState:         nodeState,
		CurrentState:      currentState,
		StopChan:          make(chan struct{}),
		IsStopped:         false,
		PersistenceMod:    persistenceMod,
		InboundMessages:   make(chan servernode.Message, 100),
		NodeAddresses:     nodeAddresses,
		HeartbeatResponse: make(map[int]chan bool),
	}

	sn.CoordinatorMod = servernode.NewCoordinatorModule(id, allNodeIDs, sn.NodeState)
	sn.MonitorMod = servernode.NewMonitorModule(id, -1, sn.CoordinatorMod)
	sn.SyncMod = servernode.NewSynchronizationModule(id, sn.CoordinatorMod, sn.CurrentState, sn.NodeState, sn.PersistenceMod)

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
		for i := 0; i < 3; i++ {
			err := SendMessageTCP(targetAddr, msg)
			if err != nil {
				fmt.Printf("Nodo %d: Intento %d - Error al enviar %s a %d (%s): %v\n", sn.NodeID, i+1, msgType, targetID, targetAddr, err)
				time.Sleep(1 * time.Second)
				continue
			}
			return true
		}
		fmt.Printf("Nodo %d: Fallaron los 3 intentos de enviar %s a %d\n", sn.NodeID, msgType, targetID)
		return false
	}

	sn.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		return sendMessageFunc(targetID, servernode.MessageTypeElection, "")
	}
	sn.CoordinatorMod.SendOKMessage = func(targetID int) {
		sendMessageFunc(targetID, servernode.MessageTypeOK, "")
	}
	sn.CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
		fmt.Printf("Nodo %d: Enviando COORDINATOR a %d\n", sn.NodeID, targetID)
		payloadData := struct {
			CoordinatorID int `json:"coordinator_id"`
		}{CoordinatorID: coordinatorID}
		payloadJSON, _ := json.Marshal(payloadData)
		ok := sendMessageFunc(targetID, servernode.MessageTypeCoordinator, string(payloadJSON))
		if !ok {
			fmt.Printf("Nodo %d: Error al enviar COORDINATOR a %d\n", sn.NodeID, targetID)
		}
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
		}{Entries: entries, SequenceNumber: newSequenceNumber}
		payloadJSON, _ := json.Marshal(payloadData)
		sendMessageFunc(targetID, servernode.MessageTypeSendLog, string(payloadJSON))
	}

	sn.MonitorMod.SendHeartbeat = func(targetID int) bool {
		sn.hrMutex.Lock()
		responseChan := make(chan bool, 1)
		sn.HeartbeatResponse[targetID] = responseChan
		sn.hrMutex.Unlock()

		sent := sendMessageFunc(targetID, servernode.MessageTypeAreYouAlive, "")
		if !sent {
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID)
			sn.hrMutex.Unlock()
			return false
		}

		select {
		case alive := <-responseChan:
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID)
			sn.hrMutex.Unlock()
			return alive
		case <-time.After(servernode.HeartbeatTimeout):
			fmt.Printf("Nodo %d: Timeout esperando respuesta de heartbeat de %d.\n", sn.NodeID, targetID)
			sn.hrMutex.Lock()
			delete(sn.HeartbeatResponse, targetID)
			sn.hrMutex.Unlock()
			return false
		}
	}

	return sn
}

func (sn *ServerNodeWrapper) processInboundMessages() {
	for {
		select {
		case msg := <-sn.InboundMessages:
			log.Printf("Nodo %d: Recibido mensaje de %d, tipo: %s\n", sn.NodeID, msg.SenderID, msg.MessageType)
			switch msg.MessageType {
			case servernode.MessageTypeElection:
				sn.CoordinatorMod.HandleElectionMessage(msg.SenderID)
			case servernode.MessageTypeOK:
				sn.hrMutex.Lock()
				if responseChan, ok := sn.HeartbeatResponse[msg.SenderID]; ok {
					select {
					case responseChan <- true:
					default:
					}
				}
				sn.hrMutex.Unlock()
			case servernode.MessageTypeCoordinator:
				var payload struct {
					CoordinatorID int `json:"coordinator_id"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &payload); err == nil {
					sn.CoordinatorMod.HandleCoordinatorMessage(payload.CoordinatorID)
				}
			case servernode.MessageTypeAreYouAlive:
				fmt.Printf("Nodo %d: Recibido ARE_YOU_ALIVE de %d. Respondiendo OK.\n", sn.NodeID, msg.SenderID)
				targetAddr := sn.NodeAddresses[msg.SenderID]
				SendMessageTCP(targetAddr, servernode.Message{
					SenderID:    sn.NodeID,
					TargetID:    msg.SenderID,
					MessageType: servernode.MessageTypeOK,
					Payload:     "",
				})
			default:
				fmt.Printf("Nodo %d: Mensaje desconocido: %s\n", sn.NodeID, msg.MessageType)
			}
		case <-sn.StopChan:
			return
		}
	}
}

func (sn *ServerNodeWrapper) Start() {
	fmt.Printf("Nodo %d: Iniciando...\n", sn.NodeID)
	sn.IsStopped = false
	go ListenForMessages(sn, sn.NodeAddresses[sn.NodeID])
	go sn.processInboundMessages()
	time.Sleep(2 * time.Second)

	if sn.NodeID == 1 && !sn.NodeState.IsPrimary {
		fmt.Printf("Nodo %d: No es primario al inicio y es el Nodo 1. Iniciando elección para convertirse en primario inicial.\n", sn.NodeID)
		sn.CoordinatorMod.StartElection()
		time.Sleep(3 * time.Second)
	}

	go func() {
		for {
			select {
			case <-sn.StopChan:
				return
			case <-time.After(2 * time.Second):
				sn.CoordinatorMod.ElectionLock.Lock()
				isPrimary := sn.NodeState.IsPrimary
				knownPrimary := sn.CoordinatorMod.PrimaryID
				sn.CoordinatorMod.ElectionLock.Unlock()

				if knownPrimary == -1 {
					fmt.Printf("Nodo %d: Aún no hay primario conocido. Esperando antes de monitorear.\n", sn.NodeID)
					continue
				}
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

	go func() {
		for {
			select {
			case <-sn.StopChan:
				return
			case <-time.After(1 * time.Second):
				sn.CoordinatorMod.ElectionLock.Lock()
				isPrimary := sn.NodeState.IsPrimary
				sn.CoordinatorMod.ElectionLock.Unlock()
				if isPrimary {
					fmt.Printf("Nodo %d: Es primario, comenzando o continuando la adición de eventos.\n", sn.NodeID)
					ticker := time.NewTicker(4 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							sn.CoordinatorMod.ElectionLock.Lock()
							currentIsPrimary := sn.NodeState.IsPrimary
							sn.CoordinatorMod.ElectionLock.Unlock()
							if currentIsPrimary {
								newEvent := servernode.Evento{Value: fmt.Sprintf("Mensaje_de_Nodo%d", sn.NodeID)}
								sn.SyncMod.AddEvent(newEvent)
							} else {
								fmt.Printf("Nodo %d: Ya no soy primario, deteniendo generación de eventos.\n", sn.NodeID)
								return
							}
						case <-sn.StopChan:
							return
						}
					}
				}
			}
		}
	}()

	<-sn.StopChan
	fmt.Printf("Nodo %d: Detenido.\n", sn.NodeID)
	sn.IsStopped = true
	if sn.MonitorMod != nil && sn.MonitorStarted {
		sn.MonitorMod.Stop()
	}
	sn.NodeState.SaveNodeStateToFile()
	sn.PersistenceMod.SaveState(sn.CurrentState)
}

func (sn *ServerNodeWrapper) Stop() {
	close(sn.StopChan)
}

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
				continue
			}
		}
		go handleConnection(conn, node.InboundMessages)
	}
}

func handleConnection(conn net.Conn, inboundMessages chan<- servernode.Message) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	decoder := json.NewDecoder(reader)
	var msg servernode.Message
	if err := decoder.Decode(&msg); err != nil {
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
		1: "10.10.28.17:8001",
		2: "10.10.28.18:8001",
		3: "10.10.28.19:8001",
	}

	allNodeIDs := []int{1, 2, 3}
	node := NewServerNodeWrapper(nodeID, allNodeIDs, nodeAddresses)
	node.Start()
}