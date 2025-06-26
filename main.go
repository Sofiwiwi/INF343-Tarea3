package main

import (
	"INF343-Tarea3/servernode"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"io/ioutil"
	//"sync"
	"time"
)

// ServerNodeWrapper encapsula toda la lógica y estado de un nodo.
type ServerNodeWrapper struct {
	NodeID         int
	CoordinatorMod *servernode.CoordinatorModule
	MonitorMod     *servernode.MonitorModule
	SyncMod        *servernode.SynchronizationModule
	PersistenceMod *servernode.PersistenceModule
	NodeState      *servernode.Nodo
	CurrentState   *servernode.Estado
	StopChan       chan struct{}
	NodeAddresses  map[int]string
	MonitorStarted bool
	httpClient     *http.Client 
}

// NewServerNodeWrapper crea e inicializa un nuevo nodo con todos sus módulos.
func NewServerNodeWrapper(id int, allNodeIDs []int, nodeAddresses map[int]string) *ServerNodeWrapper {
	// Carga del estado persistido del nodo (si es primario o no)
	nodeState, err := servernode.LoadNodeStateFromFile(id)
	if err != nil {
		fmt.Printf("Nodo %d: Error cargando estado del nodo: %v. Creando nuevo estado.\n", id, err)
		nodeState = &servernode.Nodo{ID: id, IsPrimary: false}
	}
	// Carga del estado replicado (el log de eventos)
	persistenceMod := servernode.NewPersistenceModule(id)
	currentState, err := persistenceMod.LoadState()
	if err != nil {
		fmt.Printf("Nodo %d: Error cargando estado replicado: %v. Inicializando vacío.\n", id, err)
		currentState = &servernode.Estado{SequenceNumber: 0, EventLog: []servernode.Evento{}}
	}
	sn := &ServerNodeWrapper{
		NodeID:         id,
		NodeState:      nodeState,
		CurrentState:   currentState,
		StopChan:       make(chan struct{}),
		PersistenceMod: persistenceMod,
		NodeAddresses:  nodeAddresses,
		httpClient: &http.Client{
			Timeout: 2 * time.Second, 
		},
	}
	// Inicialización de los módulos
	sn.CoordinatorMod = servernode.NewCoordinatorModule(id, allNodeIDs, sn.NodeState)
	sn.MonitorMod = servernode.NewMonitorModule(id, -1, sn.CoordinatorMod)
	sn.SyncMod = servernode.NewSynchronizationModule(id, sn.CoordinatorMod, sn.CurrentState, sn.NodeState, sn.PersistenceMod)
	sn.injectCommunicationFunctions()

	return sn
}

// sendAPIRequest es una función genérica para realizar llamadas HTTP a otros nodos.
func (sn *ServerNodeWrapper) sendAPIRequest(targetID int, method, endpoint string, payload interface{}) (*http.Response, error) {
	targetAddr, ok := sn.NodeAddresses[targetID]
	if !ok {
		return nil, fmt.Errorf("dirección desconocida para el nodo %d", targetID)
	}
	url := fmt.Sprintf("http://%s%s", targetAddr, endpoint)
	var reqBody []byte
	var err error
	if payload != nil {
		reqBody, err = json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("error al serializar payload: %w", err)
		}
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, fmt.Errorf("error al crear request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sender-ID", strconv.Itoa(sn.NodeID))
	return sn.httpClient.Do(req)
}

// injectCommunicationFunctions asigna las implementaciones de las funciones de comunicación a los módulos.
func (sn *ServerNodeWrapper) injectCommunicationFunctions() {
	// --- Funciones para el Módulo de Coordinación ---
	sn.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		resp, err := sn.sendAPIRequest(targetID, "POST", "/election", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		resp.Body.Close()
		return true
	}

	sn.CoordinatorMod.SendCoordinatorMessage = func(targetID, coordinatorID int) {
		payload := map[string]int{"coordinator_id": coordinatorID}
		for i := 0; i < 3; i++ { // Reintentos
			resp, err := sn.sendAPIRequest(targetID, "POST", "/coordinator", payload)
			if err == nil {
				resp.Body.Close()
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Printf("Nodo %d: Fallaron todos los intentos de enviar COORDINATOR a %d\n", sn.NodeID, targetID)
	}

	// --- Funciones para el Módulo de Monitoreo ---
	sn.MonitorMod.SendHeartbeat = func(targetID int) bool {
		resp, err := sn.sendAPIRequest(targetID, "GET", "/are-you-alive", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		resp.Body.Close()
		return true
	}

	// --- Funciones para el Módulo de Sincronización ---
	sn.SyncMod.SendRequestStateMessage = func(targetID int, payload string) {
		resp, err := sn.sendAPIRequest(targetID, "POST", "/request-state", nil)
		if err == nil {
			resp.Body.Close()
		}
	}

	sn.SyncMod.SendStateMessage = func(targetID int, state servernode.Estado) {
		resp, err := sn.sendAPIRequest(targetID, "POST", "/send-state", state)
		if err == nil {
			resp.Body.Close()
		}
	}

	sn.SyncMod.SendLogEntriesMessage = func(targetID int, entries []servernode.Evento, newSequenceNumber int) {
		payload := map[string]interface{}{"entries": entries, "sequence_number": newSequenceNumber}
		resp, err := sn.sendAPIRequest(targetID, "POST", "/send-log", payload)
		if err != nil {
			fmt.Printf("Nodo %d (Primario): [SONDA] Error de red al intentar enviar /send-log al Nodo %d. Error: %v\n", sn.NodeID, targetID, err)
		}
		if err == nil {
			resp.Body.Close()
		}
	}
}

// startApiServer inicia el servidor HTTP para escuchar las peticiones de otros nodos.
func (sn *ServerNodeWrapper) startApiServer() {
	mux := http.NewServeMux()

	// Endpoint para recibir un mensaje de elección
	mux.HandleFunc("/election", func(w http.ResponseWriter, r *http.Request) {
		senderID, _ := strconv.Atoi(r.Header.Get("X-Sender-ID"))
		sn.CoordinatorMod.HandleElectionMessage(senderID)
		w.WriteHeader(http.StatusOK)
	})

	// Endpoint para recibir el anuncio de un nuevo coordinador
	mux.HandleFunc("/coordinator", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			CoordinatorID int `json:"coordinator_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			sn.CoordinatorMod.HandleCoordinatorMessage(payload.CoordinatorID)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	// Endpoint para los heartbeats (detección de fallos)
	mux.HandleFunc("/are-you-alive", func(w http.ResponseWriter, r *http.Request) {
		senderID, _ := strconv.Atoi(r.Header.Get("X-Sender-ID"))
		fmt.Printf("Nodo %d: Recibido ARE_YOU_ALIVE de %d. Respondiendo OK.\n", sn.NodeID, senderID)
		w.WriteHeader(http.StatusOK)
	})

	// Endpoint para que un nodo solicite el estado completo
	mux.HandleFunc("/request-state", func(w http.ResponseWriter, r *http.Request) {
		senderID, _ := strconv.Atoi(r.Header.Get("X-Sender-ID"))
		sn.SyncMod.HandleRequestStateMessage(senderID)
		w.WriteHeader(http.StatusOK)
	})

	// Endpoint para recibir el estado completo del primario
	mux.HandleFunc("/send-state", func(w http.ResponseWriter, r *http.Request) {
		var state servernode.Estado
		if err := json.NewDecoder(r.Body).Decode(&state); err == nil {
			sn.SyncMod.UpdateState(&state)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	// Endpoint para recibir entradas de log del primario
	mux.HandleFunc("/send-log", func(w http.ResponseWriter, r *http.Request) {
		senderID, _ := strconv.Atoi(r.Header.Get("X-Sender-ID"))
		var payload struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			sn.SyncMod.AddLogEntries(senderID, payload.Entries, payload.SequenceNumber)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	})

	mux.HandleFunc("/reactivated", func(w http.ResponseWriter, r *http.Request) {
		if !sn.NodeState.IsPrimary {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		var payload struct {
			NodeID int `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		fmt.Printf("Nodo %d (Primario): Nodo %d se ha reincorporado. Enviando COORDINATOR y estado.\n", sn.NodeID, payload.NodeID)
		sn.CoordinatorMod.SendCoordinatorMessage(payload.NodeID, sn.NodeID)
		sn.SyncMod.SendStateMessage(payload.NodeID, *sn.CurrentState)
		w.WriteHeader(http.StatusOK)
	})


	// Endpoint para simular la llegada de un nuevo evento al primario
	mux.HandleFunc("/add-event", func(w http.ResponseWriter, r *http.Request) {
		if !sn.NodeState.IsPrimary {
			http.Error(w, "No soy el primario", http.StatusServiceUnavailable)
			return
		}
		var event servernode.Evento
		if err := json.NewDecoder(r.Body).Decode(&event); err == nil {
			sn.SyncMod.AddEvent(event)
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Cuerpo de la petición inválido", http.StatusBadRequest)
		}
	})

	listenAddr := sn.NodeAddresses[sn.NodeID]
	fmt.Printf("Nodo %d: Servidor API escuchando en http://%s\n", sn.NodeID, listenAddr)
	if err := http.ListenAndServe(listenAddr, mux); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Nodo %d: Fallo al iniciar servidor API: %v", sn.NodeID, err)
	}
}

func (sn *ServerNodeWrapper) Start() {
	fmt.Printf("Nodo %d: Iniciando...\n", sn.NodeID)
	go sn.startApiServer()

	time.Sleep(7 * time.Second)

	if !sn.NodeState.IsPrimary && sn.CoordinatorMod.PrimaryID != -1 {
		fmt.Printf("Nodo %d: Reincorporado. Solicita estado al primario %d\n", sn.NodeID, sn.CoordinatorMod.PrimaryID)
		go sn.SyncMod.ReconcileStateWithPrimary()
	}

	go func() {
		time.Sleep(2 * time.Second)
		if !sn.NodeState.IsPrimary && sn.CoordinatorMod.PrimaryID != -1 {
			fmt.Printf("Nodo %d: Notificando mi reactivación al primario %d\n", sn.NodeID, sn.CoordinatorMod.PrimaryID)
			_, err := sn.sendAPIRequest(sn.CoordinatorMod.PrimaryID, "POST", "/reactivated", map[string]int{"node_id": sn.NodeID})
			if err != nil {
				fmt.Printf("Nodo %d: Error al notificar reactivación al primario: %v\n", sn.NodeID, err)
			}
		}
	}()


	go func() {
		time.Sleep(12 * time.Second)
		sn.CoordinatorMod.ElectionLock.Lock()
		knownPrimary := sn.CoordinatorMod.PrimaryID
		sn.CoordinatorMod.ElectionLock.Unlock()

		if !sn.NodeState.IsPrimary && knownPrimary != -1 {
			fmt.Printf("Nodo %d: (Reintento) solicitando estado al primario %d\n", sn.NodeID, knownPrimary)
			sn.SyncMod.ReconcileStateWithPrimary()
		}
	}()

	go func() {
		if !sn.NodeState.IsPrimary {
			if sn.CurrentState.SequenceNumber == 0 {
				lowestNodeID := sn.CoordinatorMod.Nodes[0]
				if sn.NodeID == lowestNodeID {
					fmt.Printf("Nodo %d: Soy el de ID más bajo en un arranque desde cero. Iniciando elección del Matón...\n", sn.NodeID)
					go sn.CoordinatorMod.StartElection() // Elección estándar (solo a IDs >)
				} else {
					fmt.Printf("Nodo %d: Nodo secundario en arranque desde cero. Esperando elección...\n", sn.NodeID)
				}
			} else {
				fmt.Printf("Nodo %d: Reintegrándose al clúster. Iniciando sondeo de descubrimiento.\n", sn.NodeID)
				go sn.CoordinatorMod.DiscoverLeader() // Nueva función de descubrimiento (a TODOS)
			}
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sn.StopChan:
				return
			case <-ticker.C:
				sn.CoordinatorMod.ElectionLock.Lock()
				isPrimary := sn.NodeState.IsPrimary
				knownPrimary := sn.CoordinatorMod.PrimaryID
				sn.CoordinatorMod.ElectionLock.Unlock()

				if !isPrimary && knownPrimary != -1 {
					if !sn.MonitorStarted {
						fmt.Printf("Nodo %d: Activando monitor del primario (%d).\n", sn.NodeID, knownPrimary)
						sn.MonitorMod.Start()
						sn.MonitorStarted = true
					}
				} else if isPrimary && sn.MonitorStarted {
					fmt.Printf("Nodo %d: Soy primario, deteniendo monitor.\n", sn.NodeID)
					sn.MonitorMod.Stop()
					sn.MonitorStarted = false
				}
			}
		}
	}()

	go func() {
		eventTicker := time.NewTicker(4 * time.Second)
		defer eventTicker.Stop()

		for {
			select {
			case <-eventTicker.C:
				if sn.NodeState.IsPrimary {
					fmt.Printf("Nodo %d (Primario): Generando evento periódico...\n", sn.NodeID)
					newEvent := servernode.Evento{Value: fmt.Sprintf("Evento_simulado_del_Nodo_%d_a_las_%s", sn.NodeID, time.Now().Format("15:04:05"))}
					sn.SyncMod.AddEvent(newEvent)
				}
			case <-sn.StopChan:
				return
			}
		}
	}()

	go func() {
		if sn.NodeState.IsPrimary {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-sn.StopChan:
					return
				case <-ticker.C:
					for id := range sn.NodeAddresses {
						if id == sn.NodeID {
							continue
						}
						if sn.MonitorMod.SendHeartbeat(id) {
							fmt.Printf("Nodo %d (Primario): Nodo %d está activo. Enviando COORDINATOR + replicando estado...\n", sn.NodeID, id)
							sn.CoordinatorMod.SendCoordinatorMessage(id, sn.NodeID)
							sn.SyncMod.SendStateMessage(id, *sn.CurrentState)
						}
					}
				}
			}
		}
	}()

	<-sn.StopChan
	fmt.Printf("Nodo %d: Detenido.\n", sn.NodeID)
}

// Stop detiene el nodo de forma segura.
func (sn *ServerNodeWrapper) Stop() {
	close(sn.StopChan)
	if sn.MonitorStarted {
		sn.MonitorMod.Stop()
	}
	// Persistir estado final
	sn.NodeState.SaveNodeStateToFile()
	sn.PersistenceMod.SaveState(sn.CurrentState)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <nodeID>")
		os.Exit(1)
	}
	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Error: '%s' no es un ID de nodo válido.\n", os.Args[1])
		os.Exit(1)
	}

	// Configuración de las direcciones de los nodos
	nodeAddresses := map[int]string{
		1: "10.10.28.17:8001",
		2: "10.10.28.18:8001",
		3: "10.10.28.19:8001",
	}
	// Asegúrate de que el ID proporcionado existe en la configuración.
	if _, ok := nodeAddresses[nodeID]; !ok {
		fmt.Printf("Error: ID de nodo %d no encontrado en la configuración de direcciones.\n", nodeID)
		os.Exit(1)
	}


	allNodeIDs := make([]int, 0, len(nodeAddresses))
	for id := range nodeAddresses {
		allNodeIDs = append(allNodeIDs, id)
	}

	node := NewServerNodeWrapper(nodeID, allNodeIDs, nodeAddresses)

	pid := os.Getpid()
	pidStr := strconv.Itoa(pid)
	err = ioutil.WriteFile("node.pid", []byte(pidStr), 0644)
	if err != nil {
		log.Fatalf("Nodo %d: No se pudo escribir el archivo PID: %v", nodeID, err)
	}
	
	sigChan := make(chan os.Signal, 1)
	go func() {
		<-sigChan
		fmt.Printf("\nNodo %d: Recibida señal de interrupción, deteniendo...\n", node.NodeID)
		node.Stop()
	}()

	node.Start()
}