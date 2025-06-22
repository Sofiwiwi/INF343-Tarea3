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
	httpClient     *http.Client // Cliente HTTP para la comunicación entre nodos
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
			Timeout: 2 * time.Second, // Timeout para las llamadas API
		},
	}

	// Inicialización de los módulos
	sn.CoordinatorMod = servernode.NewCoordinatorModule(id, allNodeIDs, sn.NodeState)
	sn.MonitorMod = servernode.NewMonitorModule(id, -1, sn.CoordinatorMod)
	sn.SyncMod = servernode.NewSynchronizationModule(id, sn.CoordinatorMod, sn.CurrentState, sn.NodeState, sn.PersistenceMod)

	// Inyección de dependencias de comunicación (funciones que usan el cliente HTTP)
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
	// Se añade un header para identificar al emisor, útil en los handlers
	req.Header.Set("X-Sender-ID", strconv.Itoa(sn.NodeID))

	return sn.httpClient.Do(req)
}

// injectCommunicationFunctions asigna las implementaciones de las funciones de comunicación a los módulos.
func (sn *ServerNodeWrapper) injectCommunicationFunctions() {
	// --- Funciones para el Módulo de Coordinación ---
	sn.CoordinatorMod.SendElectionMessage = func(targetID int) bool {
		resp, err := sn.sendAPIRequest(targetID, "POST", "/election", nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			// Si hay error o el status no es OK, se considera que no respondió.
			return false
		}
		resp.Body.Close()
		return true // Un 200 OK significa que el nodo superior respondió.
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
		// El payload de esta función no se usa en la nueva implementación.
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

	// Endpoint para recibir nuevas entradas del log
	mux.HandleFunc("/send-log", func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Entries        []servernode.Evento `json:"entries"`
			SequenceNumber int                 `json:"sequence_number"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err == nil {
			sn.SyncMod.AddLogEntries(payload.Entries, payload.SequenceNumber)
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
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

// Start inicia todos los procesos en segundo plano del nodo.
func (sn *ServerNodeWrapper) Start() {
	fmt.Printf("Nodo %d: Iniciando...\n", sn.NodeID)
	go sn.startApiServer() // Inicia el servidor API

	// Espera un poco para que los servidores de los otros nodos se inicien
	time.Sleep(3 * time.Second)

	// Lógica de arranque y recuperación
	go func() {
		// Si un nodo se reinicia, no sabe quién es el primario. Debe reconciliarse.
		if !sn.NodeState.IsPrimary {
			fmt.Printf("Nodo %d: Nodo secundario iniciado. Intentando encontrar al primario para sincronizar...\n", sn.NodeID)
			// Primero, intenta una elección para asegurar que haya un primario activo.
			// Si ya hay uno, la elección cederá rápidamente. Si no, se elegirá uno.
			go sn.CoordinatorMod.StartElection()
		}

		// Lógica periódica para verificar si el monitor de primario debe estar activo.
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

	// Lógica para que el primario simule y replique eventos
	go func() {
		for {
			time.Sleep(1 * time.Second) // Chequea cada segundo si se ha convertido en primario
			select {
			case <-sn.StopChan:
				return
			default:
				if sn.NodeState.IsPrimary {
					fmt.Printf("Nodo %d: Soy primario. Iniciando generación y replicación de eventos.\n", sn.NodeID)
					eventTicker := time.NewTicker(4 * time.Second)
					defer eventTicker.Stop()

					for {
						select {
						case <-eventTicker.C:
							if sn.NodeState.IsPrimary {
								// Simula un nuevo evento
								newEvent := servernode.Evento{Value: fmt.Sprintf("Evento_simulado_del_Nodo_%d_a_las_%s", sn.NodeID, time.Now().Format("15:04:05"))}
								sn.SyncMod.AddEvent(newEvent)
							} else {
								fmt.Printf("Nodo %d: Dejé de ser primario, deteniendo la generación de eventos.\n", sn.NodeID)
								return // Sale del bucle de generación de eventos
							}
						case <-sn.StopChan:
							return
						}
					}
				}
			}
		}
	}()

	<-sn.StopChan // Bloquea hasta que se llame a Stop()
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
	
	// Utiliza un canal para esperar una señal de interrupción (Ctrl+C) y detener el nodo limpiamente.
	sigChan := make(chan os.Signal, 1)
	go func() {
		<-sigChan
		fmt.Printf("\nNodo %d: Recibida señal de interrupción, deteniendo...\n", node.NodeID)
		node.Stop()
	}()

	node.Start()
}