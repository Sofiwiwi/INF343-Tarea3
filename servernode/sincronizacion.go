// servernode/sincronizacion.go
package servernode

import (
	// Asegurado que esté importado para la consistencia del Message struct
	"fmt"
	"sync"
	"time"
)

// Definimos nuevos tipos de mensajes para la sincronización
const (
	MessageTypeRequestState = "REQUEST_STATE"
	MessageTypeSendState    = "SEND_STATE"
	MessageTypeRequestLog   = "REQUEST_LOG"
	MessageTypeSendLog      = "SEND_LOG"
)

// Message representa un mensaje genérico entre nodos (copiado de coordinacion.go para no tener dependencia circular)
// Se añaden las json tags para consistencia con el marshalling/unmarshalling en main.go
type Message struct {
	SenderID    int    `json:"sender_id"`
	TargetID    int    `json:"target_id"`
	MessageType string `json:"message_type"`
	Payload     string `json:"payload"`
}

// SynchronizationModule es el módulo encargado de la sincronización del estado
type SynchronizationModule struct {
	NodeID            int
	Coordinator       *CoordinatorModule // Necesita acceso al coordinador para saber quién es el primario
	CurrentState      *Estado            // Referencia al estado replicado de este nodo
	NodeState         *Nodo              // Referencia al estado del propio nodo
	mu                sync.Mutex         // Mutex para proteger el estado durante la sincronización
	PersistenceModule *PersistenceModule // Referencia al módulo de persistencia

	// Funciones que deben ser inyectadas por el entorno de ejecución (main.go)
	SendRequestStateMessage func(targetID int, payload string)
	SendStateMessage        func(targetID int, state Estado)
	SendLogEntriesMessage   func(targetID int, entries []Evento, newSequenceNumber int)
}

// NewSynchronizationModule crea una nueva instancia del módulo de sincronización
func NewSynchronizationModule(nodeID int, coordinator *CoordinatorModule, currentState *Estado, nodeState *Nodo, persistence *PersistenceModule) *SynchronizationModule {
	return &SynchronizationModule{
		NodeID:            nodeID,
		Coordinator:       coordinator,
		CurrentState:      currentState,
		NodeState:         nodeState,
		PersistenceModule: persistence,
		// Las funciones de envío se asignarán en main.go
	}
}

// HandleRequestStateMessage maneja un mensaje de solicitud de estado.
// El primario debería responder con su estado completo. Los secundarios pueden redirigir o ignorar.
func (sm *SynchronizationModule) HandleRequestStateMessage(senderID int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.NodeState.IsPrimary {
		fmt.Printf("Nodo %d (Primario): Recibida solicitud de estado de %d. Enviando estado actual (Seq: %d).\n", sm.NodeID, senderID, sm.CurrentState.SequenceNumber)
		sm.SendStateMessage(senderID, *sm.CurrentState)
	} else {
		fmt.Printf("Nodo %d (Secundario): Recibida solicitud de estado de %d, pero no soy primario. Primario conocido: %d.\n", sm.NodeID, senderID, sm.Coordinator.PrimaryID)
	}
}

// UpdateState actualiza el estado replicado de este nodo con el estado recibido de otro nodo (usualmente el primario).
func (sm *SynchronizationModule) UpdateState(newState *Estado) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if newState.SequenceNumber > sm.CurrentState.SequenceNumber {
		sm.CurrentState = newState
		fmt.Printf("Nodo %d: Estado actualizado a SequenceNumber %d. Eventos: %d.\n", sm.NodeID, sm.CurrentState.SequenceNumber, len(sm.CurrentState.EventLog))
		sm.PersistenceModule.SaveState(sm.CurrentState)
	} else {
		fmt.Printf("Nodo %d: Recibido estado con SequenceNumber %d, pero el local es %d. No se actualiza.\n", sm.NodeID, newState.SequenceNumber, sm.CurrentState.SequenceNumber)
	}
}

// ReconcileStateWithPrimary solicita el estado completo al primario.
func (sm *SynchronizationModule) ReconcileStateWithPrimary() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.NodeState.IsPrimary {
		fmt.Printf("Nodo %d: No necesita reconciliar, es el primario.\n", sm.NodeID)
		return
	}

	primaryID := sm.Coordinator.PrimaryID
	if primaryID == -1 {
		fmt.Printf("Nodo %d: No hay primario conocido para reconciliar. Esperando elección.\n", sm.NodeID)
		return
	}

	fmt.Printf("Nodo %d: Solicitando estado completo al primario %d...\n", sm.NodeID, primaryID)
	// Envía un mensaje pidiendo el estado completo al primario
	sm.SendRequestStateMessage(primaryID, "")
}

// AddEvent permite al primario añadir un nuevo evento al log.
func (sm *SynchronizationModule) AddEvent(event Evento) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Solo el primario actualiza el estado local de esta manera y replica
	if sm.NodeState.IsPrimary {
		sm.CurrentState.SequenceNumber++
		sm.CurrentState.EventLog = append(sm.CurrentState.EventLog, event)
		// Corregido: \\n a \n
		fmt.Printf("Nodo %d (Primario): Nuevo evento añadido. Seq: %d, EventID: %d\n", sm.NodeID, sm.CurrentState.SequenceNumber, event.ID)

		// Persistir el estado inmediatamente después de actualizarlo
		sm.PersistenceModule.SaveState(sm.CurrentState) // <-- LLAMADA AL MÓDULO DE PERSISTENCIA

		// Después de actualizar su propio estado, el primario debería replicar esto a los secundarios.
		sm.ReplicateEventToSecondaries(event, sm.CurrentState.SequenceNumber)
	} else {
		// Corregido: \\n a \n
		fmt.Printf("Nodo %d: Intentó actualizar estado con evento, pero no es primario. Primario conocido: %d\n", sm.NodeID, sm.Coordinator.PrimaryID)
		// Opcional: Redirigir la solicitud al primario si no lo es.
	}
}

// ReplicateEventToSecondaries replica un evento y el nuevo número de secuencia a todos los secundarios.
func (sm *SynchronizationModule) ReplicateEventToSecondaries(event Evento, newSequence int) {
	sm.mu.Lock() // Proteger la lista de nodos si se modificara
	defer sm.mu.Unlock()
	fmt.Printf("Nodo %d (Primario): Replicando evento (ID: %d, Seq: %d) a todos los secundarios.\n", sm.NodeID, event.ID, newSequence)
	newEntries := []Evento{event} // Enviar solo el último evento añadido

	for _, targetID := range sm.Coordinator.Nodes { // Usa la lista de nodos del coordinador
		fmt.Printf("Nodo %d (Primario): [SONDA] Dentro del bucle de replicación. Intentando contactar al Nodo %d.\n", sm.NodeID, targetID)
		if targetID != sm.NodeID { 
			sm.SendLogEntriesMessage(targetID, newEntries, newSequence)
		}
	}
}

// AddLogEntries maneja la adición de entradas de log replicadas desde el primario.
func (sm *SynchronizationModule) AddLogEntries(senderID int, entries []Evento, sequenceNumber int) {
    sm.mu.Lock()
    defer sm.mu.Unlock()

    if sm.Coordinator.PrimaryID == -1 {
        fmt.Printf("Nodo %d: Descubierto al líder %d a través de replicación de logs.\n", sm.NodeID, senderID)
        sm.Coordinator.PrimaryID = senderID
        sm.Coordinator.PrimaryLastSeen = time.Now()
    }

    if sequenceNumber > sm.CurrentState.SequenceNumber {
        sm.CurrentState.EventLog = append(sm.CurrentState.EventLog, entries...)
        sm.CurrentState.SequenceNumber = sequenceNumber
        fmt.Printf("Nodo %d: Entradas de log añadidas. Nuevo SequenceNumber: %d. Total eventos: %d.\n", sm.NodeID, sm.CurrentState.SequenceNumber, len(sm.CurrentState.EventLog))
        sm.PersistenceModule.SaveState(sm.CurrentState)
    } else {
        fmt.Printf("Nodo %d: Ignorando entradas de log con SequenceNumber %d (local es %d).\n", sm.NodeID, sequenceNumber, sm.CurrentState.SequenceNumber)
    }
}
