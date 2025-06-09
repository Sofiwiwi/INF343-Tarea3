package servernode

import (
	"encoding/json"
	"fmt"
	"sync"
	//"time"
)

// Definimos nuevos tipos de mensajes para la sincronización
const (
	MessageTypeRequestState = "REQUEST_STATE"
	MessageTypeSendState    = "SEND_STATE"
	MessageTypeRequestLog   = "REQUEST_LOG"
	MessageTypeSendLog      = "SEND_LOG"
)

// SynchronizationModule es el módulo encargado de la sincronización del estado
type SynchronizationModule struct {
	NodeID        int
	Coordinator   *CoordinatorModule // Necesita acceso al coordinador para saber quién es el primario
	CurrentState  *Estado          // Referencia al estado replicado de este nodo
	NodeState     *Nodo            // Referencia al estado del propio nodo
	mu            sync.Mutex       // Mutex para proteger el estado durante la sincronización
}

// NewSynchronizationModule crea una nueva instancia del módulo de sincronización
func NewSynchronizationModule(nodeID int, coordinator *CoordinatorModule, currentState *Estado, nodeState *Nodo) *SynchronizationModule {
	return &SynchronizationModule{
		NodeID:        nodeID,
		Coordinator:   coordinator,
		CurrentState:  currentState,
		NodeState:     nodeState,
	}
}

// RequestStateFromPrimary solicita el estado completo o incremental al primario.
// Este método se llamaría cuando un nodo se reintegra.
func (sm *SynchronizationModule) RequestStateFromPrimary() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.Coordinator.PrimaryID == -1 || sm.Coordinator.PrimaryID == sm.NodeID {
		fmt.Printf("Nodo %d: No se puede solicitar estado; no hay primario conocido o soy el primario.\n", sm.NodeID)
		return
	}

	fmt.Printf("Nodo %d: Solicitando estado al primario %d. Mi secuencia actual: %d\n", sm.NodeID, sm.Coordinator.PrimaryID, sm.CurrentState.SequenceNumber)

	// Simular el envío del mensaje de solicitud de estado
	// El payload puede incluir el SequenceNumber del nodo que solicita para una sincronización incremental
	requestPayload := fmt.Sprintf("%d", sm.CurrentState.SequenceNumber) // Envía el último SequenceNumber conocido
	sm.SendRequestStateMessage(sm.Coordinator.PrimaryID, requestPayload)
}

// ApplyState aplica el estado recibido del primario.
// Esto se usa para una sincronización completa.
func (sm *SynchronizationModule) ApplyState(newState Estado) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Solo aplica el estado si el sequence number del nuevo estado es mayor
	if newState.SequenceNumber > sm.CurrentState.SequenceNumber {
		sm.CurrentState.SequenceNumber = newState.SequenceNumber
		sm.CurrentState.EventLog = newState.EventLog
		fmt.Printf("Nodo %d: Estado actualizado completamente. Nuevo SequenceNumber: %d, EventLog tamaño: %d\n",
			sm.NodeID, sm.CurrentState.SequenceNumber, len(sm.CurrentState.EventLog))
	} else {
		fmt.Printf("Nodo %d: Estado recibido con SequenceNumber %d no es más nuevo que el actual %d. Ignorando.\n",
			sm.NodeID, newState.SequenceNumber, sm.CurrentState.SequenceNumber)
	}
}

// ApplyLogEntries aplica entradas de log incrementales.
// Esto se usa para una sincronización incremental (el BONUS).
func (sm *SynchronizationModule) ApplyLogEntries(entries []Evento, newSequenceNumber int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Asegurarse de que las entradas sean realmente nuevas
	if newSequenceNumber > sm.CurrentState.SequenceNumber {
		for _, entry := range entries {
			// Evitar duplicados si las entradas ya están en el log local (puede ocurrir en reintentos)
			found := false
			for _, existingEntry := range sm.CurrentState.EventLog {
				if existingEntry.ID == entry.ID { // Asumiendo que ID de Evento es único
					found = true
					break
				}
			}
			if !found {
				sm.CurrentState.EventLog = append(sm.CurrentState.EventLog, entry)
			}
		}
		sm.CurrentState.SequenceNumber = newSequenceNumber
		fmt.Printf("Nodo %d: Estado actualizado incrementalmente. Nuevo SequenceNumber: %d, EventLog tamaño: %d\n",
			sm.NodeID, sm.CurrentState.SequenceNumber, len(sm.CurrentState.EventLog))
		// sm.PersistenceModule.SaveState(sm.CurrentState)
	} else {
		fmt.Printf("Nodo %d: Entradas de log recibidas con SequenceNumber %d no son más nuevas que el actual %d. Ignorando.\n",
			sm.NodeID, newSequenceNumber, sm.CurrentState.SequenceNumber)
	}
}

// SendRequestStateMessage simula el envío de una solicitud de estado.
func (sm *SynchronizationModule) SendRequestStateMessage(targetID int, payload string) {
	fmt.Printf("Nodo %d: Enviando REQUEST_STATE (mi secuencia: %s) a Nodo %d\n", sm.NodeID, payload, targetID)
	// En una implementación real, esto enviaría un mensaje de red al targetID
}

// SendStateMessage simula el envío del estado completo.
func (sm *SynchronizationModule) SendStateMessage(targetID int, state Estado) {
	_, err := json.Marshal(state)
	if err != nil {
		fmt.Printf("Error al serializar estado para envío: %v\n", err)
		return
	}
	fmt.Printf("Nodo %d: Enviando SEND_STATE (seq: %d) a Nodo %d\n", sm.NodeID, state.SequenceNumber, targetID)
}

// SendLogEntriesMessage simula el envío de entradas de log incrementales.
func (sm *SynchronizationModule) SendLogEntriesMessage(targetID int, entries []Evento, newSequenceNumber int) {
	payloadData := struct {
		Entries        []Evento `json:"entries"`
		SequenceNumber int      `json:"sequence_number"`
	}{
		Entries:        entries,
		SequenceNumber: newSequenceNumber,
	}
	_, err := json.Marshal(payloadData)
	if err != nil {
		fmt.Printf("Error al serializar entradas de log para envío: %v\n", err)
		return
	}
	fmt.Printf("Nodo %d: Enviando SEND_LOG (seq: %d, %d entradas) a Nodo %d\n", sm.NodeID, newSequenceNumber, len(entries), targetID)
	// En una implementación real, esto enviaría un mensaje de red al targetID
}

// --- Métodos que serían llamados por main.go ---

// HandleRequestStateMessage es llamado cuando el primario recibe una solicitud de estado.
func (sm *SynchronizationModule) HandleRequestStateMessage(requesterID int, currentSequence int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.Coordinator.IsPrimary {
		fmt.Printf("Nodo %d: Recibido REQUEST_STATE de Nodo %d, pero no soy el primario. Ignorando.\n", sm.NodeID, requesterID)
		return
	}

	fmt.Printf("Nodo %d (Primario): Recibido REQUEST_STATE de Nodo %d. Secuencia del solicitante: %d. Mi secuencia: %d\n",
		sm.NodeID, requesterID, currentSequence, sm.CurrentState.SequenceNumber)

	// Implementación del BONUS: enviar solo los logs faltantes
	if currentSequence < sm.CurrentState.SequenceNumber {
		// Encontrar las entradas de log que le faltan al nodo solicitante
		var entriesToSend []Evento
		for _, event := range sm.CurrentState.EventLog {
			if event.ID > currentSequence { // Asumiendo que el currentSequence del solicitante se mapea a IDs de evento o un índice similar
				entriesToSend = append(entriesToSend, event)
			}
		}

		if len(entriesToSend) > 0 {
			fmt.Printf("Nodo %d (Primario): Enviando %d entradas de log a Nodo %d.\n", sm.NodeID, len(entriesToSend), requesterID)
			sm.SendLogEntriesMessage(requesterID, entriesToSend, sm.CurrentState.SequenceNumber)
		} else {
			fmt.Printf("Nodo %d (Primario): Nodo %d ya tiene el estado más reciente, o no hay entradas nuevas. Enviando estado completo si es necesario.\n", sm.NodeID, requesterID)
			// Si no hay entradas nuevas o la lógica anterior no funcionó, podríamos enviar el estado completo como fallback
			sm.SendStateMessage(requesterID, *sm.CurrentState)
		}
	} else {
		fmt.Printf("Nodo %d (Primario): Enviando estado COMPLETO a Nodo %d (secuencia solicitante %d >= mi secuencia %d).\n", sm.NodeID, requesterID, currentSequence, sm.CurrentState.SequenceNumber)
		sm.SendStateMessage(requesterID, *sm.CurrentState)
	}
}

// HandleReceiveState es llamado cuando un nodo recibe un estado completo del primario.
func (sm *SynchronizationModule) HandleReceiveState(newState Estado) {
	fmt.Printf("Nodo %d: Recibido estado COMPLETO de primario (seq: %d).\n", sm.NodeID, newState.SequenceNumber)
	sm.ApplyState(newState)
	// sm.PersistenceModule.SaveState(sm.CurrentState) // Persistir el estado actualizado
}

// HandleReceiveLogEntries es llamado cuando un nodo recibe entradas de log incrementales.
func (sm *SynchronizationModule) HandleReceiveLogEntries(entries []Evento, newSequenceNumber int) {
	fmt.Printf("Nodo %d: Recibido %d entradas de log incrementales (seq: %d).\n", sm.NodeID, len(entries), newSequenceNumber)
	sm.ApplyLogEntries(entries, newSequenceNumber)
	// sm.PersistenceModule.SaveState(sm.CurrentState) // Persistir el estado actualizado
}

// --- Métodos auxiliares para los otros módulos, si necesitan actualizar el estado ---

// UpdateStateFromEvent es llamado por el primario cuando se agrega un nuevo evento.
func (sm *SynchronizationModule) UpdateStateFromEvent(event Evento) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Solo el primario actualiza el estado local de esta manera y replica
	if sm.NodeState.IsPrimary {
		sm.CurrentState.SequenceNumber++
		sm.CurrentState.EventLog = append(sm.CurrentState.EventLog, event)
		fmt.Printf("Nodo %d (Primario): Nuevo evento añadido. Seq: %d, EventID: %d\n", sm.NodeID, sm.CurrentState.SequenceNumber, event.ID)
		sm.ReplicateEventToSecondaries(event, sm.CurrentState.SequenceNumber)
	} else {
		fmt.Printf("Nodo %d: Intentó actualizar estado con evento, pero no es primario.\n", sm.NodeID)
	}
}

// ReplicateEventToSecondaries replica un evento y el nuevo número de secuencia a todos los secundarios.
func (sm *SynchronizationModule) ReplicateEventToSecondaries(event Evento, newSequence int) {
	sm.mu.Lock() // Proteger la lista de nodos si se modificara
	defer sm.mu.Unlock()

	fmt.Printf("Nodo %d (Primario): Replicando evento (ID: %d, Seq: %d) a todos los secundarios.\n", sm.NodeID, event.ID, newSequence)
	for _, targetID := range sm.Coordinator.Nodes { // Usa la lista de nodos del coordinador
		if targetID != sm.NodeID { // No enviar a sí mismo
			sm.SendLogEntriesMessage(targetID, []Evento{event}, newSequence)
		}
	}
}

func (sm *SynchronizationModule) PersistState() {
	fmt.Printf("Nodo %d: Persistiendo estado (simulado).\n", sm.NodeID)
	// currentStateAsJSON, _ := json.MarshalIndent(sm.CurrentState, "", "  ")
	// fmt.Printf("Estado persistido:\n%s\n", currentStateAsJSON)
}