package servernode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"os"
	//"strconv"
	"sync"
	"time"
)

const (
	MessageTypeElection   = "ELECTION"
	MessageTypeCoordinator = "COORDINATOR"
	MessageTypeAreYouAlive = "ARE_YOU_ALIVE"
	MessageTypeOK          = "OK"
)

// Message representa un mensaje genérico entre nodos
type Message struct {
	SenderID   int
	TargetID   int // Usado para mensajes directos, 0 para broadcast (no implementado en este ejemplo específico)
	MessageType string
	Payload    string // Puede ser útil para enviar información adicional, como el ID del nuevo coordinador
}

// CoordinatorModule es la estructura para el módulo de coordinación
type CoordinatorModule struct {
	NodeID          int
	Nodes           []int // IDs de todos los nodos conocidos en el sistema (necesario para el algoritmo del matón)
	IsPrimary       bool
	PrimaryID       int
	ElectionLock    sync.Mutex // Para evitar múltiples elecciones simultáneas
	ElectionRunning bool
	NodeState       *Nodo // Referencia al estado del propio nodo
}

// NewCoordinatorModule crea una nueva instancia del módulo de coordinación
func NewCoordinatorModule(nodeID int, allNodes []int, nodeState *Nodo) *CoordinatorModule {
	// Asegúrate de que allNodes esté ordenado para facilitar la elección
	sort.Ints(allNodes)
	return &CoordinatorModule{
		NodeID:    nodeID,
		Nodes:     allNodes,
		IsPrimary: false, // Inicialmente, no es primario
		PrimaryID: -1,    // ID desconocido al inicio
		NodeState: nodeState,
	}
}

// StartElection inicia el algoritmo del matón
func (cm *CoordinatorModule) StartElection() {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	if cm.ElectionRunning {
		fmt.Printf("Nodo %d: Ya hay una elección en curso. Ignorando solicitud.\n", cm.NodeID)
		return
	}

	fmt.Printf("Nodo %d: El primario %d ha caído o no responde. Iniciando elección.\n", cm.NodeID, cm.PrimaryID)
	cm.ElectionRunning = true
	cm.IsPrimary = false 
	cm.NodeState.IsPrimary = false

	higherNodes := cm.getHigherIDNodes()

	if len(higherNodes) == 0 {
		cm.declareSelfAsCoordinator()
		return
	}

	fmt.Printf("Nodo %d: Enviando mensajes de elección a nodos con ID superior: %v\n", cm.NodeID, higherNodes)
	// Enviar mensajes de elección a todos los nodos con ID superior
	respondedOK := make(chan bool, len(higherNodes)) // Canal para esperar respuestas OK

	for _, targetID := range higherNodes {
		go func(targetID int) {
			ok := cm.SendElectionMessage(targetID) // Simula el envío y espera de respuesta
			respondedOK <- ok
		}(targetID)
	}

	// Esperar respuestas OK o un tiempo de espera
	timeout := time.After(5 * time.Second) // configurable
	okResponses := 0
	for i := 0; i < len(higherNodes); i++ {
		select {
		case ok := <-respondedOK:
			if ok {
				okResponses++
			}
		case <-timeout:
			fmt.Printf("Nodo %d: Tiempo de espera agotado para respuestas OK en la elección.\n", cm.NodeID)
			break // Salir del bucle, los que no respondieron se consideran caídos o no involucrados
		}
	}

	if okResponses == 0 {
		// Nadie con ID más alto respondió OK, este nodo es el nuevo primario
		cm.declareSelfAsCoordinator()
	} else {
		fmt.Printf("Nodo %d: Nodos con ID superior respondieron OK. Esperando mensaje de coordinador.\n", cm.NodeID)
	}
	cm.ElectionRunning = false
}

// getHigherIDNodes devuelve una lista de IDs de nodos con un ID mayor al propio.
func (cm *CoordinatorModule) getHigherIDNodes() []int {
	var higherNodes []int
	for _, id := range cm.Nodes {
		if id > cm.NodeID {
			higherNodes = append(higherNodes, id)
		}
	}
	return higherNodes
}

// HandleElectionMessage es llamado cuando el nodo recibe un mensaje de elección.
func (cm *CoordinatorModule) HandleElectionMessage(senderID int) {
	fmt.Printf("Nodo %d: Recibido mensaje de ELECCIÓN de Nodo %d.\n", cm.NodeID, senderID)

	// Responder OK al remitente (simulado)
	cm.SendOKMessage(senderID)

	// Si el nodo actual tiene un ID más alto y no está ya en una elección, inicia su propia elección.
	if cm.NodeID > senderID && !cm.ElectionRunning {
		cm.StartElection()
	}
}

// HandleCoordinatorMessage es llamado cuando el nodo recibe un mensaje de coordinador.
func (cm *CoordinatorModule) HandleCoordinatorMessage(coordinatorID int) {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d: Recibido mensaje de COORDINADOR de Nodo %d. El nuevo primario es: %d\n", cm.NodeID, coordinatorID, coordinatorID)
	cm.PrimaryID = coordinatorID
	cm.IsPrimary = (cm.NodeID == coordinatorID)
	cm.NodeState.IsPrimary = cm.IsPrimary
	cm.ElectionRunning = false // La elección ha terminado
	fmt.Printf("Nodo %d: Actualizado estado de primario a %t, PrimaryID a %d\n", cm.NodeID, cm.IsPrimary, cm.PrimaryID)
}

// declareSelfAsCoordinator se llama cuando el nodo se convierte en el coordinador.
func (cm *CoordinatorModule) declareSelfAsCoordinator() {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d: ¡Soy el nuevo primario!\n", cm.NodeID)
	cm.IsPrimary = true
	cm.PrimaryID = cm.NodeID
	cm.NodeState.IsPrimary = true
	cm.ElectionRunning = false

	// Anunciar a todos los demás nodos que es el nuevo coordinador
	fmt.Printf("Nodo %d: Anunciando que soy el nuevo coordinador a todos los nodos.\n", cm.NodeID)
	for _, targetID := range cm.Nodes {
		if targetID != cm.NodeID {
			cm.SendCoordinatorMessage(targetID, cm.NodeID) // Simula el envío
		}
	}
}

func (cm *CoordinatorModule) SendElectionMessage(targetID int) bool {
	fmt.Printf("Nodo %d: Enviando ELECCIÓN a Nodo %d\n", cm.NodeID, targetID)
	// En una implementación real, esto sería una llamada de red bloqueante o con timeout
	// Simulamos una respuesta OK
	time.Sleep(500 * time.Millisecond) // Simula latencia de red
	fmt.Printf("Nodo %d: Recibido OK de Nodo %d (simulado).\n", cm.NodeID, targetID)
	return true // Asumimos que recibe OK
}

// SendOKMessage simula el envío de un mensaje OK.
func (cm *CoordinatorModule) SendOKMessage(targetID int) {
	fmt.Printf("Nodo %d: Enviando OK a Nodo %d\n", cm.NodeID, targetID)
}

// SendCoordinatorMessage simula el envío de un mensaje de coordinador.
func (cm *CoordinatorModule) SendCoordinatorMessage(targetID, coordinatorID int) {
	fmt.Printf("Nodo %d: Enviando COORDINATOR (nuevo primario %d) a Nodo %d\n", cm.NodeID, coordinatorID, targetID)
}

// SaveNodeStateToFile guarda el estado actual del nodo en un archivo JSON.
// Esto es útil para la persistencia del estado (similar a lo que haría el módulo de persistencia).
func (n *Nodo) SaveNodeStateToFile() error {
	fileName := fmt.Sprintf("node_%d.json", n.ID)
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return fmt.Errorf("error al serializar el estado del nodo: %w", err)
	}
	err = ioutil.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("error al escribir el estado del nodo en el archivo %s: %w", fileName, err)
	}
	return nil
}

// LoadNodeStateFromFile carga el estado del nodo desde un archivo JSON.
func LoadNodeStateFromFile(nodeID int) (*Nodo, error) {
	fileName := fmt.Sprintf("node_%d.json", nodeID)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			// Si el archivo no existe, retornamos un nodo nuevo
			return &Nodo{ID: nodeID, IsPrimary: false, LastMessage: time.Now().Format(time.RFC3339)}, nil
		}
		return nil, fmt.Errorf("error al leer el archivo de estado del nodo %s: %w", fileName, err)
	}
	var node Nodo
	err = json.Unmarshal(data, &node)
	if err != nil {
		return nil, fmt.Errorf("error al deserializar el estado del nodo desde el archivo %s: %w", fileName, err)
	}
	return &node, nil
}