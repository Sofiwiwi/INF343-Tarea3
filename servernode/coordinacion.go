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

// CoordinatorModule es la estructura para el módulo de coordinación
type CoordinatorModule struct {
	NodeID          int
	Nodes           []int // IDs de todos los nodos conocidos en el sistema (necesario para el algoritmo del matón)
	IsPrimary       bool
	PrimaryID       int
	ElectionLock    sync.Mutex // Para evitar múltiples elecciones simultáneas
	ElectionRunning bool
	NodeState       *Nodo // Referencia al estado del propio nodo
	// Funciones simuladas para enviar mensajes
	SendElectionMessage    func(targetID int) bool
	SendOKMessage          func(targetID int)
	SendCoordinatorMessage func(targetID, coordinatorID int)
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
	respondedOK := make(chan bool, len(higherNodes))

	for _, targetID := range higherNodes {
		go func(targetID int) {
			// Usar el campo de función aquí
			ok := cm.SendElectionMessage(targetID) //
			respondedOK <- ok
		}(targetID)
	}
	// Esperar respuestas de los nodos superiores
	select {
	case <-time.After(2 * time.Second): // Esperar un tiempo por las respuestas
		fmt.Printf("Nodo %d: Timeout esperando respuestas OK. Asumiendo que no hay nodos de mayor ID activos.\n", cm.NodeID)
		if !cm.ElectionRunning { // Si la elección ya terminó por otro mensaje, no hacer nada
			return
		}
		cm.declareSelfAsCoordinator()
	case ok := <-respondedOK:
		if ok {
			fmt.Printf("Nodo %d: Recibido OK de un nodo superior. Esperando mensaje de coordinador.\n", cm.NodeID)
			cm.ElectionRunning = false // Termina su rol en esta elección, espera al coordinador.
		} else {
			fmt.Printf("Nodo %d: No se recibió OK de un nodo superior (o el envío falló). Esto no debería suceder en la simulación si los nodos están activos.\n", cm.NodeID)
		}
	}
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
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()
	fmt.Printf("Nodo %d: Recibido ELECTION de Nodo %d.\n", cm.NodeID, senderID)

	if cm.NodeID > senderID {
		fmt.Printf("Nodo %d: Mi ID es mayor que %d. Respondiendo OK.\n", cm.NodeID, senderID)
		cm.SendOKMessage(senderID) 
		if !cm.ElectionRunning {
			go cm.StartElection() 
		}
	} else {
		fmt.Printf("Nodo %d: Mi ID es menor que %d. No respondo OK, solo espero el coordinador.\n", cm.NodeID, senderID)
	}
}

// HandleCoordinatorMessage es llamado cuando el nodo recibe un mensaje de coordinador.
func (cm *CoordinatorModule) HandleCoordinatorMessage(coordinatorID int) {
	cm.ElectionLock.Lock() // Proteger el estado durante la actualización del coordinador
	defer cm.ElectionLock.Unlock()
	fmt.Printf("Nodo %d: Recibido COORDINATOR de Nodo %d. El nuevo primario es %d.\n", cm.NodeID, coordinatorID, coordinatorID)
	cm.PrimaryID = coordinatorID
	cm.ElectionRunning = false 
	if cm.NodeID == coordinatorID {
		cm.IsPrimary = true
		cm.NodeState.IsPrimary = true
		fmt.Printf("Nodo %d: ¡Soy el nuevo primario!\n", cm.NodeID)
		// Persistir el estado del nodo (incluyendo IsPrimary)
		cm.NodeState.SaveNodeStateToFile()
	} else {
		cm.IsPrimary = false
		cm.NodeState.IsPrimary = false
		fmt.Printf("Nodo %d: El primario es %d.\n", cm.NodeID, coordinatorID)
		// Persistir el estado del nodo (incluyendo IsPrimary)
		cm.NodeState.SaveNodeStateToFile()
	}
}

// declareSelfAsCoordinator se llama cuando el nodo se convierte en el coordinador.
func (cm *CoordinatorModule) declareSelfAsCoordinator() {
	cm.IsPrimary = true
	cm.PrimaryID = cm.NodeID
	cm.ElectionRunning = false
	cm.NodeState.IsPrimary = true // Actualizar el estado del propio nodo
	cm.NodeState.SaveNodeStateToFile() // Persistir el estado del nodo

	fmt.Printf("Nodo %d: ¡Soy el nuevo coordinador/primario!\n", cm.NodeID)

	for _, targetID := range cm.Nodes {
		if targetID != cm.NodeID {
			cm.SendCoordinatorMessage(targetID, cm.NodeID) //
		}
	}
}

/* Funciones default (son reescritas en el main.go)
func (cm *CoordinatorModule) SendElectionMessage(targetID int) bool {
	fmt.Printf("Nodo %d: Enviando ELECCIÓN a Nodo %d\n", cm.NodeID, targetID)
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
*/

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