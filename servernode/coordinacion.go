package servernode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	MessageTypeElection    = "ELECTION"
	MessageTypeCoordinator = "COORDINATOR"
	MessageTypeAreYouAlive = "ARE_YOU_ALIVE"
	// MessageTypeOK ya no es necesario, la respuesta HTTP 200 lo reemplaza.
)

// CoordinatorModule es la estructura para el módulo de coordinación
type CoordinatorModule struct {
	NodeID          int
	Nodes           []int
	IsPrimary       bool
	PrimaryID       int
	ElectionLock    sync.Mutex
	ElectionRunning bool
	NodeState       *Nodo
	PrimaryLastSeen time.Time
	PrimaryTimeout  time.Duration
	LastElectionTime time.Time

	// Funciones inyectadas desde main.go para la comunicación
	SendElectionMessage    func(targetID int) bool
	SendCoordinatorMessage func(targetID, coordinatorID int)
}

// NewCoordinatorModule crea una nueva instancia del módulo de coordinación
func NewCoordinatorModule(nodeID int, allNodes []int, nodeState *Nodo) *CoordinatorModule {
	sort.Ints(allNodes)
	cm := &CoordinatorModule{
		NodeID:         nodeID,
		Nodes:          allNodes,
		IsPrimary:      nodeState.IsPrimary,
		PrimaryID:      -1,
		NodeState:      nodeState,
		PrimaryTimeout: 5 * time.Second,
		PrimaryLastSeen:  time.Now(),
		LastElectionTime: time.Unix(0, 0),
	}
	if nodeState.IsPrimary {
		cm.IsPrimary = true
		cm.PrimaryID = nodeID
		fmt.Printf("Nodo %d: Cargado como primario desde persistencia.\n", nodeID)
	}
	return cm
}

// StartElection inicia el algoritmo del matón.
func (cm *CoordinatorModule) StartElection() {
	cm.ElectionLock.Lock()

	if cm.ElectionRunning {
		fmt.Printf("Nodo %d: Elección ya en curso. No se inicia una nueva.\n", cm.NodeID)
		cm.ElectionLock.Unlock()
		return
	}
	if time.Since(cm.LastElectionTime) < 3*time.Second {
		fmt.Printf("Nodo %d: Elección reciente. Esperando antes de iniciar otra.\n", cm.NodeID)
		cm.ElectionLock.Unlock()
		return
	}

	cm.ElectionRunning = true
	cm.PrimaryID = -1
	cm.LastElectionTime = time.Now()
	fmt.Printf("Nodo %d: Iniciando elección...\n", cm.NodeID)
	cm.ElectionLock.Unlock()

	higherNodesResponded := false
	for _, targetID := range cm.Nodes {
		if targetID > cm.NodeID {
			fmt.Printf("Nodo %d: Enviando ELECTION a %d\n", cm.NodeID, targetID)
			// SendElectionMessage ahora es síncrono. Si devuelve 'true',
			// un nodo superior ha respondido y tomará el control.
			if cm.SendElectionMessage(targetID) {
				fmt.Printf("Nodo %d: Nodo con ID más alto (%d) respondió. Cediendo elección.\n", cm.NodeID, targetID)
				higherNodesResponded = true
				break
			}
		}
	}

	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	// Es importante verificar si la elección sigue siendo "nuestra"
	if !cm.ElectionRunning {
		fmt.Printf("Nodo %d: La elección fue cedida o cancelada. No se tomarán más acciones.\n", cm.NodeID)
		return
	}

	if higherNodesResponded {
		cm.ElectionRunning = false // Se cede la elección
		fmt.Printf("Nodo %d: Esperando mensaje COORDINATOR del nuevo líder.\n", cm.NodeID)
		// Se establece un temporizador. Si no se anuncia un nuevo coordinador, se inicia otra elección.
		go func() {
			time.Sleep(7 * time.Second) // Tiempo de espera para el anuncio del coordinador
			cm.ElectionLock.Lock()
			defer cm.ElectionLock.Unlock()
			if cm.PrimaryID == -1 {
				fmt.Printf("Nodo %d: No se recibió mensaje COORDINATOR a tiempo. Reiniciando elección.\n", cm.NodeID)
				go cm.StartElection() // Usar go para no bloquear
			}
		}()
	} else {
		// Nadie con ID más alto respondió, este nodo es el nuevo primario.
		fmt.Printf("Nodo %d: ¡Soy el nuevo primario!\n", cm.NodeID)
		cm.IsPrimary = true
		cm.PrimaryID = cm.NodeID
		cm.NodeState.IsPrimary = true
		cm.NodeState.SaveNodeStateToFile()
		cm.ElectionRunning = false
		cm.AnnounceCoordinator()
	}
}

// AnnounceCoordinator anuncia el nuevo primario a todos los demás nodos.
func (cm *CoordinatorModule) AnnounceCoordinator() {
	fmt.Printf("Nodo %d (Primario): Anunciando que soy el primario a todos los demás nodos.\n", cm.NodeID)
	for _, targetID := range cm.Nodes {
		if targetID != cm.NodeID {
			cm.SendCoordinatorMessage(targetID, cm.NodeID)
		}
	}
	cm.PrimaryLastSeen = time.Now()
}


func (cm *CoordinatorModule) HandleElectionMessage(senderID int) {
    fmt.Printf("Nodo %d: Recibió ELECTION de %d.\n", cm.NodeID, senderID)

    cm.ElectionLock.Lock()
    defer cm.ElectionLock.Unlock()

    // --- LÓGICA DE DEFENSA DEL LÍDER ---
    if cm.IsPrimary {
        fmt.Printf("Nodo %d (Primario): Ignorando elección de %d y reafirmando mi rol.\n", cm.NodeID, senderID)
        // Le enviamos un mensaje COORDINATOR al iniciador para que sepa quién manda.
        cm.SendCoordinatorMessage(senderID, cm.NodeID)
        return
    }

    if cm.NodeID > senderID {
        if !cm.ElectionRunning {
            fmt.Printf("Nodo %d: Soy más alto que %d. Iniciando mi propia elección.\n", cm.NodeID, senderID)
            go cm.StartElection()
        } else {
            fmt.Printf("Nodo %d: Soy más alto que %d, pero ya hay una elección en curso.\n", cm.NodeID, senderID)
        }
    }
}

// HandleCoordinatorMessage procesa un mensaje COORDINATOR recibido.
func (cm *CoordinatorModule) HandleCoordinatorMessage(coordinatorID int) {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	// Solo se actualiza si el nuevo coordinador tiene un ID mayor o si no había primario
	if coordinatorID > cm.PrimaryID || cm.PrimaryID == -1 {
		fmt.Printf("Nodo %d: Recibido COORDINATOR. Nuevo primario es %d.\n", cm.NodeID, coordinatorID)
		cm.PrimaryID = coordinatorID
		cm.IsPrimary = (cm.NodeID == coordinatorID)
		cm.ElectionRunning = false // Detiene cualquier elección en curso

		cm.NodeState.IsPrimary = cm.IsPrimary
		cm.NodeState.SaveNodeStateToFile()

		cm.PrimaryLastSeen = time.Now()
	} else {
		fmt.Printf("Nodo %d: Ignorando mensaje COORDINATOR de %d porque el primario actual (%d) es más alto.\n", cm.NodeID, coordinatorID, cm.PrimaryID)
	}
}

func (cm *CoordinatorModule) DiscoverLeader() {
    fmt.Printf("Nodo %d: Iniciando sondeo de descubrimiento de líder...\n", cm.NodeID)
    for _, targetID := range cm.Nodes {
        if targetID == cm.NodeID {
            continue
        }
        go cm.SendElectionMessage(targetID)
    }
}

// SaveNodeStateToFile guarda el estado del nodo (si es primario) en un archivo JSON.
func (n *Nodo) SaveNodeStateToFile() error {
	fileName := fmt.Sprintf("node_%d.json", n.ID)
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return fmt.Errorf("error al serializar estado del nodo: %w", err)
	}
	err = ioutil.WriteFile(fileName, data, 0644)
	if err != nil {
		return fmt.Errorf("error al escribir estado del nodo en %s: %w", fileName, err)
	}
	fmt.Printf("Nodo %d: Estado persistido en %s (Es primario: %t).\n", n.ID, fileName, n.IsPrimary)
	return nil
}

// LoadNodeStateFromFile carga el estado del nodo desde un archivo JSON.
func LoadNodeStateFromFile(nodeID int) (*Nodo, error) {
	fileName := fmt.Sprintf("node_%d.json", nodeID)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return &Nodo{ID: nodeID, IsPrimary: false}, nil
		}
		return nil, fmt.Errorf("error al leer archivo de estado %s: %w", fileName, err)
	}
	var node Nodo
	if err = json.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("error al deserializar estado del nodo desde %s: %w", fileName, err)
	}
	fmt.Printf("Nodo %d: Estado cargado desde %s (Es primario: %t).\n", node.ID, fileName, node.IsPrimary)
	return &node, nil
}