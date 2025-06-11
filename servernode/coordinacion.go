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
	MessageTypeOK          = "OK" // Mensaje de respuesta a ELECTION y ARE_YOU_ALIVE
)

// CoordinatorModule es la estructura para el módulo de coordinación
type CoordinatorModule struct {
	NodeID           int
	Nodes            []int // IDs de todos los nodos conocidos en el sistema (necesario para el algoritmo del matón)
	IsPrimary        bool
	PrimaryID        int // ID del nodo primario conocido
	ElectionLock     sync.Mutex
	ElectionRunning  bool
	NodeState        *Nodo         // Referencia al estado del propio nodo
	PrimaryLastSeen  time.Time     // Última vez que se recibió un mensaje (heartbeat/OK) del primario
	PrimaryTimeout   time.Duration // Tiempo de espera para considerar al primario caído
	LastElectionTime time.Time     // Para evitar elecciones muy seguidas

	// Funciones que deben ser inyectadas por el entorno de ejecución (main.go)
	SendElectionMessage    func(targetID int) bool
	SendOKMessage          func(targetID int)
	SendCoordinatorMessage func(targetID, coordinatorID int)
}

// NewCoordinatorModule crea una nueva instancia del módulo de coordinación
func NewCoordinatorModule(nodeID int, allNodes []int, nodeState *Nodo) *CoordinatorModule {
	sort.Ints(allNodes) // Asegúrate de que allNodes esté ordenado para facilitar la elección
	cm := &CoordinatorModule{
		NodeID:           nodeID,
		Nodes:            allNodes,
		IsPrimary:        nodeState.IsPrimary, // Carga el estado de primario desde la persistencia
		PrimaryID:        -1,                  // ID desconocido al inicio
		NodeState:        nodeState,
		PrimaryTimeout:   5 * time.Second, // 5 segundos para considerar al primario caído
		PrimaryLastSeen:  time.Now(),      // Inicializa con el tiempo actual
		LastElectionTime: time.Unix(0, 0), // Inicializa a un tiempo muy antiguo
	}
	// Si se carga el estado y era primario, asumirlo de nuevo
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
	defer cm.ElectionLock.Unlock()

	// Evitar iniciar una nueva elección si ya hay una en curso o si se acaba de hacer una
	if cm.ElectionRunning {
		fmt.Printf("Nodo %d: Elección ya en curso. No iniciar de nuevo.\n", cm.NodeID)
		return
	}
	if time.Since(cm.LastElectionTime) < 2*time.Second { // No iniciar elección si una terminó hace poco
		fmt.Printf("Nodo %d: Elección reciente. Esperando antes de iniciar otra.\n", cm.NodeID)
		return
	}

	cm.ElectionRunning = true
	cm.PrimaryID = -1 // Resetear el primario conocido al iniciar elección
	cm.LastElectionTime = time.Now()

	fmt.Printf("Nodo %d: Iniciando elección...\n", cm.NodeID)

	higherNodesResponded := false
	for _, targetID := range cm.Nodes {
		if targetID > cm.NodeID {
			fmt.Printf("Nodo %d: Enviando ELECTION a %d\n", cm.NodeID, targetID)
			// SendElectionMessage debe esperar una respuesta OK
			if cm.SendElectionMessage(targetID) {
				// En un sistema real, esperaríamos el mensaje OK y lo procesaríamos en HandleOKMessage.
				// Para este modelo, si SendElectionMessage devuelve true, simulamos que responde OK.
				// Pero la lógica de detección de "OK" de un nodo superior es más compleja con la red.
				// El algoritmo del matón dice que si alguien *más alto* responde, se cede.
				higherNodesResponded = true
				break // Un nodo con ID más alto respondió, se cede la elección
			}
		}
	}

	if higherNodesResponded {
		fmt.Printf("Nodo %d: Nodo con ID más alto respondió. Esperando mensaje COORDINATOR.\n", cm.NodeID)
		// Esperar por un mensaje COORDINATOR por un tiempo
		go func() {
			time.Sleep(5 * time.Second) // Dar tiempo al nuevo coordinador para anunciarse
			cm.ElectionLock.Lock()
			defer cm.ElectionLock.Unlock()
			if cm.PrimaryID == -1 { // Si no se ha anunciado un coordinador después del tiempo de espera
				fmt.Printf("Nodo %d: No se recibió mensaje COORDINATOR. Reiniciando elección.\n", cm.NodeID)
				cm.ElectionRunning = false // Permitir nueva elección
				cm.StartElection()         // Reintentar
			} else {
				cm.ElectionRunning = false
			}
		}()
	} else {
		// Nadie con ID más alto respondió, este nodo es el nuevo primario
		cm.IsPrimary = true
		cm.PrimaryID = cm.NodeID
		cm.NodeState.IsPrimary = true // Actualizar estado persistente del nodo
		cm.NodeState.SaveNodeStateToFile()
		fmt.Printf("Nodo %d: ¡Soy el nuevo primario!\n", cm.NodeID)
		cm.ElectionRunning = false
		cm.AnnounceCoordinator()
	}
}

// AnnounceCoordinator anuncia el nuevo primario a todos los demás nodos.
func (cm *CoordinatorModule) AnnounceCoordinator() {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d (Primario): Anunciando que soy el primario (%d) a todos los demás nodos.\n", cm.NodeID, cm.NodeID)
	for _, targetID := range cm.Nodes {
		if targetID != cm.NodeID {
			cm.SendCoordinatorMessage(targetID, cm.NodeID)
		}
	}
	cm.PrimaryLastSeen = time.Now() // El primario se "ve" a sí mismo
}

// HandleElectionMessage procesa un mensaje ELECTION recibido.
func (cm *CoordinatorModule) HandleElectionMessage(senderID int) {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d: Recibió ELECTION de %d.\n", cm.NodeID, senderID)

	// Responder OK al nodo que envió la elección
	cm.SendOKMessage(senderID)

	// Si este nodo tiene un ID más alto, inicia su propia elección si no hay una en curso.
	if cm.NodeID > senderID && !cm.ElectionRunning {
		fmt.Printf("Nodo %d: Soy más alto que %d. Iniciando mi propia elección.\n", cm.NodeID, senderID)
		go cm.StartElection() // Iniciar en una goroutine para no bloquear el procesamiento de mensajes
	} else if cm.NodeID > senderID && cm.ElectionRunning {
		fmt.Printf("Nodo %d: Soy más alto que %d, pero ya hay una elección en curso (liderada por mí o uno superior).\n", cm.NodeID, senderID)
	}
}

// HandleOKMessage procesa un mensaje OK recibido.
func (cm *CoordinatorModule) HandleOKMessage(senderID int) {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d: Recibió OK de %d.\n", cm.NodeID, senderID)
	// Si este nodo estaba en una elección y recibió un OK de un nodo de mayor ID,
	// significa que ese nodo de mayor ID se hará cargo de la elección.
	// Este nodo debe dejar de lado su propia elección y esperar a que el nuevo coordinador se anuncie.
	if cm.ElectionRunning && senderID > cm.NodeID {
		fmt.Printf("Nodo %d: Un nodo de mayor ID (%d) respondió OK. Desactivando mi elección y esperando nuevo coordinador.\n", cm.NodeID, senderID)
		cm.ElectionRunning = false // Ceder la elección
	}

	// También usar el OK como una señal de vida, especialmente si el senderID es el primario actual
	if senderID == cm.PrimaryID {
		cm.PrimaryLastSeen = time.Now()
		// fmt.Printf("Nodo %d: Actualizado PrimaryLastSeen para el primario %d.\n", cm.NodeID, cm.PrimaryID) // Descomentar para debug
	}
}

// HandleCoordinatorMessage procesa un mensaje COORDINATOR recibido.
func (cm *CoordinatorModule) HandleCoordinatorMessage(coordinatorID int) {
	cm.ElectionLock.Lock()
	defer cm.ElectionLock.Unlock()

	fmt.Printf("Nodo %d: Recibió COORDINATOR de %d. Nuevo primario es %d.\n", cm.NodeID, coordinatorID, coordinatorID)
	cm.PrimaryID = coordinatorID
	cm.IsPrimary = (cm.NodeID == coordinatorID) // Actualiza si este nodo es el primario
	cm.ElectionRunning = false                  // Terminar cualquier elección en curso

	// Actualizar el estado persistente del nodo
	cm.NodeState.IsPrimary = cm.IsPrimary
	cm.NodeState.SaveNodeStateToFile()

	// Si este nodo es un secundario y se acaba de elegir un nuevo primario,
	// debería solicitar la sincronización de estado.
	// Esto se manejará en main.go después de que el coordinador se anuncie.
	cm.PrimaryLastSeen = time.Now() // El nuevo primario se acaba de anunciar
}

// getHigherIDNodes devuelve una lista de IDs de nodos con un ID mayor al propio.
// Se ha movido aquí para estar con los métodos del módulo.
func (cm *CoordinatorModule) getHigherIDNodes() []int {
	var higherNodes []int
	for _, id := range cm.Nodes {
		if id > cm.NodeID {
			higherNodes = append(higherNodes, id)
		}
	}
	return higherNodes
}

// declareSelfAsCoordinator se llama cuando el nodo se convierte en el coordinador.
// Se ha movido aquí para estar con los métodos del módulo.
func (cm *CoordinatorModule) declareSelfAsCoordinator() {
	cm.IsPrimary = true
	cm.PrimaryID = cm.NodeID
	cm.ElectionRunning = false
	cm.NodeState.IsPrimary = true      // Actualizar el estado del propio nodo
	cm.NodeState.SaveNodeStateToFile() // Persistir el estado del nodo

	fmt.Printf("Nodo %d: ¡Soy el nuevo coordinador/primario!\n", cm.NodeID)

	for _, targetID := range cm.Nodes {
		if targetID != cm.NodeID {
			cm.SendCoordinatorMessage(targetID, cm.NodeID) // Usar la función inyectada
		}
	}
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
	fmt.Printf("Nodo %d: Estado del nodo persistido en %s (Es primario: %t).\n", n.ID, fileName, n.IsPrimary)
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
	fmt.Printf("Nodo %d: Estado del nodo cargado desde %s (Es primario: %t).\n", node.ID, fileName, node.IsPrimary)
	return &node, nil
}
