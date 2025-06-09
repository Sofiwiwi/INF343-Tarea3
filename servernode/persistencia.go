package servernode

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	//"time" // Not explicitly needed in this file
)

// PersistenceModule es el módulo encargado de la persistencia del estado
type PersistenceModule struct {
	NodeID   int
	FilePath string
	mu       sync.Mutex // Mutex para proteger el archivo de persistencia
}

// NewPersistenceModule crea una nueva instancia del módulo de persistencia
func NewPersistenceModule(nodeID int) *PersistenceModule {
	return &PersistenceModule{
		NodeID:   nodeID,
		FilePath: fmt.Sprintf("node_%d_state.json", nodeID), // Un nombre de archivo específico para el estado replicado
	}
}

// SaveState guarda el estado actual del nodo en un archivo JSON.
func (pm *PersistenceModule) SaveState(state *Estado) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("error al serializar el estado para guardar: %w", err)
	}

	err = ioutil.WriteFile(pm.FilePath, data, 0644)
	if err != nil {
		return fmt.Errorf("error al escribir el estado en el archivo %s: %w", pm.FilePath, err)
	}
	fmt.Printf("Nodo %d: Estado persistido en %s (SequenceNumber: %d).\n", pm.NodeID, pm.FilePath, state.SequenceNumber)
	return nil
}

// LoadState carga el estado del nodo desde un archivo JSON.
func (pm *PersistenceModule) LoadState() (*Estado, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	data, err := ioutil.ReadFile(pm.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Si el archivo no existe, retornamos un estado inicial vacío
			fmt.Printf("Nodo %d: Archivo de estado %s no encontrado. Inicializando estado vacío.\n", pm.NodeID, pm.FilePath)
			return &Estado{SequenceNumber: 0, EventLog: []Evento{}}, nil
		}
		return nil, fmt.Errorf("error al leer el archivo de estado %s: %w", pm.FilePath, err)
	}

	var state Estado
	err = json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("error al deserializar el estado desde el archivo %s: %w", pm.FilePath, err)
	}
	fmt.Printf("Nodo %d: Estado cargado desde %s (SequenceNumber: %d).\n", pm.NodeID, pm.FilePath, state.SequenceNumber)
	return &state, nil
}
