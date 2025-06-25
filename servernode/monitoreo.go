package servernode

import (
	"fmt"
	"sync"
	"time"
)

const HeartbeatTimeout = 2 * time.Second
// MonitorModule es el módulo encargado de vigilar la salud del primario
type MonitorModule struct {
	NodeID      int
	PrimaryID   int
	Coordinator *CoordinatorModule
	Ticker      *time.Ticker
	Done        chan bool
	mu          sync.Mutex
	
	SendHeartbeat func(targetID int) bool
}

// NewMonitorModule crea una instancia del módulo de monitoreo
func NewMonitorModule(nodeID int, primaryID int, coordinator *CoordinatorModule) *MonitorModule {
	return &MonitorModule{
		NodeID:      nodeID,
		PrimaryID:   primaryID,
		Coordinator: coordinator,
		Ticker:      time.NewTicker(3 * time.Second), // monitoreo cada 3 segundos
		Done:        make(chan bool),
	}
}

// Start comienza el monitoreo del primario
func (mm *MonitorModule) Start() {
	go func() {
		for {
			select {
			case <-mm.Done:
				return
			case <-mm.Ticker.C:
				mm.CheckPrimary()
			}
		}
	}()
}

// Stop detiene el monitoreo
func (mm *MonitorModule) Stop() {
	mm.Ticker.Stop()
	mm.Done <- true
}

// CheckPrimary verifica si el primario está respondiendo
func (mm *MonitorModule) CheckPrimary() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// No monitorea si este nodo es el primario
	if mm.Coordinator.IsPrimary {
		return
	}

	fmt.Printf("Nodo %d: Enviando ARE_YOU_ALIVE al primario %d...\n", mm.NodeID, mm.Coordinator.PrimaryID)

	alive := mm.SendHeartbeat(mm.Coordinator.PrimaryID)

	if !alive {
		fmt.Printf("Nodo %d: ¡El primario %d no respondió! Iniciando elección...\n", mm.NodeID, mm.Coordinator.PrimaryID)
		go mm.Coordinator.StartElection()
	}
}