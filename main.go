package main

import (
	"INF343-Tarea3/servernode"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Uso: go run main.go <nodeID>")
		os.Exit(1)
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Error: %s no es un número válido\n", os.Args[1])
		os.Exit(1)
	}

	// Lista de nodos del sistema (ajústala si agregas más)
	allNodes := []int{1, 2, 3}

	// Cargar estado persistente del nodo (mock)
	nodo, _ := servernode.LoadNodeStateFromFile(nodeID)
	estado := &servernode.Estado{
		SequenceNumber: 0,
		EventLog:       []servernode.Evento{},
	}

	// Crear módulos
	coordinador := servernode.NewCoordinatorModule(nodeID, allNodes, nodo)
	syncMod := servernode.NewSynchronizationModule(nodeID, nodo, estado, coordinador)
	monMod := servernode.NewMonitorModule(nodeID, -1, coordinador)

	// Mostrar información inicial
	fmt.Printf("\n== Nodo %d iniciado ==\n", nodeID)
	fmt.Printf("¿Es primario?: %v\n", nodo.IsPrimary)

	if nodo.IsPrimary {
		event := servernode.Evento{ID: 1, Value: fmt.Sprintf("Evento desde nodo %d", nodeID)}
		syncMod.AddEvent(event)
	} else {
		time.Sleep(2 * time.Second)
		syncMod.RequestStateFromPrimary()
	}

	monMod.Start()
	time.Sleep(10 * time.Second)
	monMod.Stop()

	fmt.Println("== Nodo finalizado ==")
}
