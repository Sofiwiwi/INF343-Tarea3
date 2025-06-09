package servernode

import (
	"fmt"
	"net"
	"time"
)

// Variables globales del nodo actual
type Coordinador struct {
	ID               int
	ListaNodos       []int           // Lista de IDs de todos los nodos
	IsPrimary        bool
	DireccionNodos   map[int]string  // Mapa: ID -> direccion IP:puerto
	CanalElecciones  chan bool       // Señal para iniciar elección
	ResultadoEleccion chan int       // Resultado de la elección
}

// Inicia el proceso de coordinación (escucha elecciones y lanza si es necesario)
func (c *Coordinador) IniciarCoordinacion() {
	go c.escucharMensajes()
	for {
		select {
		case <-c.CanalElecciones:
			fmt.Println("[Coordinacion] Se detectó falla del primario. Lanzando elección...")
			c.iniciarEleccion()
		}
	}
}

// Implementa el algoritmo del matón (Bully Algorithm)
func (c *Coordinador) iniciarEleccion() {
	//mayorID := c.ID
	respuestas := 0

	for _, id := range c.ListaNodos {
		if id > c.ID {
			addr := c.DireccionNodos[id]
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err == nil {
				fmt.Fprintf(conn, "ELECTION|%d\n", c.ID)
				conn.Close()
				respuestas++
			}
		}
	}

	if respuestas == 0 {
		// Nadie respondió, soy el líder
		fmt.Printf("[Coordinacion] Nodo %d es el nuevo líder.\n", c.ID)
		c.IsPrimary = true
		c.anunciarNuevoLider()
		c.ResultadoEleccion <- c.ID
	} else {
		// Esperar resultado de otro nodo con ID mayor
		fmt.Printf("[Coordinacion] Nodo %d espera resultado de otro nodo.\n", c.ID)
	}
}

// Escucha mensajes TCP entrantes
func (c *Coordinador) escucharMensajes() {
	ln, err := net.Listen("tcp", c.DireccionNodos[c.ID])
	if err != nil {
		fmt.Println("Error al escuchar:", err)
		return
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go c.procesarMensaje(conn)
	}
}

func (c *Coordinador) procesarMensaje(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	mensaje := string(buf[:n])

	if mensaje[:8] == "ELECTION" {
		fmt.Printf("[Coordinacion] Nodo %d recibió mensaje de elección.\n", c.ID)
		// Responder al nodo emisor (implícito en este ejemplo)
		go c.iniciarEleccion()
	} else if mensaje[:6] == "LEADER" {
		// Extraer ID del nuevo líder
		fmt.Printf("[Coordinacion] Nodo %d detecta nuevo líder: %s\n", c.ID, mensaje[7:])
		c.IsPrimary = false
	}
}

// Enviar mensaje LEADER a todos los demás nodos
func (c *Coordinador) anunciarNuevoLider() {
	for _, id := range c.ListaNodos {
		if id != c.ID {
			addr := c.DireccionNodos[id]
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err == nil {
				fmt.Fprintf(conn, "LEADER|%d\n", c.ID)
				conn.Close()
			}
		}
	}
}
