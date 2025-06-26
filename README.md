# INF343-Tarea3

## Integrantes:
- Giuseppe Queirolo (202273112-5)
- Sofía Ramírez (202273008-0)

## Instrucciones de uso:

En cada máquina virtual se encuentra clonado el repositorio con los permisos correspondientes para ejecutar los archivos **bash**. Además se encuentra instalado Go.

(Opcional) Para una limpieza inicial se pueden ejecutar los siguientes comandos en las máquinas:

```bash
rm *.log *.json *.pid
```

Para iniciar un nodo, en cada máquina ejecutar lo siguiente:

```bash
# En la máquina 1)
./start.sh 1

# En la máquina 2)
./start.sh 2

# En la máquina 3)
./start.sh 3
```
Si se inician los tres nodos en tiempos muy similares, se ejecutará el algoritmo del matón y ganará el que tenga mayor ID. Si se inicia un nodo y se espera hasta iniciar otros, el nodo inicial será el coordinador. 

Un nodo deja de ser primario sólo cuando se cae, para detener un proceso se debe hacer el siguiente comando:

```bash
./kill.sh <id>
```
## Consideraciones:
- El sistema al iniciar tiene una pausa, para mitigar condiciones de carrera cuando arrancan los nodos
- Persistencia de Estado: El sistema guarda dos tipos de archivos de estado:
    - *node_X.json*: Almacena el estado propio del nodo (si es primario y el último mensaje). Es borrado estratégicamente por start.sh para asegurar una reintegración correcta. Cuando se terminan todos los procesos ocurre que este archivo, para todos los nodos, los marca como primarios. 
    - *node_X_state.json*: Almacena el estado de la aplicación replicada (el log de eventos). Este archivo no se borra para simular una recuperación realista.
