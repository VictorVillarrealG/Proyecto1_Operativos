# Proyecto - Message Broker (Mini-Kafka)

## Descripción del sistema implementado

Este sistema simula un broker de mensajes distribuido inspirado en Apache Kafka, implementado en C estándar. El broker funciona como intermediario entre múltiples productores y consumidores, gestionando la recepción, almacenamiento y distribución de mensajes de forma concurrente, ordenada y segura.

Características principales:
- Productores envían mensajes al broker a través de sockets TCP.
- El broker asigna un ID secuencial a cada mensaje y lo registra en archivos persistentes.
- Los consumidores se organizan en grupos. Cada mensaje es entregado solo una vez por grupo.
- Los consumidores mantienen un `offset` para reanudar la lectura donde quedaron.
- El broker es concurrente y usa un pool de hilos para procesar múltiples conexiones simultáneamente.

---

## Estructura de archivos
- `broker.c`: Contiene la lógica del broker, el manejo de conexiones, grupos, control de concurrencia y persistencia.
- `producer.c`: Envía un mensaje al broker mediante socket TCP.
- `consumer.c`: Se conecta al broker, solicita mensajes periódicamente y los consume.
- `messages_store.txt`: Archivo de almacenamiento persistente para los mensajes (ID + contenido).
- `log.txt`: Registro de eventos del sistema (recepción, entrega, procesamiento, desconexiones).
- `Makefile`: Script de compilación de los tres componentes.
- `.gitignore`: Ignora binarios, logs y otros archivos innecesarios en control de versiones.

---

## Instrucciones para compilar y ejecutar

1. **Compilacion**:

Desde el directorio raíz del proyecto:
make

Esto genera los ejecutables de broker, producer y consumer.

2. **Ejecucion**:
En una terminal:
./broker

En otra terminal (para probar producer):
./producer

En otra terminal (para probar consumer):
./consumer

---

## Estrategia para evitar interbloqueos
El sistema aplica varias técnicas para evitar interbloqueos:
Orden fijo de adquisición de locks: se accede siempre en el mismo orden a mutex de estructuras compartidas.

`pthread_mutex_trylock` y `pthread_mutex_timedlock`: se usan para evitar bloqueos indefinidos en zonas críticas.

Broadcast controlado con `pthread_cond_broadcast` y `pthread_cond_timedwait` permite que los consumidores se turnen sin bloquearse mutuamente.


---

## Problemas conocidos o limitaciones
El broker actual funciona como único nodo. No hay replicación ni distribución multi-broker.

No se implementó el algoritmo del banquero como estrategia adicional.

El archivo `messages_store.txt` se abre en modo append, por lo que contiene todo el historial de mensajes a menos que se borre manualmente.

Si se lanza más de 1000 procesos simultáneamente, puede saturarse el sistema operativo en entornos con pocos recursos (como VMs).

Dado que los consumidores se organizan por grupos y consumen por turnos, el archivo `log.txt` puede mostrar mensajes con IDs no secuenciales entre líneas. Esto es un comportamiento normal en sistemas concurrentes, donde los grupos procesan a velocidades distintas. En otras palabras, los consumidores esperan su turno y pueden recibir mensajes con IDs no consecutivos desde el punto global, pero son consistentes dentro del grupo como tal...Como cada grupo avanza a su propio ritmo, el orden de entrega por grupo puede verse intercalado en el log global.  

---

## Autores
Grupo de trabajo: [Gabriel Varela Chacon, Victor Villarreal Guerrero y Angel Quesada Jimenez]
Curso: EIF-212 Sistemas Operativos
Fecha de entrega: 7 de mayo de 2025
