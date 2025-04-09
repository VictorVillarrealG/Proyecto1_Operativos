#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
//LIBRERIAS PARA SOCKETS:
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define FIFO_PATH "broker_fifo"     // Ruta relativa del FIFO (revisar, en teoria puede ir asi o con una direccion exacta)
#define LOG_FILE "log.txt"          // Archivo de log (aqui se guardan los logs)
#define MAX_MESSAGE_CONTENT 256     // Tamaño máximo del contenido del mensaje (hay que ajusrarlo no se que tan largos van a ser los mensajes)
#define MAX_QUEUE_SIZE 100          // Capacidad máxima de la cola (hay que cambiarlo dependiendo de lo que nos diga el profe)

//Nuevas constantes:
#define BROKER_PORT 5000  // Puerto TCP donde escuchará el broker
#define MAX_CONNECTIONS 10 // Número máximo de conexiones simultáneas


// No se si se deberian tener mas atributos para el mensaje.
typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

// Definición de la cola de mensajes.
typedef struct {
    Message messages[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
} MessageQueue;

MessageQueue queue;

// Función para inicializar la cola de mensajes.
void init_queue(MessageQueue *q) {
    q->front = 0;
    q->rear = -1;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
}

// Función para insertar un mensaje en la cola
int enqueue(MessageQueue *q, Message msg) {
    pthread_mutex_lock(&q->mutex);
    if(q->count == MAX_QUEUE_SIZE) {
        pthread_mutex_unlock(&q->mutex);
        return -1; // Si la cola llego al limite salta el error
    }
    q->rear = (q->rear + 1) % MAX_QUEUE_SIZE;
    q->messages[q->rear] = msg;
    q->count++;
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

int main() {
    init_queue(&queue);

    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    // Crear socket TCP
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Error creando el socket del servidor");
        exit(EXIT_FAILURE);
    }

    // Configurar dirección del servidor
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY; // Acepta conexiones de cualquier IP
    server_addr.sin_port = htons(BROKER_PORT);

    // Bind (asocia el socket al puerto)
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Error en bind");
        exit(EXIT_FAILURE);
    }

    // Escuchar conexiones entrantes
    if (listen(server_fd, MAX_CONNECTIONS) == -1) {
        perror("Error en listen");
        exit(EXIT_FAILURE);
    }

    printf("Broker escuchando en el puerto %d...\n", BROKER_PORT);

    // Abrir archivo de log
    FILE *log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("Error al abrir archivo de log");
        exit(EXIT_FAILURE);
    }

    while (1) {
        // Aceptar una conexión entrante
        client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd == -1) {
            perror("Error aceptando conexión");
            continue;
        }

        printf("Nueva conexión aceptada.\n");

        // Recibir el mensaje
        Message msg;
        ssize_t bytes_received = recv(client_fd, &msg, sizeof(Message), 0);
        if (bytes_received > 0) {
            printf("Mensaje recibido: ID %d, Contenido: %s\n", msg.id, msg.content);

            // Registrar mensaje en log
            fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
            fflush(log_file);

            // Insertar mensaje en la cola
            if (enqueue(&queue, msg) != 0) {
                fprintf(stderr, "La cola de mensajes está llena. Mensaje descartado.\n");
            }
        } else {
            perror("Error recibiendo mensaje");
        }

        close(client_fd); // Cerramos la conexión con el cliente
    }

    // Cleanup (aunque realmente en este bucle infinito nunca se ejecutaría)
    fclose(log_file);
    close(server_fd);
    pthread_mutex_destroy(&queue.mutex);

    return 0;
}

