#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#define FIFO_PATH "broker_fifo"     // Ruta relativa del FIFO (revisar, en teoria puede ir asi o con una direccion exacta)
#define LOG_FILE "log.txt"          // Archivo de log (aqui se guardan los logs)
#define MAX_MESSAGE_CONTENT 256     // Tamaño máximo del contenido del mensaje (hay que ajusrarlo no se que tan largos van a ser los mensajes)
#define MAX_QUEUE_SIZE 100          // Capacidad máxima de la cola (hay que cambiarlo dependiendo de lo que nos diga el profe)

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

    // Crea el FIFO si no existe.
    if (mkfifo(FIFO_PATH, 0666) == -1) { // Permisos de lectura y escritura para todos (propietario, grupo y otros),
        if (errno != EEXIST) {
            perror("Error al crear el FIFO");
            exit(EXIT_FAILURE);
        }
    }
    
    printf("Broker iniciado. Esperando mensajes...\n");
    
    // Abre el FIFO en modo lectura.
    int fifo_fd = open(FIFO_PATH, O_RDONLY); // Se debe abrir solo en modo lectura
    if (fifo_fd == -1) {
        perror("Error al abrir el FIFO para lectura");
        exit(EXIT_FAILURE);
    }

    // Abre el archivo de log en modo append (no se borran, solo se agregan mas).
    FILE *log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("Error al abrir el archivo de log");
        exit(EXIT_FAILURE);
    }

    Message msg;
    ssize_t bytes_read;

    // Bucle principal: espera y procesa los mensajes recibidos.
    while (1) {
        // Lee un mensaje del FIFO.
        bytes_read = read(fifo_fd, &msg, sizeof(Message));
        if (bytes_read > 0) {
            // Se recibió un mensaje; lo muestra por pantalla.
            printf("Mensaje recibido: ID %d, Contenido: %s\n", msg.id, msg.content);

            // Registra el mensaje en el archivo de log.
            fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
            fflush(log_file);

            // Inserta el mensaje en la cola.
            if (enqueue(&queue, msg) != 0) { // si enqueue devuelve un 0 significa que se agregó exitosamente a la cola. Si no, significa que ocurrió un error (por ejemplo la cola ya esté llena).
                fprintf(stderr, "La cola de mensajes está llena. Mensaje descartado.\n");
            }
        } else if (bytes_read == 0) { //read() devuelve 0 en un FIFO porque no se esta escribiendo en ese extremo
            // Si no hay datos, es posible que los productores hayan cerrado el FIFO.
            // Se cierra y reabre para seguir recibiendo nuevos mensajes.
            close(fifo_fd);
            fifo_fd = open(FIFO_PATH, O_RDONLY);
            if (fifo_fd == -1) {
                perror("Error al reabrir el FIFO");
                break;
            }
        } else {
            perror("Error al leer del FIFO");
            break;
        }
    }

    // Limpieza de recursos.
    fclose(log_file);
    close(fifo_fd);
    pthread_mutex_destroy(&queue.mutex);

    return 0;
}
