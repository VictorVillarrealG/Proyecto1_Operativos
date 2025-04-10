#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define LOG_FILE "log.txt"
#define MAX_MESSAGE_CONTENT 256
#define MAX_QUEUE_SIZE 100
#define BROKER_PORT 5000
#define MAX_CONNECTIONS 10

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

typedef struct {
    Message messages[MAX_QUEUE_SIZE];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
} MessageQueue;

MessageQueue queue;
int next_id = 0;

void init_queue(MessageQueue *q) {
    q->front = 0;
    q->rear = -1;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);
}

int enqueue(MessageQueue *q, Message msg) {
    pthread_mutex_lock(&q->mutex);
    if (q->count == MAX_QUEUE_SIZE) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    q->rear = (q->rear + 1) % MAX_QUEUE_SIZE;
    q->messages[q->rear] = msg;
    q->count++;
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

Message* get_next_message(int *last_id) {
    pthread_mutex_lock(&queue.mutex);
    int i;
    for (i = 0; i < queue.count; i++) {
        int index = (queue.front + i) % MAX_QUEUE_SIZE;
        if (queue.messages[index].id > *last_id) {
            *last_id = queue.messages[index].id;
            pthread_mutex_unlock(&queue.mutex);
            return &queue.messages[index];
        }
    }
    pthread_mutex_unlock(&queue.mutex);
    return NULL;
}

void* handle_client(void* arg) {
    int sock = *(int*)arg;
    free(arg);
    char buffer[MAX_MESSAGE_CONTENT];

    int bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) {
        printf("[Broker] Cliente desconectado antes de identificarse.\n");
        close(sock);
        return NULL;
    }
    buffer[bytes] = '\0';

    FILE *log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("Error al abrir archivo de log");
        close(sock);
        return NULL;
    }

    if (strcmp(buffer, "PRODUCER") == 0) {
        printf("[Broker] Nuevo producer conectado.\n");
        while (1) {
            Message msg;
            bytes = recv(sock, &msg, sizeof(Message), 0);
            if (bytes <= 0) {
                printf("[Broker] Producer desconectado.\n");
                break;
            }
            msg.id = next_id++;

            fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
            fflush(log_file);

            if (enqueue(&queue, msg) != 0) {
                fprintf(stderr, "La cola de mensajes está llena. Mensaje descartado.\n");
            } else {
                printf("[Broker] Mensaje recibido: ID %d, Contenido: %s\n", msg.id, msg.content);
            }
        }
    } else if (strcmp(buffer, "CONSUMER") == 0) {
        printf("[Broker] Nuevo consumer conectado.\n");
        int last_id = -1;
        char ack[] = "READY";
        send(sock, ack, strlen(ack), 0);

        while (1) {
            bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
            if (bytes <= 0) {
                printf("[Broker] Consumer desconectado.\n");
                break;
            }
            buffer[bytes] = '\0';

            if (strcmp(buffer, "NEXT") == 0) {
                Message* msg = get_next_message(&last_id);
                if (msg) {
                    send(sock, msg, sizeof(Message), 0);
                } else {
                    char no_msg[] = "NO_MESSAGES";
                    send(sock, no_msg, strlen(no_msg), 0);
                }
            }
        }
    } else {
        printf("[Broker] Cliente desconocido conectado. Cerrando conexión.\n");
    }

    fclose(log_file);
    close(sock);
    return NULL;
}


int main() {
    int server_fd, *client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    init_queue(&queue);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Error creando el socket del servidor");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(BROKER_PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Error en bind");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, MAX_CONNECTIONS) == -1) {
        perror("Error en listen");
        exit(EXIT_FAILURE);
    }

    printf("[Broker] Escuchando en el puerto %d...\n", BROKER_PORT);

    while (1) {
        client_fd = malloc(sizeof(int));
        *client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (*client_fd == -1) {
            perror("Error aceptando conexión");
            free(client_fd);
            continue;
        }
        printf("[Broker] Conexión aceptada desde %s:%d\n",      inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, client_fd);
        pthread_detach(tid);
    }

    close(server_fd);
    pthread_mutex_destroy(&queue.mutex);
    return 0;
}

