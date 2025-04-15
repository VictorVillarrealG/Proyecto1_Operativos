/* broker.c - Broker Central usando PUERTOS SEPARADOS con tipo de cliente controlado */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PRODUCER_PORT 5000
#define CONSUMER_PORT 5001
#define MAX_CONNECTIONS 10
#define LOG_FILE "log.txt"
#define MAX_MESSAGE_CONTENT 256
#define MAX_QUEUE_SIZE 100

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
    for (int i = 0; i < queue.count; i++) {
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

void* handle_producer(void* arg) {
    int sock = *(int*)arg;
    printf("[Broker] Nuevo producer conectado.\n");
    FILE *log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("Error al abrir archivo de log");
        close(sock);
        return NULL;
    }

    while (1) {
        Message msg;
        int bytes = recv(sock, &msg, sizeof(Message), 0);
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

    fclose(log_file);
    close(sock);
    return NULL;
}

void* handle_consumer(void* arg) {
    int sock = *(int*)arg;
    printf("[Broker] Nuevo consumer conectado.\n");
    int last_id = -1;
    char ack[] = "READY";
    send(sock, ack, strlen(ack), 0);

    char buffer[128];
    while (1) {
        int bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
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

    close(sock);
    return NULL;
}

typedef struct {
    int client_fd;
    int client_type; // 0 = producer, 1 = consumer
} ClientInfo;

void* handle_client(void* arg) {
    ClientInfo* info = (ClientInfo*)arg;
    int sock = info->client_fd;
    int type = info->client_type;
    free(arg);

    if (type == 0) {
        return handle_producer(&sock);
    } else {
        return handle_consumer(&sock);
    }
}

void* listener_thread(void* arg) {
    int* args = (int*)arg;
    int server_fd = args[0];
    int client_type = args[1];

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addrlen);
        if (client_fd == -1) {
            perror("Error aceptando conexión");
            continue;
        }

        printf("[Broker] Conexión aceptada desde %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        ClientInfo* info = malloc(sizeof(ClientInfo));
        info->client_fd = client_fd;
        info->client_type = client_type;

        pthread_t tid;
        pthread_create(&tid, NULL, handle_client, info);
        pthread_detach(tid);
    }
    return NULL;
}

int create_listener_socket(int port) {
    int fd;
    struct sockaddr_in addr;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(EXIT_FAILURE);
    }
    if (listen(fd, MAX_CONNECTIONS) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    printf("[Broker] Escuchando en puerto %d...\n", port);
    return fd;
}

int main() {
    init_queue(&queue);

    int producer_fd = create_listener_socket(PRODUCER_PORT);
    int consumer_fd = create_listener_socket(CONSUMER_PORT);

    int producer_args[2] = {producer_fd, 0};
    int consumer_args[2] = {consumer_fd, 1};

    pthread_t producer_thread, consumer_thread;
    pthread_create(&producer_thread, NULL, listener_thread, producer_args);
    pthread_create(&consumer_thread, NULL, listener_thread, consumer_args);

    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);

    pthread_mutex_destroy(&queue.mutex);
    return 0;
}

