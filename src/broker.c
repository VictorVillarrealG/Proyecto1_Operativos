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
#include <semaphore.h>

#define PRODUCER_PORT 5000
#define CONSUMER_PORT 5001
#define MAX_CONNECTIONS 10
#define LOG_FILE "log.txt"
#define MAX_MESSAGE_CONTENT 256
#define MAX_QUEUE_SIZE 100

#define MAX_GROUPS       10
#define MAX_GROUP_NAME   64
#define MAX_CONSUMERS_PER_GROUP 2

#define THREAD_POOL_SIZE     4    // Hay que ver cuales poner
#define MAX_TASKS            100  // Máximo de conexiones pendientes

void* handle_client(void* arg);

typedef struct {
    int client_fd;
    int client_type;  // 0 = producer, 1 = consumer
} Task;

typedef struct {
    int client_fd;
    int client_type; // 0 = producer, 1 = consumer
} ClientInfo;

static Task            task_queue[MAX_TASKS];
static int             task_head = 0, task_tail = 0, task_count = 0;
static pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  task_cond  = PTHREAD_COND_INITIALIZER;

typedef struct {
    char name[MAX_GROUP_NAME];
    int  offset;
    int  consumer_count;
    int  members[MAX_CONSUMERS_PER_GROUP];  // sockets de consumers
    int  turn_index;                        // quién debe leer ahora
    pthread_mutex_t mutex;                  // protege estado del grupo
    pthread_cond_t  cond;                   // señal para turnos
} ConsumerGroup;

static ConsumerGroup groups[MAX_GROUPS];
static int           group_count = 0;
static pthread_mutex_t groups_mutex = PTHREAD_MUTEX_INITIALIZER;

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

ConsumerGroup* assign_consumer_to_group(int sock) {
    pthread_mutex_lock(&groups_mutex);

    for (int i = 0; i < group_count; i++) {
        if (groups[i].consumer_count < MAX_CONSUMERS_PER_GROUP) {
            groups[i].members[groups[i].consumer_count++] = sock;
            pthread_mutex_unlock(&groups_mutex);
            return &groups[i];
        }
    }

    if (group_count < MAX_GROUPS) {
   	ConsumerGroup *g = &groups[group_count];
   	snprintf(g->name, MAX_GROUP_NAME, "grupo_%d", group_count);
    	g->offset = -1;
    	g->consumer_count = 1;
    	g->members[0]     = sock;
    	g->turn_index     = 0;
    	pthread_mutex_init(&g->mutex, NULL);
    	pthread_cond_init(&g->cond,  NULL);
    	group_count++;
    	pthread_mutex_unlock(&groups_mutex);
    	return g;
       }
    pthread_mutex_unlock(&groups_mutex);
    return NULL;
}

Message* get_next_for_group(ConsumerGroup *g) {
    pthread_mutex_lock(&queue.mutex);
    for (int i = 0; i < queue.count; i++) {
        int idx = (queue.front + i) % MAX_QUEUE_SIZE;
        if (queue.messages[idx].id > g->offset) {
            Message *m = &queue.messages[idx];
            pthread_mutex_unlock(&queue.mutex);
            return m;
        }
    }
    pthread_mutex_unlock(&queue.mutex);
    return NULL;
}


void enqueue_task(Task t) {
    pthread_mutex_lock(&task_mutex);
    if (task_count < MAX_TASKS) {
        task_queue[task_tail] = t;
        task_tail = (task_tail + 1) % MAX_TASKS;
        task_count++;
        pthread_cond_signal(&task_cond);
    } else {
        close(t.client_fd);
    }
    pthread_mutex_unlock(&task_mutex);
}

Task dequeue_task() {
    pthread_mutex_lock(&task_mutex);
    while (task_count == 0) {
        pthread_cond_wait(&task_cond, &task_mutex);
    }
    Task t = task_queue[task_head];
    task_head = (task_head + 1) % MAX_TASKS;
    task_count--;
    pthread_mutex_unlock(&task_mutex);
    return t;
}

void* worker_thread(void* _) {
    (void)_;
    while (1) {
        Task t = dequeue_task();

        ClientInfo *info = malloc(sizeof(ClientInfo));
        if (!info) {
            close(t.client_fd);
            continue;
        }
        info->client_fd   = t.client_fd;
        info->client_type = t.client_type;

        handle_client(info);
    }
    return NULL;
}

void init_thread_pool() {
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_t tid;
        pthread_create(&tid, NULL, worker_thread, NULL);
        pthread_detach(tid);
    }
}


void* handle_producer(void* arg) {
    ClientInfo* info = (ClientInfo*)arg;
    int sock = info->client_fd;
    free(info);

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
    ClientInfo* info = (ClientInfo*)arg;
    int sock = info->client_fd;
    free(info);

    printf("[Broker] Nuevo consumer conectado (fd=%d).\n", sock);
    ConsumerGroup *g = assign_consumer_to_group(sock);
    if (!g) {
        send(sock, "ERROR: Grupos llenos\n", 21, 0);
        close(sock);
        return NULL;
    }

    printf("[Broker] consumer FD=%d asignado al grupo '%s'\n", sock, g->name);
    send(sock, "READY", 5, 0);

    FILE *consume_log = fopen(LOG_FILE, "a");
    if (!consume_log) {
        perror("Error al abrir log de consumo");
    }

    char buf[128];
    while (recv(sock, buf, sizeof(buf)-1, 0) > 0) {
        buf[strcspn(buf, "\n")] = '\0';
        if (strcmp(buf, "NEXT") != 0) {
            continue;
        }

        pthread_mutex_lock(&g->mutex);
        while (g->members[g->turn_index] != sock) {
            pthread_cond_wait(&g->cond, &g->mutex);
        }

        Message *m = get_next_for_group(g);
        if (m) {
            g->offset = m->id;
            send(sock, m, sizeof(*m), 0);
            if (consume_log) {
                fprintf(consume_log,
                    "[%s] consumer FD=%d procesó mensaje ID=%d: %s\n",
                    g->name, sock, m->id, m->content);
                fflush(consume_log);
            }
        } else {
            send(sock, "NO_MESSAGES", 11, 0);
        }

        g->turn_index = (g->turn_index + 1) % g->consumer_count;
        pthread_cond_broadcast(&g->cond);
        pthread_mutex_unlock(&g->mutex);
    }

    if (consume_log) {
        fclose(consume_log);
    }

    pthread_mutex_lock(&groups_mutex);
    for (int i = 0; i < g->consumer_count; i++) {
        if (g->members[i] == sock) {
            memmove(&g->members[i], &g->members[i+1],
                (g->consumer_count - i - 1) * sizeof(int));
            g->consumer_count--;
            if (g->turn_index >= g->consumer_count) {
                g->turn_index = 0;
            }
            break;
        }
    }
    pthread_mutex_unlock(&groups_mutex);

    close(sock);
    return NULL;
}

void* handle_client(void* arg) {
    ClientInfo* info = arg;
    int sock = info->client_fd;
    int type = info->client_type;

    if (type == 0) {
        return handle_producer(info);
    } else {
        return handle_consumer(info);
    }
}

void* listener_thread(void* arg) {
    int* args       = (int*)arg;
    int  server_fd  = args[0];
    int  client_type = args[1];          // 0 = producer, 1 = consumer

    FILE* log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("Error al abrir log");
    }

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);

        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addrlen);
        if (client_fd == -1) {
            perror("Error aceptando conexión");
            if (log_file) {
                fprintf(log_file,
                        "[Broker] Error aceptando conexión: %s\n",
                        strerror(errno));
                fflush(log_file);
            }
            continue;
        }

        printf("[Broker] Conexión aceptada desde %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        if (log_file) {
            fprintf(log_file,
                    "[Broker] Conexión aceptada: fd=%d, tipo=%s, origen=%s:%d\n",
                    client_fd,
                    client_type == 0 ? "PRODUCER" : "CONSUMER",
                    inet_ntoa(client_addr.sin_addr),
                    ntohs(client_addr.sin_port));
            fflush(log_file);
        }
        Task t = { .client_fd = client_fd, .client_type = client_type };
        enqueue_task(t);
    }

    if (log_file) fclose(log_file);
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
    init_thread_pool();

    int producer_fd = create_listener_socket(PRODUCER_PORT);
    int consumer_fd = create_listener_socket(CONSUMER_PORT);

    int producer_args[2] = { producer_fd, 0 };
    int consumer_args[2] = { consumer_fd, 1 };

    pthread_t producer_listener, consumer_listener;
    pthread_create(&producer_listener, NULL, listener_thread, producer_args);
    pthread_create(&consumer_listener, NULL, listener_thread, consumer_args);

    pthread_join(producer_listener, NULL);
    pthread_join(consumer_listener, NULL);

    pthread_mutex_destroy(&queue.mutex);
    pthread_mutex_destroy(&groups_mutex);
    pthread_mutex_destroy(&task_mutex);
    pthread_cond_destroy(&task_cond);

    return 0;
}
