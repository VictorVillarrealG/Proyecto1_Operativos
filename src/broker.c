/* broker.c - Broker Central usando PUERTOS SEPARADOS con tipo de cliente controlado */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>

#define PRODUCER_PORT 5000
#define CONSUMER_PORT 5001
#define LOG_FILE "log.txt"
#define STORE_FILE "messages_store.txt"
#define MAX_MESSAGE_CONTENT 256

#define MAX_GROUPS       3
#define MAX_GROUP_NAME   64

#define THREAD_POOL_SIZE  (MAX_GROUPS * 6)

void* handle_client(void* arg);
static sem_t pool_sem;

typedef struct {
    int client_fd;
    int client_type;
} Task, ClientInfo;

typedef struct TaskNode {
    Task t;
    struct TaskNode *next;
} TaskNode;

static TaskNode *task_head = NULL, *task_tail = NULL;
static pthread_mutex_t task_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  task_cond  = PTHREAD_COND_INITIALIZER;

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

typedef struct {
    Message *buf;
    size_t cap, head, tail;
    pthread_mutex_t mutex;
} MsgQueue;

static MsgQueue queue;
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char name[MAX_GROUP_NAME];
    int  offset;
    int  turn_index;
    int  consumer_count;
    int  consumer_capacity;
    int  *members;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
} ConsumerGroup;

static ConsumerGroup groups[MAX_GROUPS];
static int           group_count = 0;
static pthread_mutex_t groups_mutex = PTHREAD_MUTEX_INITIALIZER;

void init_queue(MsgQueue *q) {
    q->cap  = 1024;
    q->buf  = malloc(q->cap * sizeof(Message));
    q->head = q->tail = 0;
    pthread_mutex_init(&q->mutex, NULL);
}

int next_id = 0;

void init_consumer_group(ConsumerGroup *g, int id) {
    snprintf(g->name, MAX_GROUP_NAME, "grupo_%d", id);
    g->offset            = -1;
    g->turn_index        = 0;
    g->consumer_count    = 0;
    g->consumer_capacity = 8;
    g->members           = malloc(sizeof(int) * g->consumer_capacity);
    pthread_mutex_init(&g->mutex, NULL);
    pthread_cond_init(&g->cond, NULL);
}

static bool     groups_initialized = false;
static int      next_group_idx     = 0;

static void ensure_groups_initialized() {
    if (!groups_initialized) {
        for (int i = 0; i < MAX_GROUPS; i++) {
            init_consumer_group(&groups[i], i);
        }
        groups_initialized = true;
        next_group_idx = 0;
    }
}

ConsumerGroup* assign_consumer_to_group(int sock) {

    while (pthread_mutex_trylock(&groups_mutex) != 0) {
    	usleep(1000); // 1ms de espera antes de reintentar
    }

    ensure_groups_initialized();

    int id = next_group_idx;
    ConsumerGroup *g = &groups[id];

    if (g->consumer_count == g->consumer_capacity) {
        int new_cap = g->consumer_capacity * 2;
        int *tmp = realloc(g->members, sizeof(int) * new_cap);
        if (tmp) {
            g->members = tmp;
            g->consumer_capacity = new_cap;
        }
    }

    g->members[g->consumer_count++] = sock;

    next_group_idx = (next_group_idx + 1) % MAX_GROUPS;

    pthread_mutex_unlock(&groups_mutex);
    return g;
}

void enqueue(MsgQueue *q, Message m) {
    pthread_mutex_lock(&q->mutex);

    size_t next = (q->tail + 1) % q->cap;
    if (next == q->head) {
        size_t new_cap = q->cap * 2;
        Message *new_buf = malloc(new_cap * sizeof(Message));
        size_t i = 0, idx = q->head;
        while (idx != q->tail) {
            new_buf[i++] = q->buf[idx];
            idx = (idx + 1) % q->cap;
        }
        free(q->buf);
        q->buf  = new_buf;
        q->cap  = new_cap;
        q->head = 0;
        q->tail = i;
        next     = (q->tail + 1) % q->cap;
    }

    // 2) Escribimos el mensaje
    q->buf[q->tail] = m;
    q->tail         = next;

    pthread_mutex_unlock(&q->mutex);
}

Message* get_next(MsgQueue *q, ConsumerGroup *g) {
    pthread_mutex_lock(&q->mutex);
    for (size_t idx = q->head; idx != q->tail; idx = (idx+1) % q->cap) {
        Message *m = &q->buf[idx];
        if (m->id > g->offset) {
            pthread_mutex_unlock(&q->mutex);
            return m;
        }
    }
    pthread_mutex_unlock(&q->mutex);
    return NULL;
}

void enqueue_task(Task t) {
    TaskNode *n = malloc(sizeof(*n));
    if (!n) {
        close(t.client_fd);
        return;
    }
    n->t = t;
    n->next = NULL;

    pthread_mutex_lock(&task_mutex);
    if (!task_tail) {
        task_head = task_tail = n;
    } else {
        task_tail->next = n;
        task_tail = n;
    }
    pthread_cond_signal(&task_cond);
    pthread_mutex_unlock(&task_mutex);
}

Task dequeue_task() {

    while (pthread_mutex_trylock(&task_mutex) != 0) {
    	usleep(1000); // 1ms de espera antes de reintentar
    } 

    while (!task_head) {
        pthread_cond_wait(&task_cond, &task_mutex);
    }

    TaskNode *n = task_head;
    task_head = n->next;
    if (!task_head) {
        task_tail = NULL;
    }
    pthread_mutex_unlock(&task_mutex);

    Task t = n->t;
    free(n);
    return t;
}

void* worker_thread(void* _) {
    while (1) {
        Task t = dequeue_task();

        ClientInfo *info = malloc(sizeof(ClientInfo));
        if (!info) {
            close(t.client_fd);
            sem_post(&pool_sem);
            continue;
        }
        info->client_fd   = t.client_fd;
        info->client_type = t.client_type;

        handle_client(info);

        sem_post(&pool_sem);
    }
    return NULL;
}

void init_thread_pool(void) {
    sem_init(&pool_sem, 0, THREAD_POOL_SIZE);

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

    FILE *store_file = fopen(STORE_FILE, "a");
    if (!store_file) {
        perror("Error al abrir archivo de persistencia");
        fclose(log_file);
        close(sock);
        return NULL;
    }

    while (1) {
        Message msg;
        int bytes = recv(sock, &msg, sizeof(msg), 0);
        if (bytes <= 0) {
            printf("[Broker] Producer desconectado.\n");
            break;
        }

        /* Asignamos el ID de forma segura  y lo registramos */
        pthread_mutex_lock(&queue_mutex);
	msg.id = next_id++;
	pthread_mutex_unlock(&queue_mutex);
	//Guardar en log.txt
        fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
        fflush(log_file);
        //Guardar en messages_store.txt
        fprintf(store_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
        fflush(store_file);

        /* === Encolar en el buffer circular === */
        enqueue(&queue, msg);

        printf("[Broker] Mensaje encolado: ID %d, Contenido: %s\n",
               msg.id, msg.content);
    }

    fclose(log_file);
    fclose(store_file);
    close(sock);
    return NULL;
}

void* handle_consumer(void* arg) {
    ClientInfo* info = (ClientInfo*)arg;
    int sock = info->client_fd;
    free(info);

    printf("[Broker] Nuevo consumer conectado (fd=%d).\n", sock);
    ConsumerGroup *g = assign_consumer_to_group(sock);
    printf("[Broker] consumer FD=%d asignado al grupo '%s'\n", sock, g->name);
    send(sock, "READY", 5, 0);

    FILE *consume_log = fopen(LOG_FILE, "a");
    if (!consume_log) perror("Error al abrir log de consumo");

    char buf[128];
    while (1) {
        int bytes = recv(sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) break;
        buf[bytes] = '\0';
        if (strcmp(buf, "SHUTDOWN") == 0) break;
        if (strcmp(buf, "NEXT")  != 0) continue;

        while (pthread_mutex_trylock(&g->mutex) != 0) {
	    usleep(1000); // 1ms de espera
	}

        while (g->consumer_count > 0 && g->members[g->turn_index] != sock) {
            pthread_cond_wait(&g->cond, &g->mutex);
        }

        Message *m = get_next(&queue, g);
        if (m) {
            g->offset = m->id;
            send(sock, m, sizeof(*m), 0);
            if (consume_log) {
                fprintf(consume_log,
                        "[%s] consumer FD=%d procesó mensaje ID=%d: %s\n",
                        g->name, sock, m->id, m->content);
                fflush(consume_log);
            }
        }
	else {
            send(sock, "NO_MESSAGES", 11, 0);
        }

        g->turn_index = (g->turn_index + 1) % g->consumer_count;
        pthread_cond_broadcast(&g->cond);
        pthread_mutex_unlock(&g->mutex);
    }

    if (consume_log) fclose(consume_log);

    pthread_mutex_lock(&groups_mutex);
    for (int i = 0; i < g->consumer_count; i++) {
        if (g->members[i] == sock) {
            memmove(&g->members[i],
                    &g->members[i+1],
                    (g->consumer_count - i - 1) * sizeof(int));
            g->consumer_count--;
            if (g->turn_index >= g->consumer_count) g->turn_index = 0;
            break;
        }
    }
    pthread_mutex_unlock(&groups_mutex);

    pthread_mutex_lock(&g->mutex);
    pthread_cond_broadcast(&g->cond);
    pthread_mutex_unlock(&g->mutex);

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
    int* args        = (int*)arg;
    int  server_fd   = args[0];
    int  client_type = args[1];

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addrlen);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        if (sem_trywait(&pool_sem) == 0) {
            Task t = { .client_fd = client_fd, .client_type = client_type };
            enqueue_task(t);
        } else {
            ClientInfo *info = malloc(sizeof(ClientInfo));
            if (!info) { 
                close(client_fd);
                continue; 
            }
            info->client_fd   = client_fd;
            info->client_type = client_type;
            pthread_t tid;
            pthread_create(&tid, NULL, handle_client, info);
            pthread_detach(tid);
        }
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
    if (listen(fd, SOMAXCONN) < 0) {
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

    for (int i = 0; i < group_count; i++) {
        pthread_mutex_destroy(&groups[i].mutex);
        pthread_cond_destroy(&groups[i].cond);
        free(groups[i].members);
    }
    pthread_mutex_destroy(&groups_mutex);

    pthread_mutex_destroy(&task_mutex);
    pthread_cond_destroy(&task_cond);

    sem_destroy(&pool_sem);

    pthread_mutex_destroy(&queue.mutex);
    free(queue.buf);
    return 0;
}
