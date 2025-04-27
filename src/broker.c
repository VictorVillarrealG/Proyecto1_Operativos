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

typedef struct MessageNode {
    Message msg;
    struct MessageNode *next;
} MessageNode;

static MessageNode *msg_head = NULL, *msg_tail = NULL;
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
    pthread_mutex_lock(&groups_mutex);

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

void enqueue_message(Message m) {
    MessageNode *n = malloc(sizeof(*n));
    n->msg = m;
    n->next = NULL;
    pthread_mutex_lock(&queue_mutex);
    if (!msg_tail) msg_head = msg_tail = n;
    else { msg_tail->next = n; msg_tail = n; }
    pthread_mutex_unlock(&queue_mutex);
}

Message* get_next_for_group(ConsumerGroup *g) {
    pthread_mutex_lock(&queue_mutex);
    MessageNode *cur = msg_head;
    while (cur) {
        if (cur->msg.id > g->offset) {
            pthread_mutex_unlock(&queue_mutex);
            return &cur->msg;
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&queue_mutex);
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
    pthread_mutex_lock(&task_mutex);
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

    while (1) {
        Message msg;
        int bytes = recv(sock, &msg, sizeof(msg), 0);
        if (bytes <= 0) {
            printf("[Broker] Producer desconectado.\n");
            break;
        }

        msg.id = next_id++;
        fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
        fflush(log_file);

        enqueue_message(msg);
        printf("[Broker] Mensaje encolado: ID %d, Contenido: %s\n",
               msg.id, msg.content);
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
    while (1) {
        int bytes = recv(sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) {
            break;
        }
        buf[bytes] = '\0';
        if (strcmp(buf, "SHUTDOWN") == 0) {
            break;
        }
        if (strcmp(buf, "NEXT") != 0) {
            continue;
        }

        pthread_mutex_lock(&g->mutex);
        while (g->consumer_count > 0 && g->members[g->turn_index] != sock) {
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
            memmove(&g->members[i],
                    &g->members[i+1],
                    (g->consumer_count - i - 1) * sizeof(int));
            g->consumer_count--;
            if (g->turn_index >= g->consumer_count) {
                g->turn_index = 0;
            }
            break;
        }
    }
    if (g->consumer_count == 0) {
        pthread_mutex_destroy(&g->mutex);
        pthread_cond_destroy(&g->cond);
        free(g->members);
        int removed_idx = (int)(g - groups);
        for (int j = removed_idx; j < group_count - 1; j++) {
            groups[j] = groups[j+1];
        }
        group_count--;
        next_group_idx %= (group_count > 0 ? group_count : 1);
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

    MessageNode *cur = msg_head;
    while (cur) {
        MessageNode *next = cur->next;
        free(cur);
        cur = next;
    }
    pthread_mutex_destroy(&queue_mutex);

    pthread_mutex_destroy(&task_mutex);
    pthread_cond_destroy(&task_cond);

    sem_destroy(&pool_sem);

    return 0;
}
