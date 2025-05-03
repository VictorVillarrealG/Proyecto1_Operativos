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
#include <time.h>
#include <errno.h>
#include <signal.h> 
#include <stdatomic.h>
#include <fcntl.h>

#define PRODUCER_PORT 5000
#define CONSUMER_PORT 5001
#define LOG_FILE "log.txt"
#define STORE_FILE "messages_store.txt"
#define MAX_MESSAGE_CONTENT 256

#define MAX_GROUPS       3
#define MAX_GROUP_NAME   64
#define MAX_QUEUE_SIZE 100
#define MAX_QUEUE_CAPACITY 1024

#define THREAD_POOL_SIZE  (MAX_GROUPS * 6)
pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
void* handle_client(void* arg);
static sem_t pool_sem;

static atomic_int server_running = 1;

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
    size_t cap;
    size_t head;
    size_t tail;
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
static pthread_mutex_t groups_mutex = 
PTHREAD_MUTEX_INITIALIZER;

void shutdown_server(int sig) {
    atomic_store(&server_running, 0);
    printf("\n[Broker] Señal de terminación recibida. Cerrando el servidor...\n");
    
    pthread_mutex_lock(&task_mutex);
    pthread_cond_broadcast(&task_cond);
    pthread_mutex_unlock(&task_mutex);
    
    pthread_mutex_lock(&queue_mutex);
    pthread_cond_signal(&not_empty);
    pthread_cond_signal(&not_full);
    pthread_mutex_unlock(&queue_mutex);
    
    for (int i = 0; i < MAX_GROUPS; i++) {
        pthread_mutex_lock(&groups[i].mutex);
        pthread_cond_broadcast(&groups[i].cond);
        pthread_mutex_unlock(&groups[i].mutex);
    }
}

void init_queue(MsgQueue *q) {
    q->cap  = MAX_QUEUE_CAPACITY;
    q->buf  = malloc(q->cap * sizeof(Message));
    q->head = 0;
    q->tail = 0;
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
    	usleep(1000);
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

int enqueue(MsgQueue *q, Message msg) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 5;

    pthread_mutex_lock(&q->mutex);

  while (((q->tail + 1) % q->cap) == q->head) {
    pthread_cond_wait(&not_full, &q->mutex);  
}

    q->buf[q->tail] = msg;
    q->tail = (q->tail + 1) % q->cap;

    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
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
    pthread_mutex_lock(&task_mutex);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;

    while (!task_head && atomic_load(&server_running)) {
        int wait_result = pthread_cond_timedwait(&task_cond, &task_mutex, &ts);
        if (wait_result == ETIMEDOUT || !atomic_load(&server_running)) {
            pthread_mutex_unlock(&task_mutex);
            Task empty = { .client_fd = -1, .client_type = -1 };
            return empty; 
        }
    }
    
    if (!task_head || !atomic_load(&server_running)) {
        pthread_mutex_unlock(&task_mutex);
        Task empty = { .client_fd = -1, .client_type = -1 };
        return empty;
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
    while (atomic_load(&server_running)) {
        Task t = dequeue_task();
        if (t.client_fd == -1) {
            if (!atomic_load(&server_running)) {
                break;
            }
            continue;
        }
        
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

void init_thread_pool(void) {
    pthread_t tids[THREAD_POOL_SIZE];
    for (int i = 0; i < THREAD_POOL_SIZE; i++) {
        pthread_create(&tids[i], NULL, worker_thread, NULL);
        pthread_detach(tids[i]);
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
    while (atomic_load(&server_running)) {
        Message msg;
        int bytes = recv(sock, &msg, sizeof(msg), 0);
        if (bytes <= 0) {
            printf("[Broker] Producer desconectado.\n");
            break;
        }
        pthread_mutex_lock(&queue_mutex);
        msg.id = next_id++;
        fprintf(log_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
        fflush(log_file);
        fprintf(store_file, "ID: %d, Contenido: %s\n", msg.id, msg.content);
        fflush(store_file);
        enqueue(&queue, msg);
        pthread_mutex_unlock(&queue_mutex);
        printf("[Broker] Mensaje encolado: ID %d, Contenido: %s\n", msg.id, msg.content);
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
    while (atomic_load(&server_running)) { 
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sock, &readfds);
        
        struct timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 500000; 
        
        int ready = select(sock + 1, &readfds, NULL, NULL, &tv);
        if (ready <= 0) {
            if (!atomic_load(&server_running)) break;
            if (ready < 0 && errno != EINTR) break;
            continue;
        }
        
        int bytes = recv(sock, buf, sizeof(buf)-1, 0);
        if (bytes <= 0) break;
        buf[bytes] = '\0';
        
        if (strcmp(buf, "SHUTDOWN") == 0) break;
        if (strcmp(buf, "NEXT") != 0) continue;

        int lock_result;
        struct timespec lock_timeout;
        clock_gettime(CLOCK_REALTIME, &lock_timeout);
        lock_timeout.tv_sec += 1;  
        
        lock_result = pthread_mutex_timedlock(&g->mutex, &lock_timeout);
        if (lock_result != 0) {
            if (!atomic_load(&server_running)) break;
            continue;
        }

        if (!atomic_load(&server_running)) {
            pthread_mutex_unlock(&g->mutex);
            break;
        }

        while (g->consumer_count > 0 && g->members[g->turn_index] != sock && 
               atomic_load(&server_running)) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += 1; 
            
            int wait_result = pthread_cond_timedwait(&g->cond, &g->mutex, &ts);
            if (!atomic_load(&server_running) || wait_result == ETIMEDOUT) {
                break;
            }
        }
        
        if (!atomic_load(&server_running)) {
            pthread_mutex_unlock(&g->mutex);
            break;
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
        } else {
            send(sock, "NO_MESSAGES", 11, 0);
        }

        g->turn_index = (g->turn_index + 1) % g->consumer_count;
        pthread_cond_broadcast(&g->cond);
        pthread_mutex_unlock(&g->mutex);
    }

    if (consume_log) fclose(consume_log);

    if (pthread_mutex_timedlock(&groups_mutex, &(struct timespec){.tv_sec = time(NULL) + 1}) == 0) {
        for (int i = 0; i < g->consumer_count; i++) {
            if (g->members[i] == sock) {
                memmove(&g->members[i], &g->members[i + 1], 
                       (g->consumer_count - i - 1) * sizeof(int));
                g->consumer_count--;
                if (g->turn_index >= g->consumer_count) g->turn_index = 0;
                break;
            }
        }
        pthread_mutex_unlock(&groups_mutex);
    }

    if (pthread_mutex_timedlock(&g->mutex, &(struct timespec){.tv_sec = time(NULL) + 1}) == 0) {
        pthread_cond_broadcast(&g->cond);
        pthread_mutex_unlock(&g->mutex);
    }

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
    int* args = (int*)arg;
    int server_fd = args[0];
    int client_type = args[1];

    struct sockaddr_in client_addr;
    socklen_t addrlen = sizeof(client_addr);
    
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    while (atomic_load(&server_running)) {
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addrlen);
        if (client_fd < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                usleep(100000);  
                continue;
            }
            perror("accept");
            continue;
        }

        Task t = { .client_fd = client_fd, .client_type = client_type };
        enqueue_task(t);
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
    signal(SIGINT, shutdown_server);
    
    init_queue(&queue);
    
    pthread_t worker_threads[THREAD_POOL_SIZE];
    init_thread_pool();  

    int producer_fd = create_listener_socket(PRODUCER_PORT);
    int consumer_fd = create_listener_socket(CONSUMER_PORT);

    int producer_args[2] = { producer_fd, 0 };
    int consumer_args[2] = { consumer_fd, 1 };

    pthread_t producer_listener, consumer_listener;
    pthread_create(&producer_listener, NULL, listener_thread, producer_args);
    pthread_create(&consumer_listener, NULL, listener_thread, consumer_args);

    while (atomic_load(&server_running)) {
        sleep(1);
    }

    close(producer_fd);
    close(consumer_fd);

    printf("[Broker] Esperando a que terminen los threads...\n");
    pthread_join(producer_listener, NULL);
    pthread_join(consumer_listener, NULL);
    
    sleep(2);

    printf("[Broker] Liberando recursos...\n");
    
    sem_destroy(&pool_sem);
    pthread_mutex_destroy(&queue.mutex);
    pthread_mutex_destroy(&task_mutex);
    pthread_cond_destroy(&task_cond);
    pthread_cond_destroy(&not_empty);
    pthread_cond_destroy(&not_full);
    
    for (int i = 0; i < MAX_GROUPS; i++) {
        pthread_mutex_destroy(&groups[i].mutex);
        pthread_cond_destroy(&groups[i].cond);
        free(groups[i].members);
    }
    pthread_mutex_destroy(&groups_mutex);
    
    free(queue.buf);
    
    printf("[Broker] Terminado.\n");
    return 0;
}
