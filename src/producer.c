#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#define BROKER_IP "127.0.0.1"
#define PRODUCER_PORT 5000
#define MAX_MESSAGE_CONTENT 256

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

int main() {
    int sock_fd;
    struct sockaddr_in broker_addr;

    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Error creando socket");
        exit(EXIT_FAILURE);
    }

    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(PRODUCER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr) <= 0) {
        perror("Dirección inválida o no soportada");
        exit(EXIT_FAILURE);
    }

    if (connect(sock_fd, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) == -1) {
        perror("Error conectando al broker");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Conectado al broker (producer).\n");

    Message msg;
    memset(&msg, 0, sizeof(Message));  
    msg.id = 0;  
    snprintf(msg.content, MAX_MESSAGE_CONTENT, "Hola desde el producer!");

    if (send(sock_fd, &msg, sizeof(Message), 0) == -1) {
        perror("Error enviando mensaje");
    } else {
        printf("Mensaje enviado: Contenido: %s\n", msg.content);
    }

    close(sock_fd);
    return 0;
}

