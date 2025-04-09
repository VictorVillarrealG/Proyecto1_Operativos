#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#define BROKER_IP "127.0.0.1"  // IP del broker (localhost)
#define BROKER_PORT 5000       // Puerto donde escucha el broker
#define MAX_MESSAGE_CONTENT 256

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

int main() {
    int sock_fd;
    struct sockaddr_in broker_addr;

    // Crear socket TCP
    if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("Error creando socket");
        exit(EXIT_FAILURE);
    }

    // Configurar dirección del broker
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &broker_addr.sin_addr) <= 0) {
        perror("Dirección inválida o no soportada");
        exit(EXIT_FAILURE);
    }

    // Conectar al broker
    if (connect(sock_fd, (struct sockaddr *)&broker_addr, sizeof(broker_addr)) == -1) {
        perror("Error conectando al broker");
        close(sock_fd);
        exit(EXIT_FAILURE);
    }

    printf("Conectado al broker.\n");

    // Crear un mensaje a enviar
    Message msg;
    msg.id = 1;  // Puedes cambiar el id si quieres
    snprintf(msg.content, MAX_MESSAGE_CONTENT, "Hola desde el producer!");

    // Enviar el mensaje
    if (send(sock_fd, &msg, sizeof(Message), 0) == -1) {
        perror("Error enviando mensaje");
    } else {
        printf("Mensaje enviado: ID %d, Contenido: %s\n", msg.id, msg.content);
    }

    close(sock_fd); // Cerrar la conexión
    return 0;
}

