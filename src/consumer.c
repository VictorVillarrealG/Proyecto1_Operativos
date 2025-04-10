#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define SERVER_IP "127.0.0.1"
#define SERVER_PORT 5000
#define MAX_MESSAGE_CONTENT 256

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

int main() {
    int sock;
    struct sockaddr_in server_addr;
    char buffer[512];

    // Crear socket
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error al crear socket");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);

    if (inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr) <= 0) {
        perror("Dirección inválida/ no soportada");
        exit(EXIT_FAILURE);
    }

    // Conectar al broker
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error en la conexión");
        exit(EXIT_FAILURE);
    }

    // Enviar identificador CONSUMER
    send(sock, "CONSUMER", strlen("CONSUMER"), 0);

    // Esperar respuesta READY
    int bytes = recv(sock, buffer, sizeof(buffer)-1, 0);
    if (bytes <= 0) {
        perror("Error recibiendo READY");
        close(sock);
        exit(EXIT_FAILURE);
    }
    buffer[bytes] = '\0';
    printf("Broker dice: %s\n", buffer);

    if (strcmp(buffer, "READY") != 0) {
        printf("No se recibió READY. Cerrando.\n");
        close(sock);
        exit(EXIT_FAILURE);
    }

    // Loop para pedir mensajes
    while (1) {
        printf("Presiona ENTER para pedir el siguiente mensaje ('NEXT') o escribe 'exit' para salir: ");
        fgets(buffer, sizeof(buffer), stdin);

        if (strncmp(buffer, "exit", 4) == 0) {
            printf("Saliendo...\n");
            break;
        }

        // Mandar NEXT
        send(sock, "NEXT", strlen("NEXT"), 0);

        // Recibir respuesta
        bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            printf("Broker desconectado.\n");
            break;
        }

        if (bytes == sizeof(Message)) {
            Message* msg = (Message*)buffer;
            printf("Mensaje recibido => ID: %d, Contenido: %s\n", msg->id, msg->content);
        } else {
            buffer[bytes] = '\0';
            printf("Respuesta del broker: %s\n", buffer);
        }
    }

    close(sock);
    return 0;
}

