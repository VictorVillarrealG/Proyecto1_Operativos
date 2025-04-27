#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>

#define BROKER_IP "127.0.0.1"
#define CONSUMER_PORT 5001
#define MAX_MESSAGE_CONTENT 256
#define TIMEOUT_SECONDS 10

typedef struct {
    int id;
    char content[MAX_MESSAGE_CONTENT];
} Message;

int main() {
    int sock;
    struct sockaddr_in server_addr;
    char buffer[512];
    time_t last_message_time = time(NULL);

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error al crear socket");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(CONSUMER_PORT);
    if (inet_pton(AF_INET, BROKER_IP, &server_addr.sin_addr) <= 0) {
        perror("Dirección inválida o no soportada");
        exit(EXIT_FAILURE);
    }

    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Error en la conexión");
        exit(EXIT_FAILURE);
    }

    printf("Conectado al broker (consumer por tiempo).\n");

    int bytes = recv(sock, buffer, sizeof(buffer)-1, 0);
    if (bytes <= 0) {
        perror("Error recibiendo READY");
        close(sock);
        exit(EXIT_FAILURE);
    }
    buffer[bytes] = '\0';
    printf("Broker dice: %s\n", buffer);

    while (1) {
	sleep(1);
        send(sock, "NEXT", strlen("NEXT"), 0);

	bytes = recv(sock, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            printf("Broker desconectado.\n");
            break;
        }

        if (bytes == sizeof(Message)) {
            Message* msg = (Message*)buffer;
            printf("Mensaje recibido => ID: %d, Contenido: %s\n", msg->id, msg->content);
            last_message_time = time(NULL); // reset timeout timer
        } else {
            buffer[bytes] = '\0';
            printf("Respuesta del broker: %s\n", buffer);
            if (strcmp(buffer, "NO_MESSAGES") != 0) {
                last_message_time = time(NULL); // reset timeout timer
            }
        }

        // Verificar si ha pasado demasiado tiempo sin recibir mensajes
        if (difftime(time(NULL), last_message_time) >= TIMEOUT_SECONDS) {
            printf("No se han recibido mensajes en %d segundos. Enviando mensaje de apagado al broker y cerrando.\n", TIMEOUT_SECONDS);
            send(sock, "SHUTDOWN", strlen("SHUTDOWN"), 0);
            break;
        }
    }

    close(sock);
    return 0;
}

