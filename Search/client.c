#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include "colors.h"

#define BUFFER_SIZE 100000
#define NM_PORT 6060
#define TIMEOUT_SEC 5

void send_read_request(int sock, const char *path) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "READ %s", path);
    send(sock, buffer, strlen(buffer), 0);

    printf(GREEN "Server response:\n" RESET);
    while (1) {
        int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_received < 0) {
            perror(RED "Error receiving data from server" RESET);
            break;
        }
        buffer[bytes_received] = '\0';  // Null-terminate the received data
        if (strncmp(buffer, "ERROR: ", 7) == 0) {
            printf(RED "%s" RESET, buffer);
            close(sock);
            break;
        }
        char *stop_ptr = strstr(buffer, "STOP");
        if (stop_ptr != NULL) {
            *stop_ptr = '\0';  // Null-terminate the buffer at the "STOP" position
            printf("%s", buffer);
            close(sock);
            break;
        }
        printf("%s", buffer);
    }
    printf("\n");
}

void send_write_request(int sock, const char *path, const char *data) {
    char buffer[BUFFER_SIZE];
    printf(YELLOW"Path: %s  Data: %s\n"RESET, path, data);
    snprintf(buffer, sizeof(buffer), "WRITE %s %s", path, data);
    printf(YELLOW"Sending: %s\n"RESET, buffer);
    send(sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }
}

void send_create_request(int sock, const char *path, const char *name) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "CREATE %s %s", path, name);
    send(sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }
}

void send_delete_request(int sock, const char *path) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "DELETE %s", path);
    send(sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }
}

void send_list_all_paths_request(int sock) {
    char buffer[100000];
    snprintf(buffer, sizeof(buffer), "LIST_ALL_PATHS");
    send(sock, buffer, strlen(buffer), 0);

    printf(GREEN "Server response:\n" RESET);
    while (1) {
        int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_received < 0) {
            perror(RED "Error receiving data from server" RESET);
            break;
        }
        buffer[bytes_received] = '\0';  // Null-terminate the received data
        if (strncmp(buffer, "ERROR: ", 7) == 0) {
            printf(RED "%s" RESET, buffer);
            break;
        }
        printf("%s", buffer);
        if (bytes_received < BUFFER_SIZE - 1) {
            break;
        }
    }
    printf("\n");
    close(sock);
}

void send_get_details_request(int sock, const char *path) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "GET_DETAILS %s", path);
    send(sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }
}

void send_stream_request(int sock, const char *path) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "STREAM %s", path);
    send(sock, buffer, strlen(buffer), 0);

    printf(GREEN "Server response:\n" RESET);

    // Set socket to non-blocking mode
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);

    // Set terminal to raw mode
    struct termios oldt, newt;
    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(ICANON | ECHO);
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);

    while (1) {
        int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_received > 0) {
            buffer[bytes_received] = '\0';  // Null-terminate the received data
            if (strncmp(buffer, "ERROR: ", 7) == 0) {
                printf(RED "%s" RESET, buffer);
                break;
            }
            printf("%s", buffer);
        } else if (bytes_received < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
            perror(RED "Error receiving data from server" RESET);
            break;
        }

        // Check for user input
        if (read(STDIN_FILENO, buffer, 1) > 0) {
            if (buffer[0] == 'x') {
                printf("\nStopping streaming...\n");
                snprintf(buffer, sizeof(buffer), "STOP");
                send(sock, buffer, strlen(buffer), 0);
                break;
            } else if (buffer[0] == 's') {
                printf("\nPausing streaming...\n");
                snprintf(buffer, sizeof(buffer), "PAUSE");
                send(sock, buffer, strlen(buffer), 0);
            } else if (buffer[0] == 'p') {
                printf("\nResuming streaming...\n");
                snprintf(buffer, sizeof(buffer), "RESUME");
                send(sock, buffer, strlen(buffer), 0);
            }
        }

        usleep(100000);  // Sleep for 100ms to reduce CPU usage
    }

    // Restore terminal settings
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);

    close(sock);
}

void find_storage_server(const char *nm_ip, int nm_port, const char *path, char *server_ip, int *server_port, char* command) {
    int nm_sock;
    struct sockaddr_in nm_addr;
    char buffer[BUFFER_SIZE];
    int bytes_received;
    int total_bytes_received = 0;

    nm_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(nm_port);
    if (inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr) <= 0) {
        perror("Invalid address or address not supported");
        close(nm_sock);
        exit(EXIT_FAILURE);
    }

    if (connect(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        perror(RED "Connection to Naming Server failed" RESET);
        close(nm_sock);
        exit(EXIT_FAILURE);
    }

    snprintf(buffer, sizeof(buffer), "FIND %s %s", path, command);
    printf("|%s|\n", buffer);
    send(nm_sock, buffer, strlen(buffer), 0);

    // Set up the timeout mechanism
    fd_set read_fds;
    struct timeval timeout;
    FD_ZERO(&read_fds);
    FD_SET(nm_sock, &read_fds);
    timeout.tv_sec = TIMEOUT_SEC;
    timeout.tv_usec = 0;

    // Wait for the initial ACK with timeout
    int select_result = select(nm_sock + 1, &read_fds, NULL, NULL, &timeout);
    if (select_result == -1) {
        perror("Error during select");
        close(nm_sock);
        exit(EXIT_FAILURE);
    } else if (select_result == 0) {
        fprintf(stderr, "Error: Timeout waiting for initial ACK from Naming Server\n");
        close(nm_sock);
        exit(EXIT_FAILURE);
    }

    // Receive data in a loop until we get both ACK and server info
    while ((bytes_received = recv(nm_sock, buffer + total_bytes_received, BUFFER_SIZE - 1 - total_bytes_received, 0)) > 0) {
        total_bytes_received += bytes_received;
        buffer[total_bytes_received] = '\0';
        // printf("buffer=%s\n", buffer);
        // Check for the initial ACK
        if (strstr(buffer, "ACK") != NULL) {
            // printf("ack received\n");
            // Check for the server info
            char *server_info = strstr(buffer, "SERVER IP:");
            if (server_info != NULL) {
                if (sscanf(server_info, "SERVER IP: %s PORT: %d", server_ip, server_port) == 2) {
                    close(nm_sock);
                    return;
                } else {
                    fprintf(stderr, "Error: Invalid server info format\n");
                    close(nm_sock);
                    exit(EXIT_FAILURE);
                }
            }
        }
    }

    if (bytes_received < 0) {
        perror("Error receiving data from Naming Server");
    } else {
        fprintf(stderr, "Error: Did not receive complete data from Naming Server\n");
    }

    close(nm_sock);
    exit(EXIT_FAILURE);
}

void send_copy_request(int sock, const char *source, const char *dest) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "COPY %s %s", source, dest);
    send(sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }
}

void log_c(int argc, char *argv[])
{
    const char *nm_ip = argv[1];
    int nm_port = atoi(argv[2]);
    int nm_sock;
    struct sockaddr_in nm_addr;
    char buffer[BUFFER_SIZE];
    const char *command = argv[3];
    const char *path = (argc >= 5) ? argv[4] : NULL;
    const char *data = (argc == 6) ? argv[5] : NULL;

    nm_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(nm_port);
    if (inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr) <= 0) {
        perror("Invalid address or address not supported");
        close(nm_sock);
        exit(EXIT_FAILURE);
    }

    if (connect(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        perror("Connection to Naming Server failed");
        close(nm_sock);
        exit(EXIT_FAILURE);
    }

    snprintf(buffer, sizeof(buffer), "%s %s", command, path);
    send(nm_sock, buffer, strlen(buffer), 0);

    int bytes_received = recv(nm_sock, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from Naming Server");
    } else {
        buffer[bytes_received] = '\0';
        printf("Server response: %s\n", buffer);
    }

    close(nm_sock);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <nm_ip> <nm_port> <command> <path> [<data>]\n", argv[0]);
        fprintf(stderr, "Commands: READ <path>, WRITE <path> <data>, CREATE <path> <name>, DELETE <path>, LIST_ALL_PATHS, GET_DETAILS <path>, STREAM <path>\n");
        exit(EXIT_FAILURE);
    }

    const char *nm_ip = argv[1];
    int nm_port = atoi(argv[2]);
    char *command = argv[3];
    char *path = (argc >= 5) ? argv[4] : NULL;
    char *data = (argc == 6) ? argv[5] : NULL;

    char server_ip[INET_ADDRSTRLEN];
    int server_port;

    // log_c(argc, argv);

    if (strcmp(command, "COPY") == 0) {
        // For COPY command, communicate with the Naming Server
        int nm_sock;
        struct sockaddr_in nm_addr;
        char buffer[BUFFER_SIZE];

        nm_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (nm_sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        nm_addr.sin_family = AF_INET;
        nm_addr.sin_port = htons(nm_port);
        if (inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr) <= 0) {
            perror("Invalid address or address not supported");
            close(nm_sock);
            exit(EXIT_FAILURE);
        }

        if (connect(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
            perror("Connection to Naming Server failed");
            close(nm_sock);
            exit(EXIT_FAILURE);
        }

        snprintf(buffer, sizeof(buffer), "COPY %s %s", path, data);
        send(nm_sock, buffer, strlen(buffer), 0);

        int bytes_received = recv(nm_sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_received < 0) {
            perror("Error receiving data from Naming Server");
        } else {
            buffer[bytes_received] = '\0';
            printf("Server response: %s\n", buffer);
        }

        close(nm_sock);
    } else if (strcmp(command, "LIST_ALL_PATHS") == 0) {
        // For LIST_ALL_PATHS command, communicate with the Naming Server
        int nm_sock;
        struct sockaddr_in nm_addr;
        char buffer[100000];

        nm_sock = socket(AF_INET, SOCK_STREAM, 0);
        if (nm_sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        nm_addr.sin_family = AF_INET;
        nm_addr.sin_port = htons(nm_port);
        if (inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr) <= 0) {
            perror("Invalid address or address not supported");
            close(nm_sock);
            exit(EXIT_FAILURE);
        }

        if (connect(nm_sock, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
            perror("Connection to Naming Server failed");
            close(nm_sock);
            exit(EXIT_FAILURE);
        }

        send_list_all_paths_request(nm_sock);

        close(nm_sock);
    } else if (strcmp(command, "GET_DETAILS") == 0) {
        find_storage_server(nm_ip, nm_port, path, server_ip, &server_port, command);

        int sock;
        struct sockaddr_in server_addr;

        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(server_port);
        if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
            perror("Invalid address or address not supported");
            close(sock);
            exit(EXIT_FAILURE);
        }

        if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("Connection to Storage Server failed");
            close(sock);
            exit(EXIT_FAILURE);
        }

        send_get_details_request(sock, path);
        close(sock);
    } else {
        find_storage_server(nm_ip, nm_port, path, server_ip, &server_port, command);

        int sock;
        struct sockaddr_in server_addr;

        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("Socket creation failed");
            exit(EXIT_FAILURE);
        }

        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(server_port);
        if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
            perror("Invalid address or address not supported");
            close(sock);
            exit(EXIT_FAILURE);
        }

        printf(RED"ip=%s port=%d sock=%d\n"RESET, server_ip, server_port, sock);
        printf("done\n");
        if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("Connection to Storage Server failed");
            close(sock);
            exit(EXIT_FAILURE);
        }

        // printf("command: |%s|\n", command);

        if (strcmp(command, "READ") == 0) {
            send_read_request(sock, path);
        } else if (strcmp(command, "WRITE") == 0) {
            if (data == NULL) {
                fprintf(stderr, "Please specify the data for WRITE command\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            send_write_request(sock, path, data);
        } else if (strcmp(command, "CREATE") == 0) {
            if (data == NULL) {
                fprintf(stderr, "Please specify the name for CREATE command\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            send_create_request(sock, path, data);
        } else if (strcmp(command, "DELETE") == 0) {
            send_delete_request(sock, path);
        } else if (strcmp(command, "STREAM") == 0) {
            send_stream_request(sock, path);
        } else {
            fprintf(stderr, "Unknown command: %s\n", command);
        }

        close(sock);
    }

    return 0;
}