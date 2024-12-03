 #include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <errno.h>
#include <termios.h>
#include <mpv/client.h>
#include "colors.h"

#define BUFFER_SIZE 100000
// #define NM_IP "127.0.0.1"
// #define NM_PORT 6060
#define TIMEOUT_SEC 5

#define ERR_FILE_NOT_FOUND "ERROR: File not found"
#define ERR_UNKNOWN_COMMAND "ERROR: Unknown command"
#define ERR_CANNOT_WRITE_FILE "ERROR: Cannot write file"
#define ERR_FILE_BUSY "ERROR: File is currently being written to by another client"
#define ERR_CANNOT_DELETE "ERROR: Cannot delete file or directory"

void send_read_request(int sock, const char *path);

int from_backup = 0;

void for_backup_read(int sock, const char *path, const char* server_ip) {
    printf("Reading from backup path\n");
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "GET_STORAGE_PATHS");
    send(sock, buffer, strlen(buffer), 0);
    printf("%d\n", sock);

    int total_bytes_received = 0;
    while (1) {
        int bytes_received = recv(sock, buffer + total_bytes_received, BUFFER_SIZE - 1 - total_bytes_received, 0);
        if (bytes_received < 0) {
            perror(RED "Error receiving data from server" RESET);
            close(sock);
            return;
        }
        total_bytes_received += bytes_received;
        buffer[total_bytes_received] = '\0';  // Null-terminate the received data

        if (strstr(buffer, "STOP") != NULL) {
            break;
        }
    }

    printf(GREEN "All paths:\n" RESET);
    printf("%s\n", buffer);

    // Perform suffix string matching to find a valid matching path
    char *line = strtok(buffer, "\n");
    char matched_path[BUFFER_SIZE - 6] = {0};
    char *last_line = NULL;
    char *second_last_line = NULL;
    while (path[0] == '.' || path[0] == '/') {
        path++;
    }
    while (line != NULL) {
        second_last_line = last_line;
        last_line = line;
        if (strstr(line, "backup") != NULL) {
            snprintf(matched_path, sizeof(matched_path), "%s/%s", line, path);
            // break;
        }
        
        line = strtok(NULL, "\n");
    }

    int port_number;

    if (second_last_line != NULL) {
        port_number = atoi(second_last_line);
        printf(GREEN "Port number: %d\n" RESET, port_number);
    } else {
        printf(RED "Error: No port number found\n" RESET);
        close(sock);
        return;
    }

    if (strlen(matched_path) == 0) {
        printf(RED "Error: No matching path found\n" RESET);
        close(sock);
        return;
    }

    printf(GREEN "Matched path: %s\n" RESET, matched_path);

    // Close the socket after receiving the list of paths
    close(sock);

    // printf("Reading from backup path\n");
    // char buff[BUFFER_SIZE];
    // snprintf(buff, sizeof(buff), "GET_STORAGE_PATHS");
    // send(sock, buff, strlen(buff), 0);
    // char new_location[BUFFER_SIZE];
    // int bytes_received2 = recv(sock, new_location, sizeof(new_location), 0);
    //     if (bytes_received2 < 0) {
    //         perror(RED "Error receiving data from server" RESET);
    //         close(sock);
    //         return;
    //     }
    // printf("port: %s\n", new_location);

    // Re-establish the connection to the server
    struct sockaddr_in server_addr;
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_number);
    // server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
       if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("Invalid address or address not supported");
        close(sock);
        exit(EXIT_FAILURE);
    }

    printf("Connecting to Storage Server...\n");
    printf("ip: %s port: %d, socket: %d\n", server_ip, port_number, sock);

    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection to Storage Server failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

    // Send a read request for the matched path
    char buffer2[BUFFER_SIZE];
    snprintf(buffer2, sizeof(buffer2), "READ %s", matched_path);
    printf("%d, %s\n", sock, buffer2);

    if (send(sock, buffer2, strlen(buffer2), 0) < 0) {
        perror("Send failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

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

void send_read_request(int sock, const char *path) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "READ %s", path);
    // printf("in actual read request\n");
    // printf("%d, %s", sock, buffer);
    // send(sock, buffer, strlen(buffer), 0);
    if (send(sock, buffer, strlen(buffer), 0) < 0) {
        perror("Send failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

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

// void send_write_request(int sock, const char *path, const char *data) {
//     char buffer[BUFFER_SIZE];
//     snprintf(buffer, sizeof(buffer), "GET_STORAGE_PATHS");
//     send(sock, buffer, strlen(buffer), 0);
//     printf("%d\n", sock);

//     int total_bytes_received = 0;
//     while (1) {
//         int bytes_received = recv(sock, buffer + total_bytes_received, BUFFER_SIZE - 1 - total_bytes_received, 0);
//         if (bytes_received < 0) {
//             perror(RED "Error receiving data from server" RESET);
//             close(sock);
//             return;
//         }
//         total_bytes_received += bytes_received;
//         buffer[total_bytes_received] = '\0';  // Null-terminate the received data

//         if (strstr(buffer, "STOP") != NULL) {
//             break;
//         }
//     }
//     printf(GREEN "All paths:\n" RESET);
//     printf("%s\n", buffer);

//     char *line = strtok(buffer, "\n");
//     char matched_path[BUFFER_SIZE - 6] = {0};
//     char *last_line = NULL;
//     char *second_last_line = NULL;
//     while (path[0] == '.' || path[0] == '/') {
//         path++;
//     }
//     while (line != NULL) {
//         second_last_line = last_line;
//         last_line = line;
//         printf("line: %s\n", line);
//         if (strstr(line, "backup") != NULL) {
//             snprintf(matched_path, sizeof(matched_path), "%s/%s", line, path);
//             // break;
//         }
        
//         line = strtok(NULL, "\n");
//     }

//     int port_number;

//     printf("second_last_line: %s\n", second_last_line);

//     if (second_last_line != NULL) {
//         port_number = atoi(second_last_line);
//         printf(GREEN "Port number: %d\n" RESET, port_number);
//     } else {
//         printf(RED "Error: No port number found\n" RESET);
//         close(sock);
//         return;
//     }

//     if (strlen(matched_path) == 0) {
//         printf(RED "Error: No matching path found\n" RESET);
//         close(sock);
//         return;
//     }

//     printf(GREEN "Matched path: %s\n" RESET, matched_path);

//     close(sock);

//     struct sockaddr_in server_addr;
//     sock = socket(AF_INET, SOCK_STREAM, 0);
//     if (sock < 0) {
//         perror("Socket creation failed");
//         exit(EXIT_FAILURE);
//     }

//     server_addr.sin_family = AF_INET;
//     server_addr.sin_port = htons(port_number);
//     server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

//     if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
//         perror("Connection to Storage Server failed");
//         close(sock);
//         exit(EXIT_FAILURE);
//     }

//     memset(buffer, 0, BUFFER_SIZE);
//     printf(YELLOW"Path: %s  Data: %s\n"RESET, path, data);
//     snprintf(buffer, sizeof(buffer), "WRITE %s %s", path, data);
//     printf(YELLOW"Sending: %s\n"RESET, buffer);
//     send(sock, buffer, strlen(buffer), 0);

//     int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
//     if (bytes_received < 0) {
//         perror("Error receiving data from server");
//     } else {
//         buffer[bytes_received] = '\0';
//         printf("Server response: %s\n", buffer);
//     }

//     // Write to backup servers
//     for (int i = 0; i < 2; i++) {
//         // Query the naming server to get the backup server
//         int naming_sock = socket(AF_INET, SOCK_STREAM, 0);
//         if (naming_sock < 0) {
//             perror("Socket creation failed");
//             exit(EXIT_FAILURE);
//         }

//         struct sockaddr_in naming_server_addr;
//         naming_server_addr.sin_family = AF_INET;
//         naming_server_addr.sin_port = htons(port_number);
//         naming_server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

//         if (connect(naming_sock, (struct sockaddr *)&naming_server_addr, sizeof(naming_server_addr)) < 0) {
//             perror("Connection to Naming Server failed");
//             close(naming_sock);
//             exit(EXIT_FAILURE);
//         }

//         snprintf(buffer, sizeof(buffer), "FIND %s", matched_path);
//         send(naming_sock, buffer, strlen(buffer), 0);

//         total_bytes_received = 0;
//         while (1) {
//             bytes_received = recv(naming_sock, buffer + total_bytes_received, BUFFER_SIZE - 1 - total_bytes_received, 0);
//             if (bytes_received < 0) {
//                 perror(RED "Error receiving data from naming server" RESET);
//                 close(naming_sock);
//                 return;
//             }
//             total_bytes_received += bytes_received;
//             buffer[total_bytes_received] = '\0';  // Null-terminate the received data

//             if (strstr(buffer, "STOP") != NULL) {
//                 break;
//             }
//         }
//         close(naming_sock);

//         // Extract the backup server IP and port

//         printf("now starting backup\n");
//         char backup_ip[INET_ADDRSTRLEN];
//         int backup_port;
//         sscanf(buffer, "SERVER IP: %s PORT: %d", backup_ip, &backup_port);

//         // Write to the backup server
//         int backup_sock = socket(AF_INET, SOCK_STREAM, 0);
//         if (backup_sock < 0) {
//             perror("Socket creation failed");
//             exit(EXIT_FAILURE);
//         }

//         struct sockaddr_in backup_server_addr;
//         backup_server_addr.sin_family = AF_INET;
//         backup_server_addr.sin_port = htons(backup_port);
//         inet_pton(AF_INET, backup_ip, &backup_server_addr.sin_addr);

//         if (connect(backup_sock, (struct sockaddr *)&backup_server_addr, sizeof(backup_server_addr)) < 0) {
//             perror("Connection to Backup Storage Server failed");
//             close(backup_sock);
//             exit(EXIT_FAILURE);
//         }

//         memset(buffer, 0, BUFFER_SIZE);
//         snprintf(buffer, sizeof(buffer), "WRITE %s %s", path, data);
//         send(backup_sock, buffer, strlen(buffer), 0);

//         bytes_received = recv(backup_sock, buffer, BUFFER_SIZE - 1, 0);
//         if (bytes_received < 0) {
//             perror("Error receiving data from backup server");
//         } else {
//             buffer[bytes_received] = '\0';
//             printf("Backup server response: %s\n", buffer);
//         }

//         close(backup_sock);
//     }
// }

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

        // Extract the path from the server response
        char received_path[BUFFER_SIZE];
        sscanf(buffer, "Directory created successfully: %s", received_path);
        printf("Extracted path: %s\n", received_path);

        // Store the path in a variable
        char stored_path[BUFFER_SIZE];
        strncpy(stored_path, received_path, sizeof(stored_path) - 1);
        stored_path[sizeof(stored_path) - 1] = '\0';

        printf("Stored path: %s\n", stored_path);
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
    char buffer[BUFFER_SIZE];
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

    int pipe_fd[2];
    if (pipe(pipe_fd) == -1) {
        perror("pipe");
        return;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return;
    }

    if (pid == 0) {
        // In child process
        close(pipe_fd[1]); // Close write end of the pipe

        // Initialize mpv
        mpv_handle *mpv = mpv_create();
        if (!mpv) {
            fprintf(stderr, "failed to create mpv context\n");
            exit(1);
        }

        // Enable default key bindings, so the user can control playback
        mpv_set_option_string(mpv, "input-default-bindings", "yes");
        mpv_set_option_string(mpv, "input-vo-keyboard", "yes");

        // Initialize mpv
        if (mpv_initialize(mpv) < 0) {
            fprintf(stderr, "failed to initialize mpv\n");
            exit(1);
        }

        // Play from stdin
        const char *cmd[] = {"loadfile", "fd://0", NULL};
        if (mpv_command(mpv, cmd) < 0) {
            fprintf(stderr, "failed to load file\n");
            exit(1);
        }

        // Redirect stdin to read end of the pipe
        dup2(pipe_fd[0], STDIN_FILENO);

        // Handle mpv events
        while (1) {
            mpv_event *event = mpv_wait_event(mpv, -1);
            if (event->event_id == MPV_EVENT_END_FILE) {
                break;
            }
        }

        mpv_terminate_destroy(mpv);
        exit(0);
    } else {
        // In parent process
        close(pipe_fd[0]); // Close read end of the pipe

        // Set socket to non-blocking mode
        int flags = fcntl(sock, F_GETFL, 0);
        fcntl(sock, F_SETFL, flags | O_NONBLOCK);

        // Set terminal to non-blocking mode
        struct termios oldt, newt;
        tcgetattr(STDIN_FILENO, &oldt);
        newt = oldt;
        newt.c_lflag &= ~(ICANON | ECHO);
        tcsetattr(STDIN_FILENO, TCSANOW, &newt);
        fcntl(STDIN_FILENO, F_SETFL, O_NONBLOCK);

        int paused = 0;

        while (1) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(sock, &read_fds);
            FD_SET(STDIN_FILENO, &read_fds);

            int max_fd = sock > STDIN_FILENO ? sock : STDIN_FILENO;
            struct timeval timeout = {0, 5000}; // 50ms timeout

            int activity = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

            if (activity < 0 && errno != EINTR) {
                perror("select error");
                break;
            }

            if (FD_ISSET(sock, &read_fds)) {
                int bytes_received = recv(sock, buffer, BUFFER_SIZE - 1, 0);
                if (bytes_received > 0) {
                    buffer[bytes_received] = '\0';  // Null-terminate the received data
                    if (strncmp(buffer, "ERROR: ", 7) == 0) {
                        printf(RED "%s" RESET, buffer);
                        break;
                    }
                    if (strncmp(buffer, "STREAM_END", 10) == 0) {
                        break;
                    }
                    write(pipe_fd[1], buffer, bytes_received);
                } else if (bytes_received < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
                    perror(RED "Error receiving data from server" RESET);
                    break;
                }
            }

            if (FD_ISSET(STDIN_FILENO, &read_fds)) {
                int input = read(STDIN_FILENO, buffer, 1);
                if (input > 0) {
                    if (buffer[0] == 'x') {
                        printf("\nStopping streaming...\n");
                        snprintf(buffer, sizeof(buffer), "STOP");
                        send(sock, buffer, strlen(buffer), 0);
                        break;
                    } else if (buffer[0] == 's' && !paused) {
                        printf("\nPausing streaming...\n");
                        snprintf(buffer, sizeof(buffer), "PAUSE");
                        send(sock, buffer, strlen(buffer), 0);
                        paused = 1;
                    } else if (buffer[0] == 'p' && paused) {
                        printf("\nResuming streaming...\n");
                        snprintf(buffer, sizeof(buffer), "RESUME");
                        send(sock, buffer, strlen(buffer), 0);
                        paused = 0;
                    }
                }
            }
        }

        // Restore terminal settings
        tcsetattr(STDIN_FILENO, TCSANOW, &oldt);

        close(pipe_fd[1]); // Close write end of the pipe
        waitpid(pid, NULL, 0); // Wait for child process to finish
    }

    close(sock);
}

void find_storage_server(const char *nm_ip, int nm_port, const char *path, char *server_ip, int *server_port) {
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

    snprintf(buffer, sizeof(buffer), "FIND %s", path);
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

        // Check for the initial ACK
        if (strstr(buffer, "ACK") != NULL) {
            // Check for the server info
            char *server_info = strstr(buffer, "SERVER IP:");
            if (server_info != NULL) {
                if (strstr(server_info, "(from backup)") != NULL) {
                    from_backup = 1;
                    if (sscanf(server_info, "SERVER IP: %s PORT: %d (from backup)", server_ip, server_port) == 2) {
                        close(nm_sock);
                        return;
                    } else {
                        fprintf(stderr, "Error: Invalid server info format\n");
                        close(nm_sock);
                        exit(EXIT_FAILURE);
                    }
                } else {
                    from_backup = 0;
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
    }

    printf(YELLOW"Received data: %s\n"RESET, buffer);

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

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <nm_ip> <nm_port> <command> <path> [<data>]\n", argv[0]);
        fprintf(stderr, "Commands: READ <path>, WRITE <path> <data>, CREATE <path> <name>, DELETE <path>, LIST_ALL_PATHS, GET_DETAILS <path>, STREAM <path>\n");
        exit(EXIT_FAILURE);
    }

    const char *nm_ip = argv[1];
    int nm_port = atoi(argv[2]);
    const char *command = argv[3];
    char *path = (argc >= 5) ? argv[4] : NULL;
    const char *data = (argc == 6) ? argv[5] : NULL;

    char server_ip[INET_ADDRSTRLEN];
    int server_port;

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

        send_list_all_paths_request(nm_sock);

        close(nm_sock);
    } else if (strcmp(command, "GET_DETAILS") == 0) {
        find_storage_server(nm_ip, nm_port, path, server_ip, &server_port);

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
        find_storage_server(nm_ip, nm_port, path, server_ip, &server_port);

        int nm_sock;
        struct sockaddr_in nm_addr;
        char buffer[BUFFER_SIZE];
        char backup1[2000] = "";
        char backup2[2000] = "";
        
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
        
        memset(buffer, 0, BUFFER_SIZE);
        snprintf(buffer, sizeof(buffer), "LIST_ALL_PATHS");
        send(nm_sock, buffer, strlen(buffer), 0);
        
        // Receive the response from the naming server
        int total_bytes_received = 0;
        while (1) {
            int bytes_received = recv(nm_sock, buffer + total_bytes_received, BUFFER_SIZE - 1 - total_bytes_received, 0);
            if (bytes_received < 0) {
                perror(RED "Error receiving data from server" RESET);
                break;
            }
            total_bytes_received += bytes_received;
            buffer[total_bytes_received] = '\0';  // Null-terminate the received data
            if (strncmp(buffer, "ERROR: ", 7) == 0) {
                printf(RED "%s" RESET, buffer);
                break;
            }
            if (bytes_received < BUFFER_SIZE - 1) {
                break;
            }
        }
        
        // Store the paths in an array
        char *paths[10000];
        int path_count = 0;
        char *line = strtok(buffer, "\n");
        while (line != NULL && path_count < 10000) {
            paths[path_count++] = strdup(line);
            line = strtok(NULL, "\n");
        }
        
        // Extract the directory path from the original path
        char dir_path[2048];
        strncpy(dir_path, path, sizeof(dir_path));
        char *last_slash = strrchr(dir_path, '/');
        if (last_slash != NULL) {
            *last_slash = '\0';  // Terminate the string at the last '/'
        }
        
        // Construct the comparison string
        char comparison_path[3000];
        char* new_dir_path = (char*)malloc(strlen(dir_path) + 1);
        strcpy(new_dir_path, dir_path);
        while (new_dir_path[0] == '.' || new_dir_path[0] == '/') {
            new_dir_path++;
        }

        snprintf(comparison_path, sizeof(comparison_path), "/backup/./%s", new_dir_path);
        char full_path[3000];
        char store_path[3000];
        strcpy(store_path, path);
        // while(path[0] == '.' || path[0] == '/') {
        //     path++;
        // }
        snprintf(full_path, sizeof(full_path), "/backup/%s", path);

        char file_path[100];
        snprintf(file_path, sizeof(file_path), "%s", full_path+strlen(comparison_path));

        if(strcmp(command, "CREATE") == 0 ) {
            strcat(comparison_path, file_path);
        }

        printf(GREEN "Comparison path: %s\n" RESET, comparison_path);
        printf(GREEN "Full path: %s\n" RESET, full_path);

        printf(GREEN "File path: %s\n" RESET, file_path);
        
        // Print the array and find backup paths
        printf(GREEN "All accessible paths:\n" RESET);
        for (int i = 0; i < path_count; i++) {
            printf(CYAN"%s\n"RESET, paths[i]);
            if (strstr(paths[i], full_path) != NULL) {
                if(strcmp(command, "CREATE") == 0) {
                    // int ind = strlen(comparison_path);
                    int ind = strlen(paths[i]);
                    int offset = ind - strlen(comparison_path);
                    printf("current path: %s\n", paths[i]+offset);
                    if(strcmp(paths[i] + offset, comparison_path) != 0) {
                        continue;
                    }
                }

                printf(RED"CORRECT COMPARE: %s\n"RESET, paths[i]);
                if (strlen(backup1) == 0) {
                    strncpy(backup1, paths[i], 2048);
                } else if (strlen(backup2) == 0 && strncmp(paths[i], backup1, strlen(backup1)) != 0) {
                    strncpy(backup2, paths[i], 2048);
                }
            }
            free(paths[i]);  // Free the duplicated strings
        }

        // printf(CYAN "path: %s\n" RESET, path);  

        char newBackup1[3000];
        snprintf(newBackup1, sizeof(newBackup1), "%s%s", backup1, file_path);

        char newBackup2[3000];
        snprintf(newBackup2, sizeof(newBackup2), "%s%s", backup2, file_path);

        // snprintf(backup1, sizeof(backup1), "%s%s", backup1, file_path);
        // snprintf(backup2, sizeof(backup2), "%s%s", backup2, file_path);
        
        
        printf(GREEN "Backup1: %s\n" RESET, backup1);
        printf(GREEN "Backup2: %s\n" RESET, backup2);
        // printf(GREEN "Backup1: %s\n" RESET, newBackup1);
        // printf(GREEN "Backup2: %s\n" RESET, newBackup2);
        
        close(nm_sock);

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

        if (strcmp(command, "READ") == 0) {
            printf("from_backup: %d\n", from_backup);
            if(from_backup) {
                for_backup_read(sock, path, server_ip);
            }
            else{
                send_read_request(sock, path);  
            }
        } else if (strcmp(command, "WRITE") == 0) {
            if (data == NULL) {
                fprintf(stderr, "Please specify the data for WRITE command\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            if(from_backup) {
                printf("Cannot write to backup server\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            else{
                send_write_request(sock, path, data);

                find_storage_server(nm_ip, nm_port, backup1, server_ip, &server_port);
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

                send_write_request(sock, backup1, data);

                find_storage_server(nm_ip, nm_port, backup2, server_ip, &server_port);
                // int sock;
                // struct sockaddr_in server_addr;

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

                send_write_request(sock, backup2, data);
            }
            // send_write_request(sock, path, data);
        } else if (strcmp(command, "CREATE") == 0) {
            if (data == NULL) {
                fprintf(stderr, "Please specify the name for CREATE command\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            if(from_backup) {
                printf("Cannot create on backup server\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            else{
                send_create_request(sock, path, data);

                printf(CYAN "Backup1: %s\n" RESET, backup1);
                printf(CYAN "Backup2: %s\n" RESET, backup2);

                find_storage_server(nm_ip, nm_port, backup1, server_ip, &server_port);
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

                send_create_request(sock, backup1, data);

                find_storage_server(nm_ip, nm_port, backup2, server_ip, &server_port);
                // int sock;
                // struct sockaddr_in server_addr;
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

                send_create_request(sock, backup2, data);
            }
            // send_create_request(sock, path, data);
        } else if (strcmp(command, "DELETE") == 0) {
            if(from_backup) {
                printf("Cannot delete from backup server\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            else{
                char path_copy[3000];
                // strcpy(path_copy, "./");
                strcat(path_copy, path);
                send_delete_request(sock, path_copy);

                find_storage_server(nm_ip, nm_port, backup1, server_ip, &server_port);

                printf(RED"server_port1: %d\n"RESET, server_port);

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

                send_delete_request(sock, backup1);

                find_storage_server(nm_ip, nm_port, backup2, server_ip, &server_port);


                printf(RED"server_port2: %d\n"RESET, server_port);

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

                send_delete_request(sock, backup2);
            }
            // send_delete_request(sock, path);
        } else if (strcmp(command, "STREAM") == 0) {
            send_stream_request(sock, path);
        } else {
            fprintf(stderr, "Unknown command: %s\n", command);
        }

        close(sock);
    }

    return 0;
}