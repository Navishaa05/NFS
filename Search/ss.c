#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include "colors.h"

#define NM_IP "127.0.0.1"
#define NM_PORT 6060
#define BUFFER_SIZE 100000
#define MAX_PATHS 100
#define MAX_PATH_LENGTH 1024

#define ERR_FILE_NOT_FOUND "ERROR: File not found"
#define ERR_UNKNOWN_COMMAND "ERROR: Unknown command"
#define ERR_CANNOT_WRITE_FILE "ERROR: Cannot write file"
#define ERR_FILE_BUSY "ERROR: File is currently being written to by another client"
#define ERR_CANNOT_DELETE "ERROR: Cannot delete file or directory"

pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

char accessible_paths[MAX_PATHS][200];
int accessible_path_count = 0;

void handle_sigchld(int sig) {
    int status;
    waitpid(-1, &status, WNOHANG);
    if (WIFEXITED(status) || WIFSIGNALED(status)) {
        // printf("Streaming stopped by client\n");
    }
}

void register_with_nm(const char *nm_ip, int nm_port, int client_port, const char paths[MAX_PATHS][MAX_PATH_LENGTH], int path_count) {
    int nm_socket;
    struct sockaddr_in nm_addr;
    char buffer[BUFFER_SIZE];
    
    nm_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_socket < 0) {
        perror(RED"Socket creation failed"RESET);
        exit(EXIT_FAILURE);
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(nm_port);
    inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr);

    if (connect(nm_socket, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        perror(RED"Connection to Naming Server failed"RESET);
        close(nm_socket);
        exit(EXIT_FAILURE);
    }

    char local_ip[INET_ADDRSTRLEN];
    struct sockaddr_in local_addr;
    socklen_t addr_len = sizeof(local_addr);
    getsockname(nm_socket, (struct sockaddr *)&local_addr, &addr_len);
    inet_ntop(AF_INET, &local_addr.sin_addr, local_ip, sizeof(local_ip));

    snprintf(buffer, sizeof(buffer), "REGISTER IP: %s PORT: %d PATHS: ", local_ip, client_port);
    printf("local_ip=%s\n", local_ip);
    for (int i = 0; i < path_count; i++) {
        if(path_count > 1 && i == 0) {
            continue;
        }
        strncat(buffer, paths[i], sizeof(buffer) - strlen(buffer) - 1);
        if (i < path_count - 1) {
            strncat(buffer, ",", sizeof(buffer) - strlen(buffer) - 1);
        }
    }
    send(nm_socket, buffer, strlen(buffer), 0);
    printf("buffer=%s\n", buffer);

    recv(nm_socket, buffer, BUFFER_SIZE, 0);
    printf(GREEN "Received from NM: %s\n"RESET, buffer);

    close(nm_socket);

    // Register accessible paths
    for (int i = 0; i < path_count; i++) {
        struct stat st;
        if (stat(paths[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            DIR *dir = opendir(paths[i]);
            if (dir != NULL) {
                struct dirent *entry;
                while ((entry = readdir(dir)) != NULL) {
                    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                        continue;
                    }
                    char full_path[MAX_PATH_LENGTH];
                    snprintf(full_path, sizeof(full_path), "%s/%s", paths[i], entry->d_name);
                    strncpy(accessible_paths[accessible_path_count++], full_path, MAX_PATH_LENGTH);
                    printf("path: %s\n", full_path);
                }
                closedir(dir);
            }
        } else {
            strncpy(accessible_paths[accessible_path_count++], paths[i], MAX_PATH_LENGTH);
        }
    }
}

int is_valid_accessible_path(const char *path) {
    for (int i = 0; i < accessible_path_count; i++) {
        if (strncmp(accessible_paths[i], path, strlen(accessible_paths[i])) == 0) {
            return 1;
        }
    }
    return 0;
}

void create_file_or_directory(int client_socket, const char *path, const char *type) {
    if (strcmp(type, "directory") == 0) {
        if (mkdir(path, 0755) == 0) {
            send(client_socket, "Directory created successfully", 30, 0);
        } else {
            perror("Error creating directory");
            send(client_socket, "ERROR: Cannot create directory", 31, 0);
        }
    } else if (strcmp(type, "file") == 0) {
        FILE *file = fopen(path, "w");
        if (file == NULL) {
            perror("Error creating file");
            send(client_socket, "ERROR: Cannot create file", 25, 0);
        } else {
            fclose(file);
            send(client_socket, "File created successfully", 24, 0);
        }
    } else {
        send(client_socket, "ERROR: Unknown type", 19, 0);
    }
}

char* copy_file_data(const char *source) {
    FILE *src = fopen(source, "rb");
    if (src == NULL) {
        perror("Error opening file for copying");
        return NULL;
    }

    fseek(src, 0, SEEK_END);
    long file_size = ftell(src);
    fseek(src, 0, SEEK_SET);

    char *data = malloc(file_size + 1);
    if (data == NULL) {
        perror("Error allocating memory for file copy");
        fclose(src);
        return NULL;
    }

    fread(data, 1, file_size, src);
    data[file_size] = '\0';

    fclose(src);
    return data;
}

int copy_file(const char *source, const char *dest) {
    FILE *src = fopen(source, "rb");
    if (src == NULL) {
        perror("Error opening source file");
        return -1;
    }

    FILE *dst = fopen(dest, "wb");
    if (dst == NULL) {
        perror("Error opening destination file");
        fclose(src);
        return -1;
    }

    char buffer[4096];
    size_t bytes;
    while ((bytes = fread(buffer, 1, sizeof(buffer), src)) > 0) {
        fwrite(buffer, 1, bytes, dst);
    }

    fclose(src);
    fclose(dst);
    return 0;
}

int copy_dir(const char *source, const char *dest) {
    struct stat st;
    if (stat(source, &st) != 0) {
        perror("Error getting source directory status");
        return -1;
    }

    if (mkdir(dest, st.st_mode) != 0 && errno != EEXIST) {
        perror("Error creating destination directory");
        return -1;
    }

    DIR *dir = opendir(source);
    if (dir == NULL) {
        perror("Error opening source directory");
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        char src_path[MAX_PATH_LENGTH];
        char dst_path[MAX_PATH_LENGTH];
        snprintf(src_path, sizeof(src_path), "%s/%s", source, entry->d_name);
        snprintf(dst_path, sizeof(dst_path), "%s/%s", dest, entry->d_name);

        if (entry->d_type == DT_DIR) {
            if (copy_dir(src_path, dst_path) != 0) {
                closedir(dir);
                return -1;
            }
        } else {
            if (copy_file(src_path, dst_path) != 0) {
                closedir(dir);
                return -1;
            }
        }
    }

    closedir(dir);
    return 0;
}

int delete_file_or_directory(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        perror("Error getting file or directory status");
        return -1;
    }

    if (S_ISDIR(st.st_mode)) {
        DIR *dir = opendir(path);
        if (dir == NULL) {
            perror("Error opening directory");
            return -1;
        }

        struct dirent *entry;
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            char full_path[MAX_PATH_LENGTH];
            snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);

            if (entry->d_type == DT_DIR) {
                if (delete_file_or_directory(full_path) != 0) {
                    closedir(dir);
                    return -1;
                }
            } else {
                if (remove(full_path) != 0) {
                    perror("Error deleting file");
                    closedir(dir);
                    return -1;
                }
            }
        }

        closedir(dir);

        if (rmdir(path) != 0) {
            perror("Error deleting directory");
            return -1;
        }
    } else {
        if (remove(path) != 0) {
            perror("Error deleting file");
            return -1;
        }
    }

    return 0;
}

void get_file_details(const char *path, char *details) {
    struct stat st;
    if (stat(path, &st) == 0) {
        snprintf(details, BUFFER_SIZE, "File: %s\nSize: %ld bytes\nPermissions: %o\nLast modified: %s",
                 path, st.st_size, st.st_mode & 0777, ctime(&st.st_mtime));
    } else {
        snprintf(details, BUFFER_SIZE, "ERROR: Cannot get file details");
    }
}

void stream_file(int client_socket, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
        return;
    }

    // Check if the file is a streamable type (e.g., mp3, wav)
    const char *ext = strrchr(path, '.');
    if (ext && (strcmp(ext, ".mp3") == 0 || strcmp(ext, ".wav") == 0)) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("Fork failed");
            send(client_socket, "ERROR: Fork failed", 18, 0);
            return;
        }

        if (pid == 0) {
            // In child process
            printf("Streaming file\n");
            fflush(stdout);
            execlp("ffplay", "ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", path, NULL);
            perror("Failed to execute ffplay");
            exit(1);
        } else {
            // In parent process
            signal(SIGCHLD, handle_sigchld);  // Set up signal handler for SIGCHLD

            int status;
            int paused = 0;
            while (1) {
                char command[BUFFER_SIZE];
                int bytes_received = recv(client_socket, command, sizeof(command) - 1, 0);
                if (bytes_received > 0) {
                    command[bytes_received] = '\0';
                    if (strcmp(command, "STOP") == 0) {
                        kill(pid, SIGTERM);  // Terminate the child process
                        printf("Streaming stopped by client\n");
                        break;
                    } else if (strcmp(command, "PAUSE") == 0) {
                        if (!paused) {
                            kill(pid, SIGSTOP);  // Pause the child process
                            paused = 1;
                            printf("Streaming paused by client\n");
                        }
                    } else if (strcmp(command, "RESUME") == 0) {
                        if (paused) {
                            kill(pid, SIGCONT);  // Resume the child process
                            paused = 0;
                            printf("Streaming resumed by client\n");
                        }
                    }
                } else if (bytes_received < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
                    perror("Error receiving data from client");
                    break;
                }

                int result = waitpid(pid, &status, WNOHANG);
                if (result == pid) {
                    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                        send(client_socket, "Streaming completed", 19, 0);
                    } else {
                        send(client_socket, "ERROR: Audio playback failed", 28, 0);
                    }
                    break;
                }

                usleep(100000);  // Sleep for 100ms to reduce CPU usage
            }
        }
    } else {
        send(client_socket, "ERROR: File is not a streamable type", 36, 0);
    }
}

void handle_client(int client_socket) {
    char buffer[BUFFER_SIZE];
    int bytes_read;

    bytes_read = recv(client_socket, buffer, BUFFER_SIZE, 0);

    printf(CYAN"Received %d bytes\n"RESET, bytes_read);

    if (bytes_read < 0) {
        perror(RED"Error reading from client"RESET);
        close(client_socket);
        return;
    }

    buffer[bytes_read] = '\0';
    printf(GREEN "Received request: "RESET);
    printf("%s\n",buffer);

    char command[15], path[256], data[BUFFER_SIZE], dest[256];
    sscanf(buffer, "%s %s %s", command, path, dest);

    // printf("command: %s\n", command);

    if (!is_valid_accessible_path(path)) {
        send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
        close(client_socket);
        return;
    }

    if (strcmp(command, "READ") == 0) {
        FILE *file = fopen(path, "r");
        if (file == NULL) {
            perror(RED"Error opening file"RESET);
            printf("hi sent\n");
            send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
            close(client_socket);  // Close the connection after sending the error message

        } else {
            while (fgets(buffer, BUFFER_SIZE, file) != NULL) {
                send(client_socket, buffer, strlen(buffer), 0);
            }
            fclose(file);
            send(client_socket, "STOP", 5, 0);
            close(client_socket);  // Close the connection after sending the response

        }
    } else if (strcmp(command, "WRITE") == 0) {
        sscanf(buffer + strlen(command) + strlen(path) + 2, "%[^\n]", data);

        pthread_mutex_lock(&file_mutex);

        FILE *file = fopen(path, "w");
        printf(YELLOW"Opening file %s\n"RESET, path);
        if (file == NULL) {
            perror(RED"Error opening file for writing"RESET);
            send(client_socket, ERR_CANNOT_WRITE_FILE, strlen(ERR_CANNOT_WRITE_FILE), 0);
            pthread_mutex_unlock(&file_mutex);
            close(client_socket);  // Close the connection after sending the error message

        } else {
            fprintf(file, "%s", data);
            fclose(file);
            send(client_socket, "File written successfully", 26, 0);
            pthread_mutex_unlock(&file_mutex);
            close(client_socket);  // Close the connection after sending the response

        }
    } else if (strcmp(command, "CREATE") == 0) {
        char name[256];
        sscanf(buffer + strlen(command) + strlen(path) + 2, "%s", name);
        char full_path[512];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, name);

        if (mkdir(full_path, 0755) == 0) {
            send(client_socket, "Directory created successfully", 30, 0);
        } else {
            perror("Error creating directory");
            send(client_socket, "ERROR: Cannot create directory", 31, 0);
        }
        close(client_socket);
    } else if (strcmp(command, "COPY") == 0) {
        struct stat st;
        if (stat(path, &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
                if (copy_dir(path, dest) == 0) {
                    send(client_socket, "Directory copied successfully", 29, 0);
                } else {
                    send(client_socket, "ERROR: Cannot copy directory", 28, 0);
                }
            } else {
                if (copy_file(path, dest) == 0) {
                    send(client_socket, "File copied successfully", 24, 0);
                } else {
                    send(client_socket, "ERROR: Cannot copy file", 24, 0);
                }
            }
        } else {
            perror("Error copying file or directory");
            send(client_socket, "ERROR: Cannot copy file or directory", 36, 0);
        }
        close(client_socket);
    } else if (strcmp(command, "DELETE") == 0) {
        if (delete_file_or_directory(path) == 0) {
            send(client_socket, "File or directory deleted successfully", 38, 0);
        } else {
            send(client_socket, ERR_CANNOT_DELETE, strlen(ERR_CANNOT_DELETE), 0);
        }
        close(client_socket);
    } else if (strcmp(command, "GET_DETAILS") == 0) {
        // printf("yayy\n");
        char details[BUFFER_SIZE];
        get_file_details(path, details);
        send(client_socket, details, strlen(details), 0);
        close(client_socket);
    } else if (strcmp(command, "STREAM") == 0) {
        stream_file(client_socket, path);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
        close(client_socket);  // Close the connection after sending the error message
    }
    close(client_socket);
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <nm_ip> <nm_port> <client_port> <accessible_paths>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *nm_ip = argv[1];
    int nm_port = atoi(argv[2]);
    int client_port = atoi(argv[3]);
    char paths[MAX_PATHS][MAX_PATH_LENGTH];
    int path_count = argc - 4;

    for (int i = 0; i < path_count; i++) {
        strncpy(paths[i], argv[i + 4], MAX_PATH_LENGTH);
    }

    int server_socket, client_socket;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len = sizeof(client_addr);

    register_with_nm(nm_ip, nm_port, client_port, paths, path_count);

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror(RED"Socket creation failed"RESET);
        exit(EXIT_FAILURE);
    }

    // Set the SO_REUSEADDR option
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror(RED "setsockopt(SO_REUSEADDR) failed" RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;  // Bind to all available interfaces
    server_addr.sin_port = htons(client_port);

        // After bind call
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror(RED"Bind failed"RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }
    printf("Server bound to IP %s and port %d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));
    
    // After listen call
    if (listen(server_socket, 5) < 0) {
        perror(RED"Listen failed"RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }
    printf("Server listening on IP %s and port %d\n", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

    // Print the IP address and port the Storage Server is running on
    char local_ip[INET_ADDRSTRLEN];
    struct sockaddr_in local_addr;
    socklen_t addr_len = sizeof(local_addr);
    getsockname(server_socket, (struct sockaddr *)&local_addr, &addr_len);
    inet_ntop(AF_INET, &local_addr.sin_addr, local_ip, sizeof(local_ip));
    printf("Storage Server is running on IP %s and port %d...\n", local_ip, client_port);

    while (1) {
        client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
        if (client_socket < 0) {
            perror(RED"Accept failed"RESET);
            continue;
        }

        handle_client(client_socket);
    }

    close(server_socket);
    return 0;
}