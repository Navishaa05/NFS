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
#include <termios.h>
#include <sys/select.h>
#include <fcntl.h>
#include <mpv/client.h> 
#include "colors.h"
#include "common.h"

#define NM_IP "127.0.0.1"
#define NM_PORT 6060
#define BUFFER_SIZE 100000
#define MAX_PATHS 100
#define MAX_PATH_LENGTH 1024
#define FILE_SIZE_THRESHOLD 1000000
#define MAX_ASYNC_TASKS 100

#define ERR_FILE_NOT_FOUND "ERROR: File not found"
#define ERR_UNKNOWN_COMMAND "ERROR: Unknown command"
#define ERR_CANNOT_WRITE_FILE "ERROR: Cannot write file"
#define ERR_FILE_BUSY "ERROR: File is currently being written to by another client"
#define ERR_CANNOT_DELETE "ERROR: Cannot delete file or directory"


int client_port;
int store_client_port;


char* copy_file_data(const char *source);
int copy_file(const char *source, const char *dest);
int copy_dir(const char *source, const char *dest);

pthread_mutex_t file_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct AsyncTask {
    char command[15];
    char path[256];
    char data[BUFFER_SIZE];
    char dest[256];
    int client_socket;
    struct AsyncTask *next;
} AsyncTask;

typedef struct AsyncQueue {
    AsyncTask *front;
    AsyncTask *rear;
    int size;
} AsyncQueue;

AsyncQueue async_queue = {NULL, NULL, 0};
pthread_mutex_t async_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t async_queue_cond = PTHREAD_COND_INITIALIZER;

void enqueue_async_task(AsyncTask *task) {
    pthread_mutex_lock(&async_queue_mutex);

    if (async_queue.size >= MAX_ASYNC_TASKS) {
        fprintf(stderr, "Async queue is full, dropping task\n");
        close(task->client_socket);
        free(task);
    } else {
        task->next = NULL;
        if (async_queue.rear == NULL) {
            async_queue.front = task;
            async_queue.rear = task;
        } else {
            async_queue.rear->next = task;
            async_queue.rear = task;
        }
        async_queue.size++;
        printf("Task enqueued: %s, Path: %s\n", task->command, task->path);
        pthread_cond_signal(&async_queue_cond);
    }

    pthread_mutex_unlock(&async_queue_mutex);
}

AsyncTask *dequeue_async_task() {
    pthread_mutex_lock(&async_queue_mutex);

    while (async_queue.size == 0) {
        pthread_cond_wait(&async_queue_cond, &async_queue_mutex);
    }

    AsyncTask *task = async_queue.front;
    async_queue.front = task->next;
    if (async_queue.front == NULL) {
        async_queue.rear = NULL;
    }
    async_queue.size--;
    printf("dequeued AsyncTask: %s, Path: %s\n", task->command, task->path);

    pthread_mutex_unlock(&async_queue_mutex);
    return task;
}

void *async_task_processor(void *arg) {
    while (1) {
        printf("in the async_task_processor function\n");
        AsyncTask *task = dequeue_async_task();

        send(task->client_socket, "ACK: Async operation processing started", 40, 0);

        if (strcmp(task->command, "WRITE") == 0) {
            printf("doing write for async\n");
            FILE *file = fopen(task->path, "w");
            // if (file) {
            //     fprintf(file, "%s", task->data);
            //     fclose(file);
            //     send(task->client_socket, "File written successfully", 26, 0);
            // } else {
            //     perror("Error writing file asynchronously");
            //     send(task->client_socket, "ERROR: Async write failed", 25, 0);
            // }
            if (file) {
                size_t offset = 0;
                size_t chunk_size = 2; // Process 1 KB at a time
                size_t data_len = strlen(task->data);

                while (offset < data_len) {
                    // Calculate how much data to write in this iteration
                    size_t bytes_to_write = (data_len - offset > chunk_size) ? chunk_size : (data_len - offset);

                    // Write the chunk to the file
                    if (fwrite(task->data + offset, 1, bytes_to_write, file) != bytes_to_write) {
                        perror("Error writing file asynchronously");
                        send(task->client_socket, "ERROR: Async write failed", 25, 0);
                        break;
                    }

                    offset += bytes_to_write;
                    // sleep(12);

                    // Notify client of progress
                    char progress_message[50];
                    snprintf(progress_message, sizeof(progress_message), "Progress: %zu/%zu bytes written", offset, data_len);
                    send(task->client_socket, progress_message, strlen(progress_message), 0);
                }

                fclose(file);
                if (offset == data_len) {
                    send(task->client_socket, "File written successfully", 26, 0);
                }
            } else {
                perror("Error opening file asynchronously");
                send(task->client_socket, "ERROR: Async write failed", 25, 0);
            }
            // const char *ack_message = "ACK";
            // send(task->client_socket, ack_message, strlen(ack_message), 0);
        }
        close(task->client_socket);
        free(task);
    }
    return NULL;
}

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

    recv(nm_socket, buffer, BUFFER_SIZE, 0);
    printf(GREEN "Received from NM: %s\n"RESET, buffer);

    close(nm_socket);

    // Register accessible paths
    for (int i = 0; i < path_count; i++) {
        struct stat st;
        if (stat(paths[i], &st) == 0) {
            if (S_ISDIR(st.st_mode)) {
            // Add the directory itself to accessible_paths
                strncpy(accessible_paths[accessible_path_count++], paths[i], MAX_PATH_LENGTH);
                printf("path (directory added): %s\n", paths[i]);

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
                        printf(CYAN"Registered path: %s\n"RESET, full_path);
                    }
                    closedir(dir);
                }
            } else {
                printf(YELLOW"Registered path: %s\n"RESET, paths[i]);
                strncpy(accessible_paths[accessible_path_count++], paths[i], MAX_PATH_LENGTH);
            }
        } else {
            // Handle the case where stat fails
            fprintf(stderr, "Error: Cannot access path %s\n", paths[i]);
        }
    }
}

int is_valid_accessible_path(const char *path) {
    for (int i = 0; i < accessible_path_count; i++) {
        printf("%s\n", accessible_paths[i]);
        if (strncmp(accessible_paths[i], path, strlen(accessible_paths[i])) == 0) {
            return 1;
        }
    }
    return 0;
}

void set_nonblocking_mode() {
    struct termios tty;
    tcgetattr(STDIN_FILENO, &tty);
    tty.c_lflag &= ~(ICANON | ECHO); // Disable canonical mode and echo
    tcsetattr(STDIN_FILENO, TCSANOW, &tty);
    fcntl(STDIN_FILENO, F_SETFL, O_NONBLOCK); // Set non-blocking mode
}

void send_status_update(const char *nm_ip, int nm_port, int client_port, int active) {

    printf("inside send_status_update\n");
    int nm_socket;
    struct sockaddr_in nm_addr;
    char buffer[BUFFER_SIZE];

    nm_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_socket < 0) {
        perror(RED"Socket creation failed"RESET);
        return;
    }

    nm_addr.sin_family = AF_INET;
    nm_addr.sin_port = htons(nm_port);
    inet_pton(AF_INET, nm_ip, &nm_addr.sin_addr);

    if (connect(nm_socket, (struct sockaddr *)&nm_addr, sizeof(nm_addr)) < 0) {
        perror(RED"Connection to Naming Server failed"RESET);
        close(nm_socket);
        return;
    }

    snprintf(buffer, sizeof(buffer), "STATUS IP: %s PORT: %d ACTIVE: %d", nm_ip, client_port, active);
    send(nm_socket, buffer, strlen(buffer), 0);

    printf("exiting status update\n");
    close(nm_socket);
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

    printf("Deleting %s\n", path);
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
        mpv_handle *mpv = mpv_create();
        if (!mpv) {
            perror("Failed to create mpv context");
            send(client_socket, "ERROR: Failed to create mpv context", 35, 0);
            return;
        }

        // Enable default key bindings, so the user can control playback
        mpv_set_option_string(mpv, "input-default-bindings", "yes");
        mpv_set_option_string(mpv, "input-vo-keyboard", "yes");

        // Initialize mpv
        if (mpv_initialize(mpv) < 0) {
            perror("Failed to initialize mpv");
            send(client_socket, "ERROR: Failed to initialize mpv", 31, 0);
            mpv_destroy(mpv);
            return;
        }

        // Load the file
        const char *cmd[] = {"loadfile", path, NULL};
        if (mpv_command(mpv, cmd) < 0) {
            perror("Failed to load file");
            send(client_socket, "ERROR: Failed to load file", 26, 0);
            mpv_destroy(mpv);
            return;
        }

        int paused = 0;

        while (1) {
            mpv_event *event = mpv_wait_event(mpv, 0);
            if (event->event_id == MPV_EVENT_END_FILE) {
                break;
            }

            // Check for commands from the client
            int flags = fcntl(client_socket, F_GETFL, 0);
            char buffer[BUFFER_SIZE];
            fcntl(client_socket, F_SETFL, flags | O_NONBLOCK);
            int command_received = recv(client_socket, buffer, sizeof(buffer) - 1, 0);
            if (command_received > 0) {
                buffer[command_received] = '\0';
                if (strcmp(buffer, "STOP") == 0) {
                    printf("Streaming stopped by client\n");
                    mpv_command_string(mpv, "stop");
                    break;
                } else if (strcmp(buffer, "PAUSE") == 0) {
                    printf("Streaming paused by client\n");
                    mpv_command_string(mpv, "set pause yes");
                    paused = 1;
                } else if (strcmp(buffer, "RESUME") == 0) {
                    printf("Streaming resumed by client\n");
                    mpv_command_string(mpv, "set pause no");
                    paused = 0;
                }
            }
        }

        mpv_terminate_destroy(mpv);
        send(client_socket, "STREAM_END", 10, 0); // Indicate end of stream
    } else {
        send(client_socket, "ERROR: File is not a streamable type", 36, 0);
    }
}

void get_ss_paths(int client_socket) {
    char buffer[2048];

    for (int i = 0; i < accessible_path_count; i++) {
        snprintf(buffer, sizeof(buffer), "%s\n", accessible_paths[i]);
        send(client_socket, buffer, strlen(buffer), 0);
    }
    char buffer2[BUFFER_SIZE];
    snprintf(buffer2, sizeof(buffer2), "%d\n", client_port);
    send(client_socket, buffer2, strlen(buffer2), 0);
    send(client_socket, "STOP", 5, 0); // Indicate end of paths
    close(client_socket);
}

void check_and_set_mode(char *mode, const char *data, size_t data_length) {
    size_t data_size = data_length;  // Data length passed to the function


    // Log data size for debugging
    printf("Data size: %zu, Threshold: %d\n", data_size, FILE_SIZE_THRESHOLD);

    printf("-------------------------------\n data: %s\n ----------------------\n", data);

    if (strcmp(mode, "--NOTSYNC") == 0) {
        printf("Mode is NOTSYNC. Checking data size...\n");
        if (data_size < FILE_SIZE_THRESHOLD) {
            printf("Data size %zu is less than threshold %d. Changing mode to SYNC.\n", data_size, FILE_SIZE_THRESHOLD);
            strcpy(mode, "--SYNC");
        } else {
            printf("Data size %zu exceeds threshold %d. Mode remains ASYNC.\n", data_size, FILE_SIZE_THRESHOLD);
            strcpy(mode, "--ASYNC");
        }
    } else {
        printf("Mode is not ASYNC: '%s'.\n", mode);
    }

    // Log current mode for debugging
    printf("Current mode: %s\n", mode);
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

    char command[15], path[256], data[BUFFER_SIZE], dest[256], mode[10];
    if( strstr(buffer, "WRITE")) {
        char *last_space = strrchr(buffer, ' ');
        if (last_space != NULL) {
            // if (strncmp(last_space + 1, "--", 2) == 0) {
            //     strncpy(mode, last_space + 1, sizeof(mode) - 1);
            //     mode[sizeof(mode) - 1] = '\0';
            //     *last_space = '\0';  // Remove mode from buffer
            // }
            strncpy(mode, last_space + 1, sizeof(mode) - 1);
            mode[sizeof(mode) - 1] = '\0';
            *last_space = '\0';
        }
        // Parse command and path
        sscanf(buffer, "%s %s", command, path);

        // Extract the data part (everything after the path)
        char *data_start = buffer + strlen(command) + strlen(path) + 2; // +2 for spaces
        strncpy(data, data_start, sizeof(data) - 1);
        data[sizeof(data) - 1] = '\0';

        printf("Parsed WRITE:\n");
        printf("  Command: %s\n", command);
        printf("  Path: %s\n", path);
        printf("  Data: %s\n", data);
        printf("  Mode: %s\n", mode);

        struct stat st;
        size_t data_length = strlen(data);
        printf("-------------------------------\n data: %s\n data_len: %ld\n ----------------------\n", data, data_length);
        check_and_set_mode(mode, data, data_length);

        printf("mode in hc after set: %s\n", mode);

        printf("back in handle_client\n");
        
    }
    else if (strstr(buffer, "COPY")) {
        // Parse COPY: command path dest
        sscanf(buffer, "%s %s %s", command, path, dest);

        printf("Parsed COPY:\n");
        printf("  Command: %s\n", command);
        printf("  Path: %s\n", path);
        printf("  Destination: %s\n", dest);
    }
    else
    {
        sscanf(buffer, "%s %s", command, path);
    }

    if (strcmp(command, "GET_STORAGE_PATHS") == 0) {
        get_ss_paths(client_socket);
        return;
    } 

    if (!is_valid_accessible_path(path) && ( strcmp(command, "CREATE") != 0 && strcmp(command, "WRITE") != 0) ) {
        printf(RED"Invalid path: %s\n\n"RESET, path);
        send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
        close(client_socket);
        return;
    }

    printf(CYAN"Valid path\n"RESET);

    if (strcmp(command, "READ") == 0) {
        printf("buffer: %s\n", buffer);
        FILE *file = fopen(path, "r");
        if (file == NULL) {
            perror(RED"Error opening file"RESET);
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
        if(strcmp(mode, "--ASYNC") == 0) {
            printf("222\n");
            AsyncTask* task = (AsyncTask*)malloc(sizeof(AsyncTask));
            strncpy(task->command, command, sizeof(task->command));
            strncpy(task->path, path, sizeof(task->path));
            // strncpy(task->dest, dest, sizeof(task->dest));
            task->client_socket = client_socket;                
            // char* data_start = (buffer + strlen(command) + strlen(path) + 2);
            // char *flag_position = strstr(data_start, "--ASYNC");
            // *flag_position = '\0';
            sscanf(data, "%[^\n]", task->data);
            enqueue_async_task(task);
            send(client_socket, "Task added to async queue", 26, 0);
            pthread_t async_thread;
            pthread_create(&async_thread, NULL, async_task_processor, NULL);

        } else {
            printf("444\n");
            printf("buffer: %s\n", buffer); 
            printf("path: %s\n", path);
            printf("data: %s\n", data);
            printf("555\n");
            FILE *file = fopen(path, "w");
            if (file == NULL) {
                perror(RED "Error opening file for writing" RESET);
                send(client_socket, ERR_CANNOT_WRITE_FILE, strlen(ERR_CANNOT_WRITE_FILE), 0);
            } else {
                fprintf(file, "%s", data);
                fclose(file);
                send(client_socket, "File written successfully", 26, 0);
            }
            pthread_mutex_unlock(&file_mutex);
        }
    } else if (strcmp(command, "CREATE") == 0) {
        char name[256];
        sscanf(buffer + strlen(command) + strlen(path) + 2, "%s", name);
        char full_path[512];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, name);

        if (mkdir(full_path, 0755) == 0) {
            char success_message[BUFFER_SIZE];
            snprintf(success_message, sizeof(success_message), "Directory created successfully: %s", full_path);
            send(client_socket, success_message, strlen(success_message), 0);
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
                    char success_message[BUFFER_SIZE];
                    snprintf(success_message, sizeof(success_message), "%s", dest);
                    send(client_socket, success_message, strlen(success_message), 0);
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
    client_port = atoi(argv[3]);

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
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(client_port);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror(RED"Bind failed"RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(server_socket, 5) < 0) {
        perror(RED"Listen failed"RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    printf("Storage Server is running and waiting for clients on port %d...\n", client_port);
        char local_ip[INET_ADDRSTRLEN];
    struct sockaddr_in local_addr;
    socklen_t addr_len = sizeof(local_addr);
    getsockname(server_socket, (struct sockaddr *)&local_addr, &addr_len);
    inet_ntop(AF_INET, &local_addr.sin_addr, local_ip, sizeof(local_ip));
    printf("Storage Server is running on IP %s and port %d...\n", local_ip, client_port);

    set_nonblocking_mode();

    int active = 1;
    fd_set read_fds;
    struct timeval timeout;
    
    while (1) {
        FD_ZERO(&read_fds);
        FD_SET(server_socket, &read_fds);
        FD_SET(STDIN_FILENO, &read_fds);
    
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000; // 100ms
    
        int max_fd = server_socket > STDIN_FILENO ? server_socket : STDIN_FILENO;
    
        int activity = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
    
        if (activity < 0 && errno != EINTR) {
            perror(RED"Select error"RESET);
            break;
        }
    
        if (FD_ISSET(STDIN_FILENO, &read_fds)) {
            char ch = getchar();
            if (ch == 'x') {
                active = 0;
                send_status_update(nm_ip, nm_port, client_port, active);
                printf("Storage Server paused. Press 'c' to continue.\n");
                continue;
            } else if (ch == 'c') {
                active = 1;
                send_status_update(nm_ip, nm_port, client_port, active);
                printf("Storage Server resumed.\n");
                continue;
            }
        }
    
        if (active && FD_ISSET(server_socket, &read_fds)) {
            client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
            if (client_socket < 0) {
                perror(RED"Accept failed"RESET);
                continue;
            }
    
            handle_client(client_socket);
        }
    }
    
    close(server_socket);
    return 0;
}