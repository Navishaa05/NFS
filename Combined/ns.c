#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <dirent.h>
#include <errno.h>  // Include errno.h for errno and EEXIST
#include <sys/stat.h>  // Include sys/stat.h for struct stat, stat, and S_ISDIR
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <termios.h>
#include <fcntl.h>
#include <netdb.h>
#include "colors.h"
#include "common.h"

#define PORT 6060
#define BUFFER_SIZE 100000
#define MAX_PATHS 100
#define MAX_PATH_LENGTH 1024
#define MAX_SERVERS 50
#define HASHMAP_SIZE 100  // Define size for the hashmap
#define LRU_SIZE 10  // Define size for the LRU cache
#define MAX_LINES 10
#define MAX_LINE_LENGTH 256

// Error codes
#define ERR_FILE_NOT_FOUND "ERROR: File not found"
#define ERR_UNKNOWN_COMMAND "ERROR: Unknown command"
#define ERR_MAX_SERVERS_REACHED "ERROR: Maximum number of storage servers reached"
#define ERR_FILE_BUSY "ERROR: File is currently being written to by another client"

int temp_port;

typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
    char paths[MAX_PATHS][200];
    int path_count;
    int active;
} StorageServer;

StorageServer servers[MAX_SERVERS];
int server_count = 0;
pthread_mutex_t server_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct HashMapEntry {
    char path[MAX_PATH_LENGTH];
    StorageServer *server;
    struct HashMapEntry *next;  // For chaining in case of collisions
} HashMapEntry;

HashMapEntry *hashmap[HASHMAP_SIZE];  // Hashmap for storing paths

typedef struct {
    char path[MAX_PATH_LENGTH];
    int port;
} PathInfo;

PathInfo found_backup_paths[2];
int found_count = 0;

typedef struct {
    char path[MAX_PATH_LENGTH];
    StorageServer *server;  // Pointer to the associated server
} LRUCacheEntry;

LRUCacheEntry lru_cache[LRU_SIZE];  // Circular array to store paths and server pointers
int lru_head = 0;  // Index for the most recently accessed path

// Hash function to compute the index for a given path
unsigned int hash(const char *path) {
    unsigned int hash = 0;
    while (*path) {
        hash = (hash << 5) + *path++;  // Simple hash function
    }
    return hash % HASHMAP_SIZE;
}

int copy_file(const char *source, const char *dest) {
    printf(PINK"Copying file %s to %s\n"RESET, source, dest);
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


        // Skip directories that contain "backup" in their path
        if (strstr(src_path, "backup") != NULL) {
            continue;
        }
        
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

// Function to insert a path into the hashmap
void insert_into_hashmap(const char *path, StorageServer *server) {
    // printf("entered insert_into_hashmap\n");  // Debug statement
    unsigned int index = hash(path);
    HashMapEntry *new_entry = malloc(sizeof(HashMapEntry));
    strncpy(new_entry->path, path, MAX_PATH_LENGTH);
    
    // Allocate memory for server
    new_entry->server = malloc(sizeof(StorageServer));
    if (new_entry->server != NULL) {
        memcpy(new_entry->server, server, sizeof(StorageServer));
    }

    new_entry->next = hashmap[index];
    hashmap[index] = new_entry;  // Insert at the head of the list
}

void redirect_request_to_storage_server(int client_socket, const char *request, const char *server_ip, int server_port);

char* get_ip_address() {
    struct ifaddrs *ifaddr, *ifa;
    int family, s;
    static char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1) {
        perror("getifaddrs");
        exit(EXIT_FAILURE);
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET) {
            s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                printf("getnameinfo() failed: %s\n", gai_strerror(s));
                exit(EXIT_FAILURE);
            }

            // Skip loopback address
            if (strcmp(host, "127.0.0.1") != 0) {
                break;
            }
        }
    }

    freeifaddrs(ifaddr);
    return host;
}

void set_nonblocking_mode() {
    struct termios tty;
    tcgetattr(STDIN_FILENO, &tty);
    tty.c_lflag &= ~(ICANON | ECHO); // Disable canonical mode and echo
    tcsetattr(STDIN_FILENO, TCSANOW, &tty);
    fcntl(STDIN_FILENO, F_SETFL, O_NONBLOCK); // Set non-blocking mode
}

int find_in_hashmap(char* path) {
   HashMapEntry *entry = hashmap[hash(path)];
    while (entry != NULL) {
        // Check if the path matches the entry's path
        if (strcmp(entry->path, path) == 0) {
            return 1;
        }
        entry = entry->next;  // Move to the next entry in the linked list
    }
    return 0;
}

void traverse_directory(const char *base_path, StorageServer *server) {
    // printf("entered traverse_directory\n");  // Debug statement
    struct stat st;
    if (stat(base_path, &st) == 0 && S_ISDIR(st.st_mode)) {
        DIR *dir = opendir(base_path);
        if (dir != NULL) {
            struct dirent *sub_entry;
            while ((sub_entry = readdir(dir)) != NULL) {
                if (strcmp(sub_entry->d_name, ".") == 0 || strcmp(sub_entry->d_name, "..") == 0) {
                    continue;
                }
                char full_path[MAX_PATH_LENGTH];
                snprintf(full_path, sizeof(full_path), "%s/%s", base_path, sub_entry->d_name);

                // Check if the new path can be added to the server's paths
                if (server->path_count < MAX_PATHS) {
                    strncpy(server->paths[server->path_count], full_path, MAX_PATH_LENGTH);
                    // if(find_in_hashmap(server->paths[server->path_count]))
                    //     continue;
                    insert_into_hashmap(server->paths[server->path_count], server);  // Update hashmap with new path
                    server->path_count++;  // Increment path count after insertion
                }

                // Recursively traverse subdirectories
                traverse_directory(full_path, server);
            }
            closedir(dir);
        }
    }
}

void process_hashmap() {
    // printf("entered process_hashmap\n");  // Debug statement
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashMapEntry *entry = hashmap[i];
        while (entry != NULL) {
            traverse_directory(entry->path, entry->server);
            entry = entry->next;  // Move to the next entry in the hashmap
        }
    }
}

void check()
{
    printf("Check HashMap Entries:\n");
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashMapEntry *entry = hashmap[i];
        while (entry != NULL) {
            printf("Path: %s, Server IP: %s, Server Port: %d, Active: %d\n", 
                   entry->path, 
                   entry->server->ip, 
                   entry->server->port,
                   entry->server->active);
            entry = entry->next;  // Move to the next entry in the linked list
        }
    }
    printf("\n");
}

// Function to check if a path is a backup folder
int is_backup_folder(const char *path) {
    return strstr(path, "/backup") != NULL;
}

void register_storage_server(char *request) {
    pthread_mutex_lock(&server_mutex);

    if (server_count >= MAX_SERVERS) {
        printf("%s\n", ERR_MAX_SERVERS_REACHED);
        pthread_mutex_unlock(&server_mutex);
        return;
    }

    StorageServer new_server;
    char paths[BUFFER_SIZE];
    sscanf(request, "REGISTER IP: %s PORT: %d PATHS: %[^\n]", new_server.ip, &new_server.port, paths);

    char *token = strtok(paths, ",");
    new_server.path_count = 0;
    while (token != NULL && new_server.path_count < MAX_PATHS) {
        strncpy(new_server.paths[new_server.path_count], token, MAX_PATH_LENGTH);
        insert_into_hashmap(new_server.paths[new_server.path_count], &new_server);  // Store in hashmap
        new_server.path_count++;  // Increment path count after insertion
        token = strtok(NULL, ",");
    }

    // Mark every third server as inactive
    // new_server.active = (server_count % 3 != 2);

    new_server.active = 1;

    printf("request: %s\n", request);

    // Check if the first path is a directory and if no specific files/subdirectories are listed
    if (new_server.path_count == 1) {
        struct stat st;
        if (stat(new_server.paths[0], &st) == 0 && S_ISDIR(st.st_mode)) {
            printf("Directory detected: %s\n", new_server.paths[0]);
            DIR *dir = opendir(new_server.paths[0]);
            if (dir != NULL) {
                printf("Directory opened: %s\n", new_server.paths[0]);
                struct dirent *entry;
                while ((entry = readdir(dir)) != NULL) {
                    if (strcmp(entry->d_name, ".") == 0 || 
                        strcmp(entry->d_name, "..") == 0 || 
                        strcmp(entry->d_name, "backup") == 0) {  // Skip backup directory
                        continue;
                    }
                    char full_path[2048];
                    snprintf(full_path, sizeof(full_path), "%s/%s", new_server.paths[0], entry->d_name);
                    strncpy(new_server.paths[new_server.path_count++], full_path, MAX_PATH_LENGTH);
                }
                closedir(dir);
            }
        }
    }

    servers[server_count++] = new_server;
    for (int i = 0; i < new_server.path_count; i++) {
        insert_into_hashmap(new_server.paths[i], &new_server);  // Store in hashmap
    }
    printf(YELLOW "Registered new storage server: IP = %s, Port = %d, Paths = " RESET,
           new_server.ip, new_server.port);

    for (int i = 0; i < new_server.path_count; i++) {
        printf("%s\n", new_server.paths[i]);
        traverse_directory(new_server.paths[i], &new_server);
        insert_into_hashmap(new_server.paths[i], &new_server);  // Store in hashmap
    }
    printf("\n");

    if (server_count == 3) {
        // Create backup folders in the 1st and 2nd servers for the data of the 3rd server
        char backup_folder_1[MAX_PATH_LENGTH];
        snprintf(backup_folder_1, sizeof(backup_folder_1), "%s/backup", servers[0].paths[0]);
        mkdir(backup_folder_1, 0755);

        char backup_folder_2[MAX_PATH_LENGTH];
        snprintf(backup_folder_2, sizeof(backup_folder_2), "%s/backup", servers[1].paths[0]);
        mkdir(backup_folder_2, 0755);

        // Create backup folder in the 3rd server
        char backup_folder_3[MAX_PATH_LENGTH];
        snprintf(backup_folder_3, sizeof(backup_folder_3), "%s/backup", new_server.paths[0]);
        mkdir(backup_folder_3, 0755);

        // Backup content of the 1st and 2nd servers in the 3rd server
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < servers[i].path_count; j++) {
                // Skip backup folders during replication
                if (strstr(servers[i].paths[j], "/backup") != NULL) {
                    continue;
                }
        
                char backup_path[MAX_PATH_LENGTH];
                snprintf(backup_path, sizeof(backup_path), "%s/backup/%s", new_server.paths[0], servers[i].paths[j]);
                printf("Replicating %s to %s\n", servers[i].paths[j], backup_path);
        
                struct stat st;
                if (stat(servers[i].paths[j], &st) == 0) {
                    printf(CYAN"no erros so far!\n"RESET);
                    if (S_ISDIR(st.st_mode)) {
                        printf(CYAN"Directory detected: %s\n"RESET, servers[i].paths[j]);
                        if (copy_dir(servers[i].paths[j], backup_path) == 0) {
                            printf("Directory %s copied to %s\n", servers[i].paths[j], backup_path);
                        } else {
                            printf("Failed to copy directory %s to %s\n", servers[i].paths[j], backup_path);
                        }
                    } else {
                        printf(CYAN"File detected: %s\n"RESET, servers[i].paths[j]);
                        if (copy_file(servers[i].paths[j], backup_path) == 0) {
                            printf("File %s copied to %s\n", servers[i].paths[j], backup_path);
                        } else {
                            printf("Failed to copy file %s to %s\n", servers[i].paths[j], backup_path);
                        }
                    }
                } else {
                    perror("Error getting file or directory status");
                }
        
                // Add the backup path to the new_server.paths array
                strncpy(new_server.paths[new_server.path_count++], backup_path, MAX_PATH_LENGTH);
                printf(RED"d: %d backup_path: %s\n"RESET, j, backup_path);
                strncpy(servers[2].paths[servers[2].path_count++], backup_path, MAX_PATH_LENGTH);
            }
        }
        
        // Add the new_server.paths to the list of accessible paths
        for (int i = 0; i < new_server.path_count; i++) {
            strncpy(accessible_paths[accessible_path_count++], new_server.paths[i], MAX_PATH_LENGTH);
            insert_into_hashmap(new_server.paths[i], &new_server);      ///////
            printf("Accessible path added: %s\n", new_server.paths[i]);
        }
        
        // Backup content of the 3rd server in the 1st and 2nd servers
        for (int i = 0; i < new_server.path_count; i++) {
            // Skip backup folders during replication
            if (strstr(new_server.paths[i], "/backup") != NULL) {
                continue;
            }

            for (int j = 0; j < 2; j++) {
                char backup_path[MAX_PATH_LENGTH];
                snprintf(backup_path, sizeof(backup_path), "%s/backup/%s", servers[j].paths[0], new_server.paths[i]);
                printf("Replicating %s to %s\n", new_server.paths[i], backup_path);

                struct stat st;
                if (stat(new_server.paths[i], &st) == 0) {
                    if (S_ISDIR(st.st_mode)) {
                        copy_dir(new_server.paths[i], backup_path);
                    } else {
                        copy_file(new_server.paths[i], backup_path);
                    }
                }

                strncpy(servers[j].paths[servers[j].path_count++], backup_path, MAX_PATH_LENGTH);
                printf(YELLOW"d: %d backup_path: %s\n"RESET,j,  backup_path);
            }
        }

        // Backup content of the 1st server in the 2nd server and vice versa
        for (int i = 0; i < servers[0].path_count; i++) {
            // Skip backup folders during replication
            if (strstr(servers[0].paths[i], "/backup") != NULL) {
                continue;
            }

            char backup_path[MAX_PATH_LENGTH];
            snprintf(backup_path, sizeof(backup_path), "%s/backup/%s", servers[1].paths[0], servers[0].paths[i]);
            printf("Replicating %s to %s\n", servers[0].paths[i], backup_path);

            struct stat st;
            if (stat(servers[0].paths[i], &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    copy_dir(servers[0].paths[i], backup_path);
                } else {
                    copy_file(servers[0].paths[i], backup_path);
                }
            }

            strncpy(servers[1].paths[servers[1].path_count++], backup_path, MAX_PATH_LENGTH);
        }

        for (int i = 0; i < servers[1].path_count; i++) {
            // Skip backup folders during replication
            if (strstr(servers[1].paths[i], "/backup") != NULL) {
                continue;
            }

            char backup_path[MAX_PATH_LENGTH];
            snprintf(backup_path, sizeof(backup_path), "%s/backup/%s", servers[0].paths[0], servers[1].paths[i]);
            printf("Replicating %s to %s\n", servers[1].paths[i], backup_path);

            struct stat st;
            if (stat(servers[1].paths[i], &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    copy_dir(servers[1].paths[i], backup_path);
                } else {
                    copy_file(servers[1].paths[i], backup_path);
                }
            }

            strncpy(servers[0].paths[servers[0].path_count++], backup_path, MAX_PATH_LENGTH);
        }
    }   

    if (server_count > 3) {
        for (int i = 0; i < new_server.path_count; i++) {

            // Skip if the path contains "backup"
            if (strstr(new_server.paths[i], "/backup") != NULL) {
                continue;
            }

            int replicated = 0;
            for (int j = 1; j < server_count && replicated < 2; j++) {
                int target_server_index = (server_count - 1 - j + server_count) % server_count;
                if (target_server_index != server_count - 1 && servers[target_server_index].active) {
                    char backup_path[MAX_PATH_LENGTH];
                    // Remove leading "./" from the path
                    const char *relative_path = new_server.paths[i];
                    // if (strncmp(relative_path, "./", 2) == 0) {
                    //     relative_path += 2;
                    // }
                    // while(relative_path[0] == '.' || relative_path[0] == '/') {
                    //     relative_path++;
                    // }
                    snprintf(backup_path, sizeof(backup_path), "%s/backup/%s", servers[target_server_index].paths[0], relative_path);
                    printf("Replicating %s to %s\n", new_server.paths[i], backup_path);

                    // Ensure the backup directory exists
                    char backup_dir[MAX_PATH_LENGTH];
                    snprintf(backup_dir, sizeof(backup_dir), "%s/backup", servers[target_server_index].paths[0]);
                    mkdir(backup_dir, 0755);

                    // Ensure the nested backup directory exists
                    char nested_backup_dir[MAX_PATH_LENGTH];
                    snprintf(nested_backup_dir, sizeof(nested_backup_dir), "%s/backup/%s", servers[target_server_index].paths[0], relative_path);
                    mkdir(nested_backup_dir, 0755);

                    // Perform the copy operation directly
                    struct stat st;
                    if (stat(new_server.paths[i], &st) == 0) {
                        if (S_ISDIR(st.st_mode)) {
                            copy_dir(new_server.paths[i], backup_path);
                        } else {
                            copy_file(new_server.paths[i], backup_path);
                        }
                    }

                    // Add the backup path to the target server's paths
                    strncpy(servers[target_server_index].paths[servers[target_server_index].path_count++], backup_path, MAX_PATH_LENGTH);

                    // Add the backup path to the accessible paths
                    strncpy(accessible_paths[accessible_path_count++], backup_path, MAX_PATH_LENGTH);

                    replicated++;
                }
            }
        }
    }    

    pthread_mutex_unlock(&server_mutex);
}

void update_lru_cache(const char *path, StorageServer *server) {
    // Store the new path and server pointer in the LRU cache
    strncpy(lru_cache[lru_head].path, path, MAX_PATH_LENGTH);
    
    // Allocate memory for the server pointer and copy the server data
    lru_cache[lru_head].server = malloc(sizeof(StorageServer));
    if (lru_cache[lru_head].server != NULL) {
        memcpy(lru_cache[lru_head].server, server, sizeof(StorageServer));  // Copy server data
    }

    lru_head = (lru_head + 1) % LRU_SIZE;  // Move head to the next position
}

StorageServer* is_path_in_lru_cache(const char *path) {
    for (int i = 0; i < LRU_SIZE; i++) {
        // Calculate the index in the circular array
        int index = (lru_head - 1 - i + LRU_SIZE) % LRU_SIZE;  // Adjust for circular indexing
        if (strncmp(lru_cache[index].path, path, MAX_PATH_LENGTH) == 0) {
            return lru_cache[index].server;  // Return the associated server pointer
        }
    }
    return NULL;  // Path not found
}

int is_valid_accessible_path(const char *path) {
    // printf("entered is_valid_accessible_path\n");  // Debug statement
    pthread_mutex_lock(&server_mutex);
    
    // Start with the full path
    char temp_path[MAX_PATH_LENGTH];
    strncpy(temp_path, path, MAX_PATH_LENGTH);
    
    // Check the full path and its parent directories
    while (strlen(temp_path) > 0) {
        unsigned int index = hash(temp_path);
        HashMapEntry *entry = hashmap[index];
        // printf("Checking path: %s\n", temp_path);
        
        // First check in the LRU cache
        StorageServer* server = is_path_in_lru_cache(temp_path);
        if (server) {
            // If found in LRU cache, update the cache and return
            update_lru_cache(temp_path, server);
            pthread_mutex_unlock(&server_mutex);
            return 1;  // Path is valid and accessible
        }

        // Iterate through the linked list at this index
        while (entry != NULL) {
            // Check if the path starts with the stored path
            if (strncmp(entry->path, temp_path, strlen(entry->path)) == 0) {
                // Update LRU cache with the accessed path
                update_lru_cache(temp_path, entry->server);  // Update LRU cache with the accessed path
                // printf("found\n");
                pthread_mutex_unlock(&server_mutex);
                return 1;  // Path is valid and accessible
            }
            entry = entry->next;  // Move to the next entry in the linked list
        }

        // Remove the last segment of the path
        char *last_slash = strrchr(temp_path, '/');
        if (last_slash != NULL) {
            *last_slash = '\0';  // Shorten the path
        } else {
            break;  // No more parent directories
        }
    }

    pthread_mutex_unlock(&server_mutex);
    return 0;  // Path is not valid
}

void handle_list_all_paths_request(int client_socket) {
    // printf("entered handle_list_all_paths_request\n");  // Debug statement
    pthread_mutex_lock(&server_mutex);
    char response[BUFFER_SIZE];
    response[0] = '\0';  // Initialize response buffer

    // Iterate through the hashmap to gather all paths
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashMapEntry *entry = hashmap[i];
        while (entry != NULL) {
            // Append the path to the response
            strncat(response, entry->path, sizeof(response) - strlen(response) - 1);
            strncat(response, "\n", sizeof(response) - strlen(response) - 1);
            entry = entry->next;  // Move to the next entry in the linked list
        }
    }

    // Send the response back to the client
    send(client_socket, response, strlen(response), 0);
    pthread_mutex_unlock(&server_mutex);
}

void handle_client_request(int client_socket, char *request) {
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "ACK");
    send(client_socket, buffer, strlen(buffer), 0);

    sleep(0.5);

    char command[20], path[256];
    char response[BUFFER_SIZE];

    sscanf(request, "%s %s", command, path);
    // printf("path: %s\n", path);

    if (strcmp(command, "FIND") == 0) {
        if (is_valid_accessible_path(path)) {
            pthread_mutex_lock(&server_mutex);

            // check();

            printf("path: %s\n", path);
            HashMapEntry *entry = hashmap[hash(path)];
            while(entry != NULL) {
                // printf("entry->path: %s\n", entry->path);
                if(strcmp(entry->path, path) == 0) {
                    // printf("entry->path: %s\n", entry->path);
                    break;
                }
                entry = entry->next;
            }
            // printf("Acitivity of servers (hash): %d\n", entry->server->active);
            // printf("Port: %d\n", entry->server->port);

            entry = hashmap[hash(path)];

            if(entry->server->port != temp_port) {
                printf("inside me\n");
                StorageServer* server = is_path_in_lru_cache(path);
                // printf("Acitivity of servers: %d\n", server->active);
                if (server && server->active) {
                    update_lru_cache(path, server);
                    // If found in LRU cache, send a success response
                    snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d", server->ip, server->port);
                    printf("Sending response: %s\n", response);
                    send(client_socket, response, strlen(response), 0);
                    close(client_socket);
                    pthread_mutex_unlock(&server_mutex);
                    return;
                }

                printf("above hash\n");
                HashMapEntry *entry = hashmap[hash(path)];
                // Proceed to search in the hashmap
                while (entry != NULL && entry->server->active) {
                    printf("inside hashh\n");
                    // Check if the path matches the entry's path
                    if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
                        // Get the associated server
                        StorageServer *server = entry->server;
                        // Update LRU cache with the accessed path
                        update_lru_cache(path, server);
                        snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d", server->ip, server->port);
                        // Send the response back to the client
                        printf("Sending response: %s\n", response);
                        send(client_socket, response, strlen(response), 0);
                        close(client_socket);
                        pthread_mutex_unlock(&server_mutex);
                        return;
                    }
                    entry = entry->next;  // Move to the next entry in the linked list
                }
            }

            pthread_mutex_unlock(&server_mutex);

            // Check for backup path
            char backup_path[BUFFER_SIZE];

            // Remove leading "./" from the path
            const char *relative_path = path;
            // if (strncmp(relative_path, "./", 2) == 0) {
            //     relative_path += 2;
            // }
            // Construct the backup path
            snprintf(backup_path, sizeof(backup_path), "backup/%s", relative_path);
            printf("backup_path: %s\n", backup_path);

            // Check for the backup path in the prior two storage servers
            pthread_mutex_lock(&server_mutex);
            found_count = 0;
            for (int i = 0; i < server_count && found_count < 2; i++) {
                if (servers[i].active) {
                    for (int j = 0; j < servers[i].path_count; j++) {
                        // printf("servers[i].paths[j]: %s\n", servers[i].paths[j]);
                        if (strstr(servers[i].paths[j], backup_path) != NULL) {
                            printf("backup path found\n");
                            
                            snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d (from backup)", servers[i].ip, servers[i].port);
                            printf("response: %s\n", response);
                            send(client_socket, response, strlen(response), 0);

                            // Store the found backup path and port number
                            strncpy(found_backup_paths[found_count].path, servers[i].paths[j], MAX_PATH_LENGTH - 1);
                            found_backup_paths[found_count].path[MAX_PATH_LENGTH - 1] = '\0'; // Ensure null-termination
                            found_backup_paths[found_count].port = servers[i].port;
                            found_count++;

                            if (found_count == 2) {
                                break;
                            }
                        }
                    }
                }
            }
            pthread_mutex_unlock(&server_mutex);

            if (found_count > 0) {
                printf("Found backup paths:\n");
                for (int i = 0; i < found_count; i++) {
                    printf("Path: %s, Port: %d\n", found_backup_paths[i].path, found_backup_paths[i].port);
                }
            } else {
                printf("No backup paths found\n");
                send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
            }

        } else {
            send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
        }
    } else if (strcmp(command, "LIST_ALL_PATHS") == 0) {
        handle_list_all_paths_request(client_socket);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
    }

    close(client_socket);
}

void redirect_request_to_storage_server(int client_socket, const char *request, const char *server_ip, int server_port) {
    int storage_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (storage_socket < 0) {
        perror("Socket creation failed");
        return;
    }

    struct sockaddr_in storage_addr;
    storage_addr.sin_family = AF_INET;
    storage_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip, &storage_addr.sin_addr);

    if (connect(storage_socket, (struct sockaddr *)&storage_addr, sizeof(storage_addr)) < 0) {
        perror("Connection to Storage Server failed");
        close(storage_socket);
        return;
    }

    send(storage_socket, request, strlen(request), 0);

    char response[BUFFER_SIZE];
    int bytes_received = recv(storage_socket, response, sizeof(response) - 1, 0);
    if (bytes_received < 0) {
        perror("Error receiving data from storage server");
    } else {
        response[bytes_received] = '\0';
        send(client_socket, response, bytes_received, 0);
    }

    close(storage_socket);
}

void handle_create_request(int client_socket, char *request) {
    char command[10], path[128], name[128];
    sscanf(request, "%s %s %s", command, path, name);

    if (strcmp(command, "CREATE") == 0) {
        printf(RED"CREATE\n"RESET);
        pthread_mutex_lock(&server_mutex);

        StorageServer* server = is_path_in_lru_cache(path);
        if (server) {
            update_lru_cache(path, server);
            char create_request[BUFFER_SIZE];
            snprintf(create_request, sizeof(create_request), "CREATE %s/%s", path, name);
            redirect_request_to_storage_server(client_socket, create_request, server->ip, server->port);

            // Add the new path to the server's paths
            snprintf(servers[server - servers].paths[servers[server - servers].path_count++], MAX_PATH_LENGTH, "%s/%s", path, name);
            pthread_mutex_unlock(&server_mutex);
            // return;
        } else {
            HashMapEntry *entry = hashmap[hash(path)];
            int found = 0;

            while (entry != NULL) {
                // Check if the path matches the entry's path
                if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
                    found = 1;
                    StorageServer *server = entry->server;  // Get the associated server
                    // Update LRU cache with the accessed path
                    update_lru_cache(path, server);
                    char create_request[BUFFER_SIZE];
                    snprintf(create_request, sizeof(create_request), "CREATE %s/%s", path, name);
                    redirect_request_to_storage_server(client_socket, create_request, server->ip, server->port);

                    // Add the new path to the server's paths
                    snprintf(servers[server - servers].paths[servers[server - servers].path_count++], MAX_PATH_LENGTH, "%s/%s", path, name);
                    pthread_mutex_unlock(&server_mutex);
                    break;
                }
                entry = entry->next;  // Move to the next entry in the linked list
            }
        }

        for (int i = 0; i < server_count; i++) {
            for (int j = 0; j < servers[i].path_count; j++) {
                if (strncmp(servers[i].paths[j], path, strlen(servers[i].paths[j])) == 0) {
                    // char create_request[BUFFER_SIZE];
                    // snprintf(create_request, sizeof(create_request), "CREATE %s/%s", path, name);
                    // redirect_request_to_storage_server(client_socket, create_request, servers[i].ip, servers[i].port);

                    // printf(YELLOW"path: %s\n"RESET, path);
                    // printf(YELLOW"name: %s\n"RESET, name);

                    // snprintf(servers[i].paths[servers[i].path_count++], MAX_PATH_LENGTH, "%s/%s", path, name);

                    // Perform the same operation on the backup paths
                    char backup_path[BUFFER_SIZE];
                    const char *relative_path = path;
                    if (strncmp(relative_path, "./", 2) == 0) {
                        relative_path += 2;
                    }
                    snprintf(backup_path, sizeof(backup_path), "backup/%s", relative_path);

                    for (int k = 0; k < server_count && found_count < 2; k++) {
                        if (servers[k].active) {
                            for (int l = 0; l < servers[k].path_count; l++) {
                                if (strstr(servers[k].paths[l], backup_path) != NULL) {
                                    char backup_create_request[BUFFER_SIZE];
                                    snprintf(backup_create_request, sizeof(backup_create_request), "CREATE %s/%s", servers[k].paths[l], name);
                                    redirect_request_to_storage_server(client_socket, backup_create_request, servers[k].ip, servers[k].port);

                                    snprintf(servers[k].paths[servers[k].path_count++], MAX_PATH_LENGTH, "%s/%s", servers[k].paths[l], name);
                                    found_count++;
                                    if (found_count == 2) {
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    pthread_mutex_unlock(&server_mutex);
                    return;
                }
            }
        }
        pthread_mutex_unlock(&server_mutex);
        send(client_socket, "ERROR: Path not found for creation", 35, 0);
    } else {
        send(client_socket, "ERROR: Unknown command", 22, 0);
    }

    close(client_socket);
}

void handle_copy_request(int client_socket, char *request) {

    // printf(RED"request: %s\n"RESET, request);
    char command[10], source[128], dest[128];
    sscanf(request, "%s %s %s", command, source, dest);
    // printf("source: %s dest: %s\n", source, dest);

    if (strcmp(command, "COPY") == 0) {
        pthread_mutex_lock(&server_mutex);
        HashMapEntry *source_entry = hashmap[hash(source)];
        HashMapEntry *dest_entry = hashmap[hash(dest)];
        int source_found = 0, dest_found = 0;
        int source_server_index = -1, dest_server_index = -1;

        int please = 0;
        int change = 0;

        while (source_entry != NULL) {
            // Check if the path matches the entry's path
            if (strncmp(source_entry->path, source, strlen(source_entry->path)) == 0) {
                source_found = 1;
                source_server_index = source_entry->server - servers;  // Get the index of the associated server
                break;  // Exit the loop if found
            }
            source_entry = source_entry->next;  // Move to the next entry in the linked list
        }

        // Check if the destination path exists in the hashmap
        while (dest_entry != NULL) {
            // Check if the path matches the entry's path
            if (strncmp(dest_entry->path, dest, strlen(dest_entry->path)) == 0) {
                dest_found = 1;  // Mark destination as found
                dest_server_index = source_entry->server - servers;
                break;  // Exit the loop if found
            }
            dest_entry = dest_entry->next;  // Move to the next entry in the linked list
        }

        // for (int i = 0; i < server_count; i++) {
        //     for (int j = 0; j < servers[i].path_count; j++) {
                
        //         if (strncmp(servers[i].paths[j], source, strlen(servers[i].paths[j])) == 0) {
        //             // printf("SOURCE : servers[i].paths[j]: %s\n", servers[i].paths[j]);
        //             source_found = 1;
        //             source_server_index = i;
        //         }
        //         if (strncmp(servers[i].paths[j], dest, strlen(servers[i].paths[j])) == 0) {
        //             please= i;
        //             change = j;
        //             // printf("DEST : servers[i].paths[j]: %s\n", servers[i].paths[j]);
        //             dest_found = 1;
        //             dest_server_index = i;
        //         }
        //     }
        // }

        if (source_found && dest_found) {
            char copy_request[BUFFER_SIZE];
            snprintf(copy_request, sizeof(copy_request), "COPY %s %s", source, dest);
            int source_socket = socket(AF_INET, SOCK_STREAM, 0);
            if (source_socket < 0) {
                perror("Socket creation failed");
                pthread_mutex_unlock(&server_mutex);
                return;
            }

            struct sockaddr_in source_addr;
            source_addr.sin_family = AF_INET;
            source_addr.sin_port = htons(servers[source_server_index].port);
            inet_pton(AF_INET, servers[source_server_index].ip, &source_addr.sin_addr);

            if (connect(source_socket, (struct sockaddr *)&source_addr, sizeof(source_addr)) < 0) {
                perror("Connection to source Storage Server failed");
                close(source_socket);
                pthread_mutex_unlock(&server_mutex);
                return;
            }

            send(source_socket, copy_request, strlen(copy_request), 0);

            char data[BUFFER_SIZE];
            int bytes_received = recv(source_socket, data, sizeof(data) - 1, 0);
    
            if (bytes_received < 0) {
                perror("Error receiving data from destination storage server");
            } else {
                data[bytes_received] = '\0';
                send(client_socket, data, bytes_received, 0);
            }
            printf("value from dest server: %s\n", data);
            strcpy(servers[please].paths[change+1], data);

            close(source_socket);

            // Perform the same operation on the backup paths
            char backup_dest[BUFFER_SIZE];
            char please[BUFFER_SIZE];
            const char *relative_dest = dest;
            const char *what = dest;
            
            if (strncmp(what, "./", 2) == 0) {
                what += 2;
            }
            snprintf(please, sizeof(please), "backup/%s", what);
            // printf("please: %s\n", please);

            // Remove trailing slash and any characters following it
            char *slash_pos = strrchr(relative_dest, '/');
            if (slash_pos != NULL) {
                *slash_pos = '\0';
            }

            snprintf(backup_dest, sizeof(backup_dest), "backup/%s", relative_dest);

             // Find elements in servers.paths array whose suffix matches backup_dest
            int matching_indices[2] = {-1, -1};
            char *matching_paths[2] = {NULL, NULL};
            int match_count = 0;
            
            for (int k = 0; k < server_count; k++) {
                for (int l = 0; l < servers[k].path_count; l++) {
                    if (strlen(servers[k].paths[l]) >= strlen(backup_dest)) {
                        const char *suffix = servers[k].paths[l] + strlen(servers[k].paths[l]) - strlen(backup_dest);
                        printf(CYAN"suffix: %s\n"RESET, suffix);
                        printf("backup_dest: %s\n", backup_dest);
                        if (strcmp(suffix, backup_dest) == 0) {
                            printf(RED"Match found\n"RESET);
                            matching_indices[match_count] = l;
                            matching_paths[match_count] = servers[k].paths[l];
                            match_count++;
                            if (match_count == 2) break;
                        }
                    }
                }
                if (match_count == 2) break;
            }
            
            if (match_count == 2) {
                printf("Matching paths found: %s, %s\n", matching_paths[0], matching_paths[1]);
            
                // Extract the last part of the data string
                char *last_slash = strrchr(data, '/');
                if (last_slash != NULL) {
                    char to_append[BUFFER_SIZE];
                    strncpy(to_append, last_slash, sizeof(to_append) - 1);
                    to_append[sizeof(to_append) - 1] = '\0'; // Ensure null-termination
            
                    // Append the extracted part to the matching paths
                    for (int i = 0; i < match_count; i++) {
                        strncat(matching_paths[i], to_append, sizeof(servers[0].paths[0]) - strlen(matching_paths[i]) - 1);
                        printf("Updated path: %s\n", matching_paths[i]);
                    }
                }
            } else {
                printf("Error: Could not find two matching paths\n");
            }

            // Perform operations on the matching paths
            for (int i = 0; i < match_count; i++) {
                int k = matching_indices[i] / MAX_PATHS; // Assuming MAX_PATHS is the maximum number of paths per server
                int l = matching_indices[i] % MAX_PATHS;

                printf(RED"DATA: %s\n"RESET, data);

                // Extract the last part of the data string
                char *last_slash = strrchr(data, '/');
                if (last_slash != NULL) {
                    char to_append[BUFFER_SIZE];
                    strncpy(to_append, last_slash, sizeof(to_append) - 1);
                    to_append[sizeof(to_append) - 1] = '\0'; // Ensure null-termination
                
                    // Check if the part to append is already present at the end of servers[k].paths[l]
                    size_t current_length = strlen(servers[k].paths[l]);
                    size_t to_append_length = strlen(to_append);
                    if (current_length < to_append_length || strcmp(servers[k].paths[l] + current_length - to_append_length, to_append) != 0) {
                        // Append the extracted part to servers[k].paths[l]
                        strncat(servers[k].paths[l], to_append, sizeof(servers[k].paths[l]) - current_length - 1);
                    }
                }

                char backup_copy_request[BUFFER_SIZE * 2]; // Increased buffer size
                // snprintf(backup_copy_request, sizeof(backup_copy_request), "COPY %s %s", source, servers[k].paths[l]);
                snprintf(backup_copy_request, sizeof(backup_copy_request), "COPY %s %s", source, matching_paths[i]);

                printf(YELLOW"backup_copy_request: %s\n"RESET, backup_copy_request);

                int backup_socket = socket(AF_INET, SOCK_STREAM, 0);
                if (backup_socket < 0) {
                    perror("Socket creation failed");
                    continue;
                }

                struct sockaddr_in backup_addr;
                backup_addr.sin_family = AF_INET;
                backup_addr.sin_port = htons(servers[k].port);
                inet_pton(AF_INET, servers[k].ip, &backup_addr.sin_addr);

                if (connect(backup_socket, (struct sockaddr *)&backup_addr, sizeof(backup_addr)) < 0) {
                    perror("Connection to backup Storage Server failed");
                    close(backup_socket);
                    continue;
                }

                send(backup_socket, backup_copy_request, strlen(backup_copy_request), 0);
                close(backup_socket);
            }
        } else {
            if (!source_found) {
                send(client_socket, "ERROR: Source path not found", 28, 0);
            }
            if (!dest_found) {
                send(client_socket, "ERROR: Destination path not found", 33, 0);
            }
        }

        pthread_mutex_unlock(&server_mutex);
    } else {
        send(client_socket, "ERROR: Unknown command", 22, 0);
    }

    close(client_socket);
}

void handle_delete_request(int client_socket, char *request) {
    printf("delete request\n");
    char command[10], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "DELETE") == 0) {
        pthread_mutex_lock(&server_mutex);

        StorageServer* server = is_path_in_lru_cache(path);
        if (server) {
            update_lru_cache(path, server);
            char delete_request[BUFFER_SIZE];
            snprintf(delete_request, sizeof(delete_request), "DELETE %s", path);
            redirect_request_to_storage_server(client_socket, delete_request, server->ip, server->port);
            
            pthread_mutex_unlock(&server_mutex);
            return;
                
        }

        // If not found in LRU cache, search in the hashmap
        HashMapEntry *entry = hashmap[hash(path)];
        while (entry != NULL) {
            // Check if the path matches the entry's path
            if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
                // Get the associated server
                StorageServer *server = entry->server;
                // Update LRU cache with the accessed path
                update_lru_cache(path, server);
                char delete_request[BUFFER_SIZE];
                snprintf(delete_request, sizeof(delete_request), "DELETE %s", path);
                redirect_request_to_storage_server(client_socket, delete_request, server->ip, server->port);
                
                pthread_mutex_unlock(&server_mutex);
                return;
            }
            entry = entry->next;  // Move to the next entry in the linked list
        }

        for (int i = 0; i < server_count; i++) {
            for (int j = 0; j < servers[i].path_count; j++) {
                if (strncmp(servers[i].paths[j], path, strlen(servers[i].paths[j])) == 0) {
                    // char delete_request[BUFFER_SIZE];
                    // snprintf(delete_request, sizeof(delete_request), "DELETE %s", path);
                    // redirect_request_to_storage_server(client_socket, delete_request, servers[i].ip, servers[i].port);

                    // Perform the same operation on the backup paths
                    char backup_path[BUFFER_SIZE];
                    const char *relative_path = path;
                    if (strncmp(relative_path, "./", 2) == 0) {
                        relative_path += 2;
                    }
                    snprintf(backup_path, sizeof(backup_path), "backup/%s", relative_path);

                    printf("backup path: %s\n", backup_path);
                    for (int k = 0; k < server_count; k++) {
                        printf("i am in\n");
                        if (servers[k].active) {
                            for (int l = 0; l < servers[k].path_count; l++) {
                                if (strstr(servers[k].paths[l], backup_path) != NULL) {
                                    char backup_delete_request[BUFFER_SIZE];
                                    snprintf(backup_delete_request, sizeof(backup_delete_request), "DELETE %s", servers[k].paths[l]);
                                    printf("deleting...\n");
                                    redirect_request_to_storage_server(client_socket, backup_delete_request, servers[k].ip, servers[k].port);
                                }
                            }
                        }
                    }

                    pthread_mutex_unlock(&server_mutex);
                    return;
                }
            }
        }
        pthread_mutex_unlock(&server_mutex);
        send(client_socket, "ERROR: Path not found for deletion", 35, 0);
    } else {
        send(client_socket, "ERROR: Unknown command", 22, 0);
    }

    close(client_socket);
}

void handle_get_details_request(int client_socket, char *request) {
    char command[20], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "GET_DETAILS") == 0) {
        pthread_mutex_lock(&server_mutex);
        StorageServer* server = is_path_in_lru_cache(path);
        if (server) {
            update_lru_cache(path, server);
            char get_details_request[BUFFER_SIZE];
            snprintf(get_details_request, sizeof(get_details_request), "GET_DETAILS %s", path);
            redirect_request_to_storage_server(client_socket, get_details_request, server->ip, server->port);
            
            pthread_mutex_unlock(&server_mutex);
            return;
                
        } else {
            // If not found in LRU cache, search in the hashmap
            HashMapEntry *entry = hashmap[hash(path)];
            while (entry != NULL) {
                // Check if the path matches the entry's path
                if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
                    // Get the associated server
                    StorageServer *server = entry->server;
                    // Update LRU cache with the accessed path
                    update_lru_cache(path, server);
                    char get_details_request[BUFFER_SIZE];
                    snprintf(get_details_request, sizeof(get_details_request), "GET_DETAILS %s", path);
                    redirect_request_to_storage_server(client_socket, get_details_request, server->ip, server->port);
                    
                    pthread_mutex_unlock(&server_mutex);
                    return;
                }
                entry = entry->next;  // Move to the next entry in the linked list
            }
        }

        // If no matching path is found, handle that case here
        pthread_mutex_unlock(&server_mutex);
        send(client_socket, "ERROR: Path not found", 21, 0);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
    }

    close(client_socket);
}

void handle_stream_request(int client_socket, char *request) {
    char command[20], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "STREAM") == 0) {
        pthread_mutex_lock(&server_mutex);
        StorageServer* server = is_path_in_lru_cache(path);
        if (server) {
            update_lru_cache(path, server);
            char response[BUFFER_SIZE];
            snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d", server->ip, server->port);
            send(client_socket, response, strlen(response), 0);
            pthread_mutex_unlock(&server_mutex);
            close(client_socket);
            return;
        } else {
            // If not found in LRU cache, search in the hashmap
            HashMapEntry *entry = hashmap[hash(path)];
            while (entry != NULL) {
                // Check if the path matches the entry's path
                if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
                    // Get the associated server
                    StorageServer *server = entry->server;
                    // Update LRU cache with the accessed path
                    update_lru_cache(path, server);
                    char response[BUFFER_SIZE];
                    snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d", server->ip, server->port);
                    send(client_socket, response, strlen(response), 0);
                    pthread_mutex_unlock(&server_mutex);
                    close(client_socket);
                    return;
                }
                entry = entry->next;  // Move to the next entry in the linked list
            }
        }

        pthread_mutex_unlock(&server_mutex);
        send(client_socket, "ERROR: Path not found", 21, 0);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
    }

    close(client_socket);
}

void handle_register_path(char* buffer) {
    printf("Received in register: %s\n", buffer);
    char new_path[3000];
    strcpy(new_path, buffer + 14);
    printf("new path: %s\n", new_path);

    char path_dir[100];
    char *slash = strchr(new_path, '/');
    if (slash != NULL) {
        char *next_slash = strchr(slash + 1, '/');
        if (next_slash != NULL) {
            size_t length = next_slash - new_path;
            strncpy(path_dir, new_path, length);
            path_dir[length] = '\0';
        } else {
            strcpy(path_dir, new_path);
        }
    } else {
        strcpy(path_dir, new_path);
    }

    printf("path_dir: %s\n", path_dir);
    printf("new_path: %s\n", new_path);

    printf("server_path: %s\n",servers[0].paths[0]);
    for(int i=0; i<server_count; i++) {
        // printf("server path 0 : %s\n", servers[i].paths[0]);
        if(strcmp(servers[i].paths[0], path_dir) == 0) {
            printf("FOUBDDDD\n");
            int index = servers[i].path_count;
            strcpy(servers[i].paths[index],new_path);
            servers[i].path_count++;
        }   
    }
}

void add_in_log(char* command, char* path, char* ip, int port)
{
    char str_port[10];
    sprintf(str_port, "%d", port);
    char line_to_add[MAX_LINE_LENGTH];
    line_to_add[0] = '\0';
    strcat(line_to_add, command);
    strcat(line_to_add, "   ");
    strcat(line_to_add, path);
    strcat(line_to_add, "   ");
    if(ip)
    {
        strcat(line_to_add, "Path found");
        strcat(line_to_add, "   ");
        strcat(line_to_add, ip);
        strcat(line_to_add, "   ");
        strcat(line_to_add, str_port);
    }
    else
        strcat(line_to_add, "Path not found");
    strcat(line_to_add, "\n");

    FILE *file = fopen("log.txt", "a+");  // Change mode to "a+" to create the file if it doesn't exist
    if (!file) {
        perror("Failed to open log.txt");
        return;
    }

    char *lines[MAX_LINES];
    int line_count = 0;

    // Read lines from the file
    while (line_count < MAX_LINES && fgets(lines[line_count] = malloc(MAX_LINE_LENGTH), MAX_LINE_LENGTH, file)) {
        line_count++;
    }

    fclose(file);

    file = fopen("log.txt", "w+");  // Change mode to "w+" to create the file and clear it if it exists
    if (!file) {
        perror("Failed to open log.txt");
        return;
    }

    // Store line_to_add in the array
    if (line_count < MAX_LINES) {
        // Add line_to_add to the end of the array
        strcpy(lines[line_count], line_to_add);
        line_count++;  // Increment line count
    } else {
        // Overwrite arr[1] with arr[9] and add line_to_add at the end
        for (int i = 0; i < line_count-1; i++) {
            strcpy(lines[i], lines[i+1]);
        }
        strcpy(lines[MAX_LINES - 1], line_to_add);  // Add new line at the end
    }

    // Write all lines to the file
    for (int i = 0; i < line_count; i++) {
        fputs(lines[i], file);
    }

    // Free allocated memory
    for (int i = 0; i < line_count; i++) {
        free(lines[i]);
    }

    fclose(file);
}

void log_ns(char* command, char* path)
{
    // printf("log_ns : %s %s\n", command, path);
    pthread_mutex_lock(&server_mutex);

    StorageServer* server = is_path_in_lru_cache(path);
    if (server) {
        add_in_log(command, path, server->ip, server->port);
        pthread_mutex_unlock(&server_mutex);
        return;
    }

    // Proceed to search in the hashmap
    HashMapEntry *entry = hashmap[hash(path)];
    while (entry != NULL) {
        // Check if the path matches the entry's path
        if (strncmp(entry->path, path, strlen(entry->path)) == 0) {
            // Get the associated server
            StorageServer *server = entry->server;
            add_in_log(command, path, server->ip, server->port);
            pthread_mutex_unlock(&server_mutex);
            return;
        }
        entry = entry->next;  // Move to the next entry in the linked list
    }

    add_in_log(command, path, NULL, 0);

    pthread_mutex_unlock(&server_mutex);
}

void handle_status_update(char *request) {
    pthread_mutex_lock(&server_mutex);

    printf("in status update\n");
    // printf("request: %s\n", request);

    char ip[INET_ADDRSTRLEN];
    int port, active;
    sscanf(request, "STATUS IP: %s PORT: %d ACTIVE: %d", ip, &port, &active);

    temp_port = port;

    for (int i = 0; i < server_count; i++) {
        if ( servers[i].port == port ) {
            servers[i].active = active;
            printf("Updated status of server %s:%d to %d\n", ip, port, active);
            break;
        }
    }

    // printf(RED"hashmap\n"RESET);

    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashMapEntry *entry = hashmap[i];
        // if(entry != NULL) {
        //     printf(CYAN"port: %d\n"RESET, entry->server->port);
        // }
        while (entry != NULL) {
            if( entry->server->port == port) {
                entry->server->active = active;
                // printf("Updated status of server %s:%d to %d\n", ip, port, active);
            }
            entry = entry->next;
        }
    }

    // check();

    pthread_mutex_unlock(&server_mutex);
}

void *client_handler(void *arg) {
    int client_socket = *(int *)arg;
    free(arg);

    char buffer[BUFFER_SIZE];
    int bytes_read = recv(client_socket, buffer, BUFFER_SIZE, 0);
    if (bytes_read < 0) {
        perror(RED "Error reading request" RESET);

        close(client_socket);
        return NULL;
    }

    buffer[bytes_read] = '\0';
    printf(GREEN"Received request: "RESET);
    printf("BUFFER: %s\n", buffer);

    // if(strncmp(buffer, "REGISTER_PATH", 13) == 0) {
    //     handle_register_path(buffer);
    // } else 

    if(strncmp(buffer, "REGISTER", 8) != 0)
    {
        char temp[BUFFER_SIZE], temp_path[BUFFER_SIZE], temp_com[BUFFER_SIZE];
        sscanf(buffer, "%s %s %s", temp, temp_path, temp_com);
        log_ns(temp_com, temp_path);
    }

    if (strncmp(buffer, "REGISTER", 8) == 0) {
        register_storage_server(buffer);
        send(client_socket, "Registered successfully", 24, 0);
    } else if (strncmp(buffer, "STATUS", 6) == 0) {
        printf("recieved status update: %s\n", buffer);
        handle_status_update(buffer);
        send(client_socket, "Status updated successfully", 27, 0);
    } else if (strncmp(buffer, "FIND", 4) == 0) {
        handle_client_request(client_socket, buffer);
    } else if (strncmp(buffer, "CREATE", 6) == 0) {
        handle_create_request(client_socket, buffer);
    } else if (strncmp(buffer, "COPY", 4) == 0) {
        handle_copy_request(client_socket, buffer);
    } else if (strncmp(buffer, "DELETE", 6) == 0) {
        handle_delete_request(client_socket, buffer);
    } else if (strncmp(buffer, "LIST_ALL_PATHS", 14) == 0) {
        handle_list_all_paths_request(client_socket);
    } else if (strncmp(buffer, "GET_DETAILS", 11) == 0) {
        handle_get_details_request(client_socket, buffer);
    } else if (strncmp(buffer, "STREAM", 6) == 0) {
        handle_stream_request(client_socket, buffer);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
        close(client_socket);
    }

    return NULL;
}

int main() {
    int server_socket;
    struct sockaddr_in server_addr;

    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        perror(RED "Socket creation failed" RESET);
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror(RED "setsockopt(SO_REUSEADDR) failed" RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    char *ip_address = get_ip_address();
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(ip_address);
    server_addr.sin_port = htons(PORT);

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror(RED "Bind failed" RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    if (listen(server_socket, 5) < 0) {
        perror(RED "Listen failed" RESET);
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    printf("Naming Server is running on IP %s and waiting for requests...\n", ip_address);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int *client_socket = malloc(sizeof(int));
        if (client_socket == NULL) {
            perror(RED "Malloc failed" RESET);
            continue;
        }

        *client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
        if (*client_socket < 0) {
            perror(RED "Accept failed" RESET);
            free(client_socket);
            continue;
        }

        pthread_t thread;
        if (pthread_create(&thread, NULL, client_handler, client_socket) != 0) {
            perror(RED "Thread creation failed" RESET);
            free(client_socket);
            continue;
        }

        pthread_detach(thread);
    }

    close(server_socket);
    return 0;
}