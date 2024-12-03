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
#include <netdb.h>  // Include netdb.h for getnameinfo, NI_MAXHOST, and NI_NUMERICHOST
#include "colors.h"

#define PORT 6060
#define BUFFER_SIZE 100000
#define MAX_PATHS 100
#define MAX_PATH_LENGTH 1024
#define MAX_SERVERS 50
#define HASHMAP_SIZE 100  // Define size for the hashmap
#define LRU_SIZE 10  // Define size for the LRU cache

// Error codes
#define ERR_FILE_NOT_FOUND "ERROR: File not found"
#define ERR_UNKNOWN_COMMAND "ERROR: Unknown command"
#define ERR_MAX_SERVERS_REACHED "ERROR: Maximum number of storage servers reached"
#define ERR_FILE_BUSY "ERROR: File is currently being written to by another client"

typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
    char paths[MAX_PATHS][200];
    int path_count;
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

// Update the LRU Cache structure to store server pointers
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

char* get_ip_address() {
    // printf("entered get_ip_address\n");  // Debug statement
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
                    if(find_in_hashmap(server->paths[server->path_count]) == 0)
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

// void check()
// {
//     printf("Check HashMap Entries:\n");
//     for (int i = 0; i < HASHMAP_SIZE; i++) {
//         HashMapEntry *entry = hashmap[i];
//         while (entry != NULL) {
//             printf("Path: %s, Server IP: %s, Server Port: %d\n", 
//                    entry->path, 
//                    entry->server->ip, 
//                    entry->server->port);
//             entry = entry->next;  // Move to the next entry in the linked list
//         }
//     }
//     printf("\n");
// }

void register_storage_server(char *request) {
    // printf(TEAL"request:%s\n"RESET, request);
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
        if(find_in_hashmap(new_server.paths[new_server.path_count]) == 0)
            insert_into_hashmap(new_server.paths[new_server.path_count], &new_server);  // Store in hashmap
        new_server.path_count++;  // Increment path count after insertion
        token = strtok(NULL, ",");
    }


    // Register the new server and traverse its paths from the hashmap
    servers[server_count++] = new_server;
    // Store the new server in the hashmap for each path
    for (int i = 0; i < new_server.path_count; i++) {
        if(find_in_hashmap(new_server.paths[i]) == 0)
            insert_into_hashmap(new_server.paths[i], &new_server);  // Store in hashmap
    }
    // check();
    printf(YELLOW "Registered new storage server: IP = %s, Port = %d, Paths = " RESET,
           new_server.ip, new_server.port);

    for (int i = 0; i < new_server.path_count; i++) {
        // printf("%s\n", new_server.paths[i]);
        traverse_directory(new_server.paths[i], &new_server);
        if(find_in_hashmap(new_server.paths[i]) == 0)
            insert_into_hashmap(new_server.paths[i], &new_server);  // Store in hashmap
    }
    for (int i = 0; i < HASHMAP_SIZE; i++) {
        HashMapEntry *entry = hashmap[i];
        while (entry != NULL) {
            printf("%s\n", entry->path);
            entry = entry->next;  // Move to the next entry in the hashmap
        }
    }
    printf("\n");

    // check();

    pthread_mutex_unlock(&server_mutex);
}

// Function to update the LRU cache with a new path and server pointer
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

// Function to check if a path is in the LRU cache and return the associated server pointer
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

// Modify the is_valid_accessible_path function to update LRU cache
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

// Modify the handle_client_request function to check LRU cache
void handle_client_request(int client_socket, char *request) {
    // printf("entered handle_client_request\n");  // Debug statement
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "ACK");
    send(client_socket, buffer, strlen(buffer), 0);

    sleep(0.5);

    char command[20], path[256];
    char response[BUFFER_SIZE];

    sscanf(request, "%s %s", command, path);
    // printf("-->%s %s\n", command, path);
    if (strcmp(command, "FIND") == 0) {
        pthread_mutex_lock(&server_mutex);

        StorageServer* server = is_path_in_lru_cache(path);
        if (server) {
            update_lru_cache(path, server);
            // If found in LRU cache, send a success response
            snprintf(response, sizeof(response), "SERVER IP: %s PORT: %d", server->ip, server->port);
            printf("Sending response: %s\n", response);
            send(client_socket, response, strlen(response), 0);
            close(client_socket);
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

        pthread_mutex_unlock(&server_mutex);
        send(client_socket, ERR_FILE_NOT_FOUND, strlen(ERR_FILE_NOT_FOUND), 0);
    } else if (strcmp(command, "LIST_ALL_PATHS") == 0) {
        handle_list_all_paths_request(client_socket);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
    }

    close(client_socket);
}

void redirect_request_to_storage_server(int client_socket, const char *request, const char *server_ip, int server_port) {
    // printf("entered redirect_request_to_storage_server\n");  // Debug statement
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
    // printf("entered handle_create_request\n");  // Debug statement
    char command[10], path[128], name[128];
    sscanf(request, "%s %s %s", command, path, name);

    if (strcmp(command, "CREATE") == 0) {
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
            return;
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
                    return;
                }
                entry = entry->next;  // Move to the next entry in the linked list
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
    // printf("entered handle_copy_request\n");  // Debug statement
    char command[10], source[128], dest[128];
    sscanf(request, "%s %s %s", command, source, dest);

    if (strcmp(command, "COPY") == 0) {
        pthread_mutex_lock(&server_mutex);
        HashMapEntry *source_entry = hashmap[hash(source)];
        HashMapEntry *dest_entry = hashmap[hash(dest)];
        int source_found = 0, dest_found = 0;
        int source_server_index = -1;  // Initialize to -1 to indicate not found

        // Check if the source path exists in the hashmap
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
                break;  // Exit the loop if found
            }
            dest_entry = dest_entry->next;  // Move to the next entry in the linked list
        }

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
            printf("Value from dest server: %s\n", data);

            close(source_socket);
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
    // printf("entered handle_delete_request\n");  // Debug statement
    char command[10], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "DELETE") == 0) {
        pthread_mutex_lock(&server_mutex);
        
        // First check in the LRU cache
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

        pthread_mutex_unlock(&server_mutex);
        send(client_socket, "ERROR: Path not found for deletion", 35, 0);
    } else {
        send(client_socket, "ERROR: Unknown command", 22, 0);
    }

    close(client_socket);
}

void handle_get_details_request(int client_socket, char *request) {
    // printf("entered handle_get_details_request\n");  // Debug statement
    char command[20], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "GET_DETAILS") == 0) {
        pthread_mutex_lock(&server_mutex);
        
        // First check in the LRU cache
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
        send(client_socket, "ERROR: Path not found for details", 35, 0);
    } else {
        send(client_socket, ERR_UNKNOWN_COMMAND, strlen(ERR_UNKNOWN_COMMAND), 0);
    }

    close(client_socket);
}

void handle_stream_request(int client_socket, char *request) {
    // printf("entered handle_stream_request\n");  // Debug statement
    char command[20], path[256];
    sscanf(request, "%s %s", command, path);

    if (strcmp(command, "STREAM") == 0) {
        pthread_mutex_lock(&server_mutex);
        
        // First check in the LRU cache
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

#define MAX_LINES 10
#define MAX_LINE_LENGTH 256

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
    // printf("|%s|\n", buffer);
    if(strncmp(buffer, "REGISTER", 8) != 0)
    {
        char temp[BUFFER_SIZE], temp_path[BUFFER_SIZE], temp_com[BUFFER_SIZE];
        sscanf(buffer, "%s %s %s", temp, temp_path, temp_com);
        log_ns(temp_com, temp_path);
    }

    if (strncmp(buffer, "REGISTER", 8) == 0) {
        register_storage_server(buffer);
        send(client_socket, "Registered successfully", 24, 0);
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
        // printf("hi i am in main\n");
        // check();
        // printf("\n");
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int *client_socket = malloc(sizeof(int));
        if (client_socket == NULL) {
            perror(RED "Malloc failed" RESET);
            continue;
        }

        // printf("hi main 1\n");
        // check();
        // printf("\n");
        *client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
        // printf("hi main 2\n");
        // check();
        // printf("\n");
        
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

        // printf("hi i out of main\n");
        // check();
        // printf("\n");
    }

    close(server_socket);
    return 0;
}