#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>

// Macros
#define MAX_MSG_SIZE 100
#define LOAD_BALANCER_MTYPE 104
#define FILENAME_SHM "./client.c"
#define FILENAME_MQ "load_balancer.c"
#define SHM_BUFFER_SIZE 1024
#define SHM_PERMISSIONS 0666
#define MAX_NODES 30

// Structure for the message
typedef struct Message
{
    long mtype;               // Message type (must be greater than 0)
    char mtext[MAX_MSG_SIZE]; // Message data
} Message;

// Main function
int main()
{
    // Variables
    key_t key_mq;                               // Identifier for message queue of load balancer
    int qd_lb;                                  // Message queue descriptor of load balancer
    Message message;                            // Variable to read message into
    int Sequence_Number;                        // Client sequence number
    int Operation_Number;                       // Opertaion number to perform
    char Graph_File_Name[10];                   // Name of graph file to operate on
    key_t key_shm;                              // Key (unique identifier) for SHM
    int shmid;                                  // Identifier of SHM
    char *shm;                                  // Character pointer to SHM segment
    int nodes;                                  // Number of nodes
    int adjMatrix[MAX_NODES][MAX_NODES];        // Adjacency matrix
    int startingVertex;                         // starting vertex for BFS/DFS traversals
    char newArr[2 * MAX_NODES * MAX_NODES + 5]; // Array to store contents to write to shared memory
    int i = 0, j = 0, k = 0;                    // Loop variable

    // Generate a unique key for the message queue
    if ((key_mq = ftok(FILENAME_MQ, 'A')) == -1)  //Generate a key unique to (FILENAME_MQ, 'A')
    {
        perror("Client: ftok");
        return 1;
    }

    // Connect to the load balancer queue
    if ((qd_lb = msgget(key_mq, 0644)) == -1)
    {
        perror("Client: msgget");
        return 1;
    }

    // Infinite loop
    while (1)
    {
        // Display the menu to the user
        printf("Menu:\n");
        printf("1. Add a new graph to the database\n");
        printf("2. Modify an existing graph of the database\n");
        printf("3. Perform DFS on an existing graph of the database\n");
        printf("4. Perform BFS on an existing graph of the database\n");

        // Get user inputs
        printf("Enter Sequence Number: ");
        scanf("%d", &Sequence_Number);

        printf("Enter Operation Number: ");
        scanf("%d", &Operation_Number);

        printf("Enter Graph File Name: ");
        scanf(" %s", Graph_File_Name);

        // Creating shared memory segment
        // Creating key
        if ((key_shm = ftok(FILENAME_SHM, Sequence_Number)) == -1)
        {
            perror("Client: ftok");
            exit(1);
        }

        // Getting SHM identifier
        if ((shmid = shmget(key_shm, sizeof(char[SHM_BUFFER_SIZE]), SHM_PERMISSIONS | IPC_CREAT)) == -1)
        {
            perror("Client: shmget");
            exit(1);
        }

        // Attaching to SHM
        if (*(shm = (char *)shmat(shmid, NULL, 0)) == -1)             //this code snippet is attempting to attach to a shared memory segment identified by shmid. 
        {                                                             //If successful, shm will point to the attached memory location
            perror("Client: ftok");
            exit(1);
        }

        // Write operations
        if (Operation_Number == 1 || Operation_Number == 2)
        {
            // Get number of nodes from user
            printf("Enter number of nodes of the graph: ");
            scanf("%d", &nodes);

            // Get adjacency matrix from user
            printf("Enter adjacency matrix, each row on a separate line and elements of a single row separated by whitespace characters: \n");
            for (int i = 0; i < nodes; i++)
            {
                for (int j = 0; j < nodes; j++)
                {
                    scanf("%d", &adjMatrix[i][j]);
                }
            }

            // Linerarising row by row 2-D array.
            // Format: Nodes,adj[0][0],adj[0][1],...,adj[1][0],adj[1][1],...,adj[nodes-1][nodes-1]
            // NOTE: commas are seperate characters to be counted
            sprintf(newArr, "%d,", nodes);
            k = strlen(newArr);
            for (i = 0; i < nodes; i++)
            {
                for (j = 0; j < nodes; j++)
                {
                    newArr[k] = (char)(adjMatrix[i][j] + 48);
                    k++;
                    newArr[k] = ',';
                    k++;
                }
            }
            newArr[k] = '\0';

            // Writing to shared memory
            strcpy(shm, newArr);

            // Setting mtype of message
            message.mtype = LOAD_BALANCER_MTYPE;  //#define LOAD_BALANCER_MTYPE 104

            // Send request to load balancer
            sprintf(message.mtext, "%d %d %s", Sequence_Number, Operation_Number, Graph_File_Name);
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Client: msgsnd");
                return 1;
            }
        }
        // Read operations
        else if (Operation_Number == 3 || Operation_Number == 4)
        {
            // Get strating vertex from user
            printf("Enter starting vertex: ");
            scanf("%d", &startingVertex);

            // Reduce 1 because of 0 based indexing
            startingVertex -= 1;

            // Store startingVertex into newArr for writing into SHM
            sprintf(newArr, "%d", startingVertex);

            // Writing to shared memory
            strcpy(shm, newArr);

            // Setting mtype of message
            message.mtype = LOAD_BALANCER_MTYPE;

            // Send request to load balancer
            sprintf(message.mtext, "%d %d %s", Sequence_Number, Operation_Number, Graph_File_Name);
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Client: msgsnd");
                return 1;
            }
        }

        // Wait for reply from servers
        if (msgrcv(qd_lb, &message, sizeof(message.mtext), (long)Sequence_Number, 0) == -1)
        {
            perror("Client: msgrcv");
            return 1;
        }

        // Display the output to the user
        printf("%s\n\n", message.mtext);

        // De-attaching the SHM segment
        if (shmdt(shm) == -1)
        {
            perror("Client: shmdt");
            exit(1);
        }

        // Deleting SHM segment
        if (shmctl(shmid, IPC_RMID, 0) == -1)
        {
            perror("Client: shmctl");
            exit(1);
        }
    }

    return 0;
}
