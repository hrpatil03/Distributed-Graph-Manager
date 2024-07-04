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
#include <pthread.h>
#include <semaphore.h>

// Macros
#define MAX_MSG_SIZE 100
#define PRIMARY_SERVER_MTYPE 101
#define LOAD_BALANCER_MTYPE 104
#define FILENAME_SHM "./client.c"
#define FILENAME_MQ "load_balancer.c"
#define SHM_BUFFER_SIZE 1024
#define SHM_PERMISSIONS 0666
#define MAX_THREADS 100
#define NUM_FILES 20

// Unique names for named semaphores
char names[3][20][2] = {{"aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj", "ak", "al", "am", "an", "ao", "ap", "aq", "ar", "as", "at"},
                        {"ba", "bb", "bc", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bk", "bl", "bm", "bn", "bo", "bp", "bq", "br", "bs", "bt"},
                        {"ca", "cb", "cc", "cd", "ce", "cf", "cg", "ch", "ci", "cj", "ck", "cl", "cm", "cn", "co", "cp", "cq", "cr", "cs", "ct"}};

// Structure for the message
typedef struct Message
{
    long mtype;               // Message type (must be greater than 0)
    char mtext[MAX_MSG_SIZE]; // Message data
} Message;

// Structure for semaphore
typedef struct Semaphore
{
    sem_t *mutex[NUM_FILES];    // Semaphores for each file
    sem_t *rw_mutex[NUM_FILES]; // Semaphores for each file
    sem_t *in_mutex[NUM_FILES]; // Semaphores for each file
} Semaphore;

// Global variables
int qd_lb;                              // Message queue descriptor
Semaphore semaphore;                    // Semaphore for files
pthread_t threadArrayMain[MAX_THREADS]; // Array to maintain child thread ids of main thread to make main thread wait for uncompleted child threads

// Function to decode message from message queue
void getTokens(int *seq_no, int *op_no, char **g_name, char *request)
{
    // Returns first token
    char *token = strtok(request, " ");
    *seq_no = atoi(token);

    // Returns second token
    token = strtok(NULL, " ");
    *op_no = atoi(token);

    // Returns third token
    token = strtok(NULL, "\0");
    *g_name = token;

    return;
}

// Function to get graph index (graph no. - 1) for locking
int getGraphIndex(char *g_name)
{
    int i = 1;
    char num[3] = {'\0', '\0', '\0'};
    while (g_name[i] != '.')
    {
        num[i - 1] = g_name[i];
        i++;
    }
    return atoi(num) - 1;
}

// Function to add graph to database by creating new graph file
void *addGraph(void *args)
{
    // Variables
    int Sequence_Number;                // Client sequence number
    int pointless;                      // Useless variable hence named pointless
    char *Graph_File_Name;              // Graph file name
    int graphIndex;                     // Graph index (graph no. - 1)
    FILE *gptr;                         // Graph file pointer
    key_t key_shm;                      // Key (unique identifier) for SHM
    int shmid;                          // Identifier of SHM
    char *shm;                          // Character pointer to SHM segment
    char temp1[3] = {'\0', '\0', '\0'}; // Character array to read nodes from SHM
    int k = 0;                          // Index for SHM
    int nodes;                          // Nodes of graph
    Message reply;                      // Reply to send to client
    int i = 0, j = 0;                   // Loop variables

    // Extracting Sequence_Number & Graph_File_Name
    getTokens(&Sequence_Number, &pointless, &Graph_File_Name, (char *)args);

    // Get index(graph no. - 1) to lock semaphore
    graphIndex = getGraphIndex(Graph_File_Name);

    // Semaphore implementation for writer end (Readers - Writers Without Starvation)
    // NOTE: Semaphore is necessary here because if a read request to read same graph arrives just after getting the request to add graph, it might clash with this write request
    if (sem_wait(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_wait");
    }

    if (sem_wait(semaphore.rw_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_wait");
    }

    // Opening graph file
    gptr = fopen(Graph_File_Name, "w");
    if (gptr == NULL)
    {
        perror("Primary server: fopen");
        exit(1);
    }

    // Shared memory segment
    // Creating key
    if ((key_shm = ftok(FILENAME_SHM, Sequence_Number)) == -1)
    {
        perror("Primary server: ftok");
        exit(1);
    }

    // Getting SHM identifier
    if ((shmid = shmget(key_shm, sizeof(char[SHM_BUFFER_SIZE]), SHM_PERMISSIONS | IPC_CREAT)) == -1)
    {
        perror("Primary server: shmget");
        exit(1);
    }

    // Attaching to SHM of Client
    shm = (char *)shmat(shmid, NULL, 0);

    // Reading number of nodes from SHM into temp array
    while (shm[k] != ',')
    {
        temp1[k] = *(shm + k);
        k++;
    }
    k++;

    // Converting character numerals into integer
    nodes = atoi(temp1);

    // Writing number of nodes into file
    fprintf(gptr, "%d\n", nodes);

    // Reading adjacency matrix from SHM and writing it directly into file
    for (i = 0; i < nodes; i++)
    {
        for (j = 0; j < nodes; j++)
        {
            fprintf(gptr, "%c ", *(shm + k));
            k += 2;
        }
        fprintf(gptr, "\n");
    }

    // De-attaching the SHM segment
    if (shmdt(shm) == -1)
    {
        perror("Primary server: shmdt");
        exit(1);
    }

    // Closing file pointer
    fclose(gptr);

    // Semaphore implementation
    if (sem_post(semaphore.rw_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_post");
    }

    if (sem_post(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_post");
    }

    // Reply to client
    sprintf(reply.mtext, "File successfully added");

    // Initialising mytpe of reply
    reply.mtype = Sequence_Number;

    // Sending reply
    if (msgsnd(qd_lb, &reply, sizeof(reply.mtext), 0) == -1)
    {
        perror("Primary server: msgsnd");
        exit(1);
    }

    // Removing thread id from threadArrayMain
    threadArrayMain[Sequence_Number - 1] = -1;

    // Exiting thread
    pthread_exit(NULL);
}

// Function to modify graph
void *modifyGraph(void *args)
{
    // Variables
    int Sequence_Number;                // Client sequence number
    int pointless;                      // Useless variable hence named pointless
    char *Graph_File_Name;              // Graph file name
    int graphIndex;                     // Graph index (graph no. - 1)
    FILE *gptr;                         // Graph file pointer
    key_t key_shm;                      // Key (unique identifier) for SHM
    int shmid;                          // Identifier of SHM
    char *shm;                          // Character pointer to SHM segment
    char temp1[3] = {'\0', '\0', '\0'}; // Character array to read nodes from SHM
    int k = 0;                          // Index for SHM
    int nodes;                          // Nodes of graph
    Message reply;                      // Reply to send to client
    int i = 0, j = 0;                   // Loop variables

    // Extracting Sequence_Number & Graph_File_Name
    getTokens(&Sequence_Number, &pointless, &Graph_File_Name, (char *)args);

    // Get index(graph no. - 1) to lock semaphore
    graphIndex = getGraphIndex(Graph_File_Name);

    // Semaphore implementation for writer end (Readers - Writers Without Starvation)
    if (sem_wait(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_wait");
    }

    if (sem_wait(semaphore.rw_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_wait");
    }

    // Opening graph file
    gptr = fopen(Graph_File_Name, "w");
    if (gptr == NULL)
    {
        perror("Primary server: fopen");
        exit(1);
    }

    // Shared memory segment
    // Creating key
    if ((key_shm = ftok(FILENAME_SHM, Sequence_Number)) == -1)
    {
        perror("Primary server: ftok");
        exit(1);
    }

    // Getting SHM identifier
    if ((shmid = shmget(key_shm, sizeof(char[SHM_BUFFER_SIZE]), SHM_PERMISSIONS | IPC_CREAT)) == -1)
    {
        perror("Primary server: shmget");
        exit(1);
    }

    // Attaching to SHM of Client
    shm = (char *)shmat(shmid, NULL, 0);

    // Reading number of nodes from SHM into temp array
    while (shm[k] != ',')
    {
        temp1[k] = *(shm + k);
        k++;
    }
    k++;

    // Converting character numerals into integer
    nodes = atoi(temp1);

    // Writing number of nodes into file
    fprintf(gptr, "%d\n", nodes);

    // Reading adjacency matrix from SHM and writing it directly into file
    for (i = 0; i < nodes; i++)
    {
        for (j = 0; j < nodes; j++)
        {
            fprintf(gptr, "%c ", *(shm + k));
            k += 2;
        }
        fprintf(gptr, "\n");
    }

    // De-attaching the SHM segment
    if (shmdt(shm) == -1)
    {
        perror("Primary server: shmdt");
        exit(1);
    }

    // Closing file pointer
    fclose(gptr);

    // Semaphore implementation
    if (sem_post(semaphore.rw_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_post");
    }

    if (sem_post(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Primary server: sem_post");
    }

    // Reply to client
    sprintf(reply.mtext, "File successfully modified");

    // Initialising mytpe of reply
    reply.mtype = Sequence_Number;

    // Sending reply
    if (msgsnd(qd_lb, &reply, sizeof(reply.mtext), 0) == -1)
    {
        perror("Primary server: msgsnd");
        exit(1);
    }

    // Removing thread id from threadArrayMain
    threadArrayMain[Sequence_Number - 1] = -1;

    // Exiting thread
    pthread_exit(NULL);
}

// Main function
int main()
{   
    // Variables
    key_t key_mq;          // Identifier for message queue of load balancer
    Message message;       // Variable to read message into
    int Sequence_Number;   // Client sequence number
    int Operation_Number;  // Opertaion number to perform
    char *Graph_File_Name; // Name of graph file to operate on
    int i = 0;             // Loop variable

    // Initialize semaphores for each file
    for (i = 0; i < NUM_FILES; ++i)
    {
        semaphore.mutex[i] = sem_open((const char *)names[0][i], O_EXCL, 0644, 1);
        if (semaphore.mutex[i] == NULL)
        {
            perror("Primary server: sem_open in");
            return 1;
        }
        semaphore.in_mutex[i] = sem_open((const char *)names[1][i], O_EXCL, 0644, 1);
        if (semaphore.in_mutex[i] == NULL)
        {
            perror("Primary server: sem_open out");
            return 1;
        }
        semaphore.rw_mutex[i] = sem_open((const char *)names[2][i], O_EXCL, 0644, 1);
        if (semaphore.rw_mutex[i] == NULL)
        {
            perror("Primary server: sem_open write");
            return 1;
        }
    }

    // Initialising threadArrayMain to -1
    for (i = 0; i < MAX_THREADS; i++)
    {
        threadArrayMain[i] = -1;
    }

    // Generate a unique key for the message queue
    if ((key_mq = ftok(FILENAME_MQ, 'A')) == -1)
    {
        perror("Primary server: ftok");
        return 1;
    }

    // Connect to the load balancer queue
    if ((qd_lb = msgget(key_mq, 0644)) == -1)
    {
        perror("Primary server: msgget");
        return 1;
    }
    
    // Infinite loop
    while (1)
    {
        // Receive a request from load balancer
        if (msgrcv(qd_lb, &message, sizeof(message.mtext), PRIMARY_SERVER_MTYPE, 0) == -1)
        {
            perror("Primary server: msgrcv");
            return 1;
        }

        // Extracting Sequence_Number & Graph_File_Name
        getTokens(&Sequence_Number, &Operation_Number, &Graph_File_Name, message.mtext);

        // Terminate
        if (Sequence_Number == -1)
        {
            for (i = 0; i < MAX_THREADS; i++)
            {
                if (threadArrayMain[i] != -1)
                {
                    if (pthread_join(threadArrayMain[i], NULL) != 0)
                    {
                        perror("Primary server: pthread_join");
                    }
                }
            }
            
            break;
        }
        // Add Graph
        else if (Operation_Number == 1)
        {
            char params[20];
            sprintf(params, "%d %d %s", Sequence_Number, 1, Graph_File_Name);

            pthread_t tid;
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            if (pthread_create(&tid, &attr, addGraph, (void *)params) != 0)
            {
                perror("Primary server: pthread_create main");
            }

            // Storing tid in threadArrayMain
            threadArrayMain[Sequence_Number - 1] = tid;
        }
        // Modify Graph
        else if (Operation_Number == 2)
        {
            char params[20];
            sprintf(params, "%d %d %s", Sequence_Number, 1, Graph_File_Name);

            pthread_t tid;
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            if (pthread_create(&tid, &attr, modifyGraph, (void *)params) != 0)
            {
                perror("Primary server: pthread_create main");
            }

            // Storing tid in threadArrayMain
            threadArrayMain[Sequence_Number - 1] = tid;
        }
    }

    // Clean-up activities
    // Close semaphores
    for (i = 0; i < NUM_FILES; ++i)
    {
        if (sem_close(semaphore.mutex[i]) == -1)
        {
            perror("Primary server: sem_close");
            return 1;
        }

        if (sem_close(semaphore.rw_mutex[i]) == -1)
        {
            perror("Primary server: sem_close");
            return 1;
        }

        if (sem_close(semaphore.in_mutex[i]) == -1)
        {
            perror("Primary server: sem_close");
            return 1;
        }
    }

    message.mtype = LOAD_BALANCER_MTYPE;
    sprintf(message.mtext, "Completed primary");
    if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
    {
        perror("Primary server: msgsnd");
        return 1;
    }

    return 0;
}