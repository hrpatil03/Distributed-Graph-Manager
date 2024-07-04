#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
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
#define EVEN_SECONDARY_SERVER_MTYPE 102
#define ODD_SECONDARY_SERVER_MTYPE 103
#define LOAD_BALANCER_MTYPE 104
#define FILENAME_SHM "./client.c"
#define FILENAME_MQ "load_balancer.c"
#define SHM_BUFFER_SIZE 1024
#define SHM_PERMISSIONS 0666
#define MAX_NODES 30
#define MAX_THREADS 100
#define QUEUE_SIZE 40
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
    int counter[NUM_FILES];
} Semaphore;

// Structure to represent a queue
typedef struct Queue
{
    int front, rear, size;
    unsigned capacity;
    int *array;
} Queue;

// Structure for passing data to the DFS thread
typedef struct DFSThreadData
{
    int adjMatrix[MAX_NODES][MAX_NODES];
    int nodes;
    int startVertex;
    int **visited;
    int **deepestNodes;
    int *countDeep;
} DFSThreadData;

// Structure for passing data to the BFS thread
typedef struct BFSThreadData
{
    int adjMatrix[MAX_NODES][MAX_NODES];
    int nodes;
    int startVertex;
    int **visited;
    Queue *queue;
    int listOfVertices[MAX_NODES];
    int *listIndex;
    pthread_mutex_t lock;
} BFSThreadData;

// Global variables
int qd_lb;                              // Message queue descriptor
Semaphore semaphore;                    // Semaphore for files
pthread_t threadArrayMain[MAX_THREADS]; // Array to maintain child thread ids of main thread to make main thread wait for uncompleted child threads

// Function to create a queue of given capacity.
// It initializes size of queue as 0
Queue *createQueue(unsigned capacity)
{
    Queue *queue = (Queue *)malloc(sizeof(Queue));
    queue->capacity = capacity;
    queue->front = queue->size = 0;

    // This is important, see the enqueue
    queue->rear = capacity - 1;
    queue->array = (int *)malloc(queue->capacity * sizeof(int));
    return queue;
}

// Queue is full when size becomes equal to the capacity
int isFull(Queue *queue)
{
    return (queue->size == queue->capacity);
}

// Queue is empty when size is 0
int isEmpty(Queue *queue)
{
    return (queue->size == 0);
}

// Function to add an item to the queue.
// It changes rear and size
void enqueue(Queue *queue, int item)
{
    if (isFull(queue))
    {
        return;
    }
    queue->rear = (queue->rear + 1) % queue->capacity;
    queue->array[queue->rear] = item;
    queue->size = queue->size + 1;
}

// Function to remove an item from queue.
// It changes front and size
int dequeue(Queue *queue)
{
    if (isEmpty(queue))
        return INT_MIN;
    int item = queue->array[queue->front];
    queue->front = (queue->front + 1) % queue->capacity;
    queue->size = queue->size - 1;
    return item;
}

// Function to get front of queue
int front(Queue *queue)
{
    if (isEmpty(queue))
        return INT_MIN;
    return queue->array[queue->front];
}

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

// Function to perform DFS
void *dfsThread(void *data)
{
    // Initialising data variable for thread
    DFSThreadData *threadData = (DFSThreadData *)data;

    // Marking current node as visited
    *(*(threadData->visited) + threadData->startVertex) = 1;

    pthread_t threads[MAX_THREADS];          // Array to maintain child threads
    DFSThreadData newArr[threadData->nodes]; // Array to store child thread data
    int threadCount = 0;                     // Integer to maintain number of child threads
    int count = 0;                           // Counting number of nodes not reachable
    int n = threadData->nodes;               // Number of nodes in graph

    for (int i = 0; i < n; i++)
    {
        count++;
        if (threadData->adjMatrix[threadData->startVertex][i] && *(*(threadData->visited) + i) == 0)
        {
            count--;

            // Set up new thread data
            newArr[i].nodes = threadData->nodes;
            newArr[i].startVertex = i;
            newArr[i].visited = threadData->visited;
            newArr[i].countDeep = threadData->countDeep;
            newArr[i].deepestNodes = threadData->deepestNodes;
            for (int j = 0; j < n; j++)
            {
                for (int k = 0; k < n; k++)
                {
                    newArr[i].adjMatrix[j][k] = threadData->adjMatrix[j][k];
                }
            }

            // Creating a new thread for the unvisited node
            if (pthread_create(&threads[threadCount++], NULL, dfsThread, &newArr[i]) != 0)
            {
                perror("Secondary server: pthread_create");
            }

            // Wait for child threads to finish
            if (pthread_join(threads[threadCount - 1], NULL) != 0)
            {
                perror("Secondary server: pthread_join");
            }
        }
    }

    // If number of nodes not visited equals n, storing current node in deepestNodes array
    if (count == n)
    {
        // Storing the deepest vertex to the deepestNodes array
        *(*(threadData->deepestNodes) + (*(threadData->countDeep))) = threadData->startVertex;
        *(threadData->countDeep) = (*(threadData->countDeep)) + 1;
    }

    // Exiting current thread
    pthread_exit(NULL);
}

// Function to start DFS and send reply back to client
void *multithreadedDFS(void *args)
{
    // Variables
    int Sequence_Number;        // Client sequence number
    int pointless;              // Useless variable hence named pointless
    char *Graph_File_Name;      // Graph file name
    DFSThreadData data;         // Thread data for child thread to perform DFS
    int graphIndex;             // Graph index (graph no. - 1)
    FILE *gptr;                 // Graph file pointer
    key_t key_shm;              // Key (unique identifier) for SHM
    int shmid;                  // Identifier of SHM
    char *shm;                  // Character pointer to SHM segment
    char buff[SHM_BUFFER_SIZE]; // Buffer for data from SHM
    char temp1[5];              // Character array for starting vertex
    int n;                      // Number of nodes
    int *visited;               // Pointer for visited array to be allocated in heap
    int *deepestNodes;          // Pointer for deepestNodes array to be allocated in heap
    int countDeep = 0;          // Variable for maintaining index of deepestNodes array
    pthread_t startDFS;         // Thread ID
    pthread_attr_t attr;        // Thread attribute variables
    Message reply;              // Reply to send to client
    int i = 0, j = 0;           // Loop variables

    // Extracting Sequence_Number & Graph_File_Name
    getTokens(&Sequence_Number, &pointless, &Graph_File_Name, (char *)args);

    // Get index(graph no. - 1) to lock semaphore
    graphIndex = getGraphIndex(Graph_File_Name);

    // Semaphore implementation for reader end (Readers - Writers Without Starvation)
    if (sem_wait(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_wait");
    }
    semaphore.counter[graphIndex]++;
    if (semaphore.counter[graphIndex] == 1)
    {
        if (sem_wait(semaphore.rw_mutex[graphIndex]) == -1)
        {
            perror("Secondary server: sem_wait");
        }
    }
    if (sem_post(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    if (sem_post(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    // Opening graph file
    gptr = fopen(Graph_File_Name, "r");
    if (gptr == NULL)
    {
        perror("Secondary server: fopen");
        exit(1);
    }

    // Shared memory segment
    // Creating key
    if ((key_shm = ftok(FILENAME_SHM, Sequence_Number)) == -1)
    {
        perror("Secondary server: ftok\n");
        exit(1);
    }

    // Getting SHM identifier
    if ((shmid = shmget(key_shm, sizeof(char[SHM_BUFFER_SIZE]), SHM_PERMISSIONS | IPC_CREAT)) == -1)
    {
        perror("Secondary server: shmget\n");
        exit(1);
    }

    // Attaching to SHM of Client
    shm = (char *)shmat(shmid, NULL, 0);

    // Reading starting vertex from shared memory of client
    strcpy(temp1, shm);

    // De-attaching the SHM segment
    if (shmdt(shm) == -1)
    {
        perror("Secondary server: shmdt");
        exit(1);
    }

    // Initialize thread data
    data.startVertex = atoi(temp1); // Converting to integer

    fscanf(gptr, "%d", &data.nodes); // Reading number of nodes from file into thread data
    n = data.nodes;

    // Reading adjacency matrix from file into thread data
    for (i = 0; i < n; i++)
    {
        for (j = 0; j < n; j++)
        {
            fscanf(gptr, "%d", &data.adjMatrix[i][j]);
        }
    }

    // Closing file pointer
    fclose(gptr);

    // Semaphore implementation
    if (sem_wait(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_wait");
    }
    semaphore.counter[graphIndex]--;
    if (semaphore.counter[graphIndex] == 0)
    {
        if (sem_post(semaphore.rw_mutex[graphIndex]) == -1)
        {
            perror("Secondary server: sem_post");
        }
    }

    if (sem_post(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    visited = (int *)malloc(n * sizeof(int));              // DMA for visited array
    deepestNodes = (int *)malloc(MAX_NODES * sizeof(int)); // DMA for list of deepest nodes

    // Initialising visited array elements to 0
    for (i = 0; i < n; i++)
    {
        visited[i] = 0;
    }

    data.visited = &visited;           // Initialising visited of data to address of visited array
    data.deepestNodes = &deepestNodes; // Initialising deepestNodes of data to address of deepestNodes array
    data.countDeep = &countDeep;       // Initialising countDeep of data to address of countDeep

    // Create the initial thread
    pthread_attr_init(&attr);
    if (pthread_create(&startDFS, &attr, dfsThread, (void *)&data) != 0)
    {
        perror("Secondary server: pthread_create");
    }

    // Waiting for child thread to finish
    if (pthread_join(startDFS, NULL) != 0)
    {
        perror("Secondary server: pthread_join");
    }

    // Reply to client
    reply.mtext[0] = '\0';

    // Printing list of nodes in reply
    for (i = 0; i < countDeep; i++)
    {
        sprintf(reply.mtext + strlen(reply.mtext), "%d ", deepestNodes[i] + 1);
    }

    // Initialising mytpe of reply
    reply.mtype = Sequence_Number;

    // Sending reply
    if (msgsnd(qd_lb, &reply, sizeof(reply.mtext), 0) == -1)
    {
        perror("Secondary server: msgsnd");
        exit(1);
    }

    // Freeing memory allocated in heap
    free(visited);
    free(deepestNodes);

    // Removing thread id from threadArrayMain
    threadArrayMain[Sequence_Number - 1] = -1;

    // Exiting thread
    pthread_exit(NULL);
}

// Thread function to enqueue child nodes of current node
void *enqueueNodes(void *data)
{
    // Initialising thread data
    BFSThreadData *threadData = (BFSThreadData *)data;

    // Number of nodes
    int n = threadData->nodes;

    // Below section is critical section beacuse multiple threads share the same queue and may enqueue the same node twice in it
    // Start of critical section
    // Locking mutex
    pthread_mutex_lock(&(threadData->lock));
    for (int i = 0; i < n; i++)
    {
        // Exploring only those child nodes that are unvisited
        if (threadData->adjMatrix[threadData->startVertex][i] == 1 && *(*threadData->visited + i) == 0)
        {
            enqueue(threadData->queue, i);     // Enqueing child node
            *(*(threadData->visited) + i) = 1; // Updating visited for child node
        }
    }

    // End of critical section
    // Releasing mutex
    pthread_mutex_unlock(&(threadData->lock));

    // Exiting thread
    pthread_exit(NULL);
}

// Function to perform BFS
void *bfsThread(void *data)
{
    // Variables
    BFSThreadData *threadData = (BFSThreadData *)data; // Converting parameter fron void* to BFSThreadData*
    BFSThreadData newArr[threadData->nodes];           // Array of thread data for chuldren threads for child nodes
    int nodeCount;                                     // Count of nodes in queue after expanding all nodes of current level
    pthread_t threads[MAX_THREADS];                    // Array to maintain child thread ids
    int threadCount = 0;                               // Count of number of children thread created while expanding
    int currNode;                                      // Node dequeued from queue
    int n = threadData->nodes;                         // Number of odes in graph
    int i = 0, j = 0;                                  // Loop variables

    // Updating list of vertices traversed
    threadData->listOfVertices[*(threadData->listIndex)] = threadData->startVertex;

    // Updatind index of listOfVertices
    *(threadData->listIndex) = *(threadData->listIndex) + 1;

    // Enqueing current node into queue
    enqueue(threadData->queue, threadData->startVertex);

    // Updating visited array
    *(*(threadData->visited) + threadData->startVertex) = 1;

    while (!isEmpty(threadData->queue))
    {
        nodeCount = threadData->queue->size; // Number of nodes in queue to be expanded on current level
        threadCount = 0;                     // Resetting value of index of threads array

        while (nodeCount > 0)
        {
            // Dequeuing node from queue to expand
            currNode = dequeue(threadData->queue);

            // Initialising values of data structure to pass on to chuld thread to expand
            newArr[currNode].listIndex = threadData->listIndex;
            newArr[currNode].nodes = threadData->nodes;
            newArr[currNode].queue = threadData->queue;
            newArr[currNode].lock = threadData->lock;
            newArr[currNode].startVertex = currNode;
            newArr[currNode].visited = threadData->visited;
            for (int i = 0; i < n; i++)
            {
                for (int j = 0; j < n; j++)
                {
                    newArr[currNode].adjMatrix[i][j] = threadData->adjMatrix[i][j];
                }
            }
            for (int i = 0; i < n; i++)
            {
                newArr[currNode].listOfVertices[i] = threadData->listOfVertices[i];
            }

            // Create a new thread for every node in same level
            if (pthread_create(&threads[threadCount++], NULL, enqueueNodes, (void *)&newArr[currNode]) != 0)
            {
                perror("Secondary server: pthread_create");
            }

            // Reducing number of nodes remaining to expand on current level
            nodeCount--;
        }

        // Wait for all child threads to finish
        for (int i = 0; i < threadCount; i++)
        {
            if (pthread_join(threads[i], NULL) != 0)
            {
                perror("Secondary server: pthread_join");
            }
        }

        // Updating list of vertices traversed by adding the children of current level
        for (int i = 0; i < (threadData->queue)->size; i++)
        {
            int t = dequeue(threadData->queue);
            threadData->listOfVertices[*(threadData->listIndex)] = t;
            *(threadData->listIndex) = *(threadData->listIndex) + 1;
            enqueue(threadData->queue, t);
        }
    }

    // Exiting thread
    pthread_exit(NULL);
}

// Function to start BFS and send reply back to client
void *multithreadedBFS(void *args)
{
    // Variables
    int Sequence_Number;        // Client sequence number
    int pointless;              // Useless variable hence named pointless
    char *Graph_File_Name;      // Graph file name
    BFSThreadData data;         // Thread data for child thread to perform BFS
    int graphIndex;             // Graph index (graph no. - 1)
    FILE *gptr;                 // Graph file pointer
    key_t key_shm;              // Key (unique identifier) for SHM
    int shmid;                  // Identifier of SHM
    char *shm;                  // Character pointer to SHM segment
    char buff[SHM_BUFFER_SIZE]; // Buffer for data from SHM
    char temp1[5];              // Character array for starting vertex
    int n;                      // Number of nodes
    int listIndex = 0;          // Variable to keep track of index of the list of nodes
    pthread_t startBFS;         // Thread ID
    pthread_attr_t attr;        // Thread attribute variables
    Message reply;              // Reply to send to client
    int i = 0, j = 0;           // Loop variables

    // Extracting Sequence_Number & Graph_File_Name
    getTokens(&Sequence_Number, &pointless, &Graph_File_Name, (char *)args);

    // Get index(graph no. - 1) to lock semaphore
    graphIndex = getGraphIndex(Graph_File_Name);

    // Semaphore implementation for reader end (Readers - Writers Without Starvation)
    if (sem_wait(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_wait");
    }
    semaphore.counter[graphIndex]++;
    if (semaphore.counter[graphIndex] == 1)
    {
        if (sem_wait(semaphore.rw_mutex[graphIndex]) == -1)
        {
            perror("Secondary server: sem_wait");
        }
    }

    if (sem_post(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    if (sem_post(semaphore.in_mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    // Opening graph file
    gptr = fopen(Graph_File_Name, "r");
    if (gptr == NULL)
    {
        perror("Secondary server: fopen");
        exit(1);
    }

    // Shared memory segment
    // Creating key
    if ((key_shm = ftok(FILENAME_SHM, Sequence_Number)) == -1)
    {
        perror("Secondary server: ftok\n");
        exit(1);
    }

    // Getting SHM identifier
    if ((shmid = shmget(key_shm, sizeof(char[SHM_BUFFER_SIZE]), SHM_PERMISSIONS | IPC_CREAT)) == -1)
    {
        perror("Secondary server: shmget\n");
        exit(1);
    }

    // Attaching to SHM of Client
    shm = (char *)shmat(shmid, NULL, 0);

    // Reading starting vertex from shared memory of client
    strcpy(temp1, shm);

    // De-attaching the SHM segment
    if (shmdt(shm) == -1)
    {
        perror("Secondary server: shmdt");
        exit(1);
    }

    // Initialize thread data
    data.startVertex = atoi(temp1); // Converting to integer

    fscanf(gptr, "%d", &data.nodes); // Reading number of nodes from file
    n = data.nodes;

    // Reading adjacency matrix from file into thread data
    for (i = 0; i < n; i++)
    {
        for (j = 0; j < n; j++)
        {
            fscanf(gptr, "%d", &data.adjMatrix[i][j]);
        }
    }

    // Closing file pointer
    fclose(gptr);

    // Semaphore implementation
    if (sem_wait(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_wait");
    }
    semaphore.counter[graphIndex]--;
    if (semaphore.counter[graphIndex] == 0)
    {
        if (sem_post(semaphore.rw_mutex[graphIndex]) == -1)
        {
            perror("Secondary server: sem_post");
        }
    }
    if (sem_post(semaphore.mutex[graphIndex]) == -1)
    {
        perror("Secondary server: sem_post");
    }

    int *visited = (int *)malloc(n * sizeof(int)); // DMA for visited array

    // Initialising visited array elements to 0
    for (i = 0; i < n; i++)
    {
        visited[i] = 0;
    }

    // Initialising listOfVertices array elements to -1
    for (i = 0; i < MAX_NODES; i++)
    {
        data.listOfVertices[i] = -1;
    }
    data.visited = &visited;              // Initialising visited of data to address of visited array
    data.queue = createQueue(QUEUE_SIZE); // Initialising queue of data by creating queue for BFS traversal
    data.listIndex = &listIndex;          // Initialising listIndex of data to address of listIndex

    // Initialize Mutex
    if (pthread_mutex_init((pthread_mutex_t *)&(data.lock), NULL) != 0)
    {
        perror("Secondary server: pthread_mutex_init");
        exit(1);
    }

    // Create the initial thread
    pthread_attr_init(&attr);
    if (pthread_create(&startBFS, &attr, bfsThread, (void *)&data) != 0)
    {
        perror("Secondary server: pthread_create");
    }

    // Waiting for child thread to finish
    if (pthread_join(startBFS, NULL) != 0)
    {
        perror("Secondary server: pthread_join");
    }

    // Destroying mutex
    if (pthread_mutex_destroy((pthread_mutex_t *)&(data.lock)) != 0)
    {
        perror("Secondary server: thread_mutex_destroy");
        exit(1);
    }

    // Reply to client
    reply.mtext[0] = '\0';

    // Printing list of nodes in reply
    for (i = 0; i < listIndex; i++)
    {
        sprintf(reply.mtext + strlen(reply.mtext), "%d ", data.listOfVertices[i] + 1);
    }

    // Initialising mytpe of reply
    reply.mtype = Sequence_Number;

    // Sending reply
    if (msgsnd(qd_lb, &reply, sizeof(reply.mtext), 0) == -1)
    {
        perror("Secondary server: msgsnd");
        exit(1);
    }

    // Freeing memory allocated in heap
    free(visited);
    free(data.queue->array);
    free(data.queue);

    // Removing thread id from threadArrayMain
    threadArrayMain[Sequence_Number - 1] = -1;

    // Exiting thread
    pthread_exit(NULL);
}

// Main function
int main()
{
    // Variables
    key_t key_mq;                // Identifier for message queue of load balancer
    Message message;             // Variable to read message into
    long secondary_server_mtype; // Type of server (even || odd)
    int temp;                    // Temporary variable for server type (odd = 1 || even = 2)
    int Sequence_Number;         // Client sequence number
    int Operation_Number;        // Opertaion number to perform
    char *Graph_File_Name;       // Name of graph file to operate on
    int i = 0;                   // Loop variable

    printf("Enter secondary server type (Odd -> 1 / Even -> 2): ");
    scanf("%d", &temp);

    if (temp % 2 == 1)
    {
        secondary_server_mtype = ODD_SECONDARY_SERVER_MTYPE;
    }
    else if (temp % 2 == 0)
    {
        secondary_server_mtype = EVEN_SECONDARY_SERVER_MTYPE;
    }

    // Initialize semaphores for each graph file
    for (int i = 0; i < NUM_FILES; ++i)
    {
        semaphore.mutex[i] = sem_open((const char *)names[0][i], O_EXCL, 0644, 1);
        if (semaphore.mutex[i] == NULL)
        {
            perror("Secondary server: sem_open in");
            return 1;
        }
        semaphore.rw_mutex[i] = sem_open((const char *)names[1][i], O_EXCL, 0644, 1);
        if (semaphore.rw_mutex[i] == NULL)
        {
            perror("Secondary server: sem_open out");
            return 1;
        }
        semaphore.in_mutex[i] = sem_open((const char *)names[2][i], O_EXCL, 0644, 1);
        if (semaphore.in_mutex[i] == NULL)
        {
            perror("Secondary server: sem_open write");
            return 1;
        }
        semaphore.counter[i] = 0;
    }

    // Initialising threadArrayMain to -1
    for (i = 0; i < MAX_THREADS; i++)
    {
        threadArrayMain[i] = -1;
    }

    // Generate a unique key for the message queue
    if ((key_mq = ftok(FILENAME_MQ, 'A')) == -1)
    {
        perror("Secondary server: ftok");
        return 1;
    }

    // Connect to the load balancer queue
    if ((qd_lb = msgget(key_mq, 0644)) == -1)
    {
        perror("Secondary server: msgget");
        return 1;
    }

    // Infinite loop
    while (1)
    {
        // Receive a request from load balancer
        if (msgrcv(qd_lb, &message, sizeof(message.mtext), secondary_server_mtype, 0) == -1)
        {
            perror("Secondary server: msgrcv");
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
                        perror("Secondary server: pthread_join");
                    }
                }
            }

            break;
        }
        // Perform DFS
        else if (Operation_Number == 3)
        {
            // Data for child thread
            char params[25];
            sprintf(params, "%d %d %s", Sequence_Number, 1, Graph_File_Name);

            // Creating thread for DFS traversal
            pthread_t tid;
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            if (pthread_create(&tid, &attr, multithreadedDFS, (void *)params) != 0)
            {
                perror("Secondary server: pthread_create");
            }

            // Storing tid in threadArrayMain
            threadArrayMain[Sequence_Number - 1] = tid;
        }
        // Perform BFS
        else if (Operation_Number == 4)
        {
            // Data for child thread
            char params[25];
            sprintf(params, "%d %d %s", Sequence_Number, 1, Graph_File_Name);

            // Creating thread for BFS traversal
            pthread_t tid;
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            if (pthread_create(&tid, &attr, multithreadedBFS, (void *)params) != 0)
            {
                perror("Secondary server: pthread_create");
            }

            // Storing tid in threadArrayMain
            threadArrayMain[Sequence_Number - 1] = tid;
        }
    }

    // Clean-up activities
    // Close semaphores
    for (int i = 0; i < NUM_FILES; ++i)
    {
        if (sem_close(semaphore.mutex[i]) == -1)
        {
            perror("Secondary server: sem_close");
            return 1;
        }

        if (sem_close(semaphore.rw_mutex[i]) == -1)
        {
            perror("Secondary server: sem_close");
            return 1;
        }

        if (sem_close(semaphore.in_mutex[i]) == -1)
        {
            perror("Secondary server: sem_close");
            return 1;
        }
    }

    message.mtype = LOAD_BALANCER_MTYPE;
    sprintf(message.mtext, "Completed secondary %ld", secondary_server_mtype);
    if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
    {
        perror("Secondary server: msgsnd");
        return 1;
    }

    return 0;
}
