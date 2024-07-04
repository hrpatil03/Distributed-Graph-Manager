#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <semaphore.h>

// Macros
#define MAX_MSG_SIZE 100
#define LOAD_BALANCER_MTYPE 104
#define FILENAME_MQ "load_balancer.c"
#define PRIMARY_SERVER_MTYPE 101
#define EVEN_SECONDARY_SERVER_MTYPE 102
#define ODD_SECONDARY_SERVER_MTYPE 103
#define NUM_FILES 20

// Unique names for named semaphores  (60)
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

// Main function
int main()
{
    // Variables
    key_t key_mq;          // Identifier for message queue of load balancer
    int qd_lb;             // Message queue descriptor
    Message message;       // Variable to read message into
    Semaphore semaphore;   // Semaphore for files
    int Sequence_Number;   // Client sequence number
    int Operation_Number;  // Opertaion number to perform
    char *Graph_File_Name; // Name of graph file to operate on
    int i = 0;             // Loop variable

    // Initialize semaphores for each file
    for (i = 0; i < NUM_FILES; ++i)
    {
        semaphore.mutex[i] = sem_open((const char *)names[0][i], O_CREAT | O_EXCL, 0644, 1);
        if (semaphore.mutex[i] == NULL)
        {
            perror("Load balancer: sem_open mutex");
            return 1;
        }
        semaphore.rw_mutex[i] = sem_open((const char *)names[1][i], O_CREAT | O_EXCL, 0644, 1);
        if (semaphore.rw_mutex[i] == NULL)
        {
            perror("Load balancer: sem_open rw_mutex");
            return 1;
        }
        semaphore.in_mutex[i] = sem_open((const char *)names[2][i], O_CREAT | O_EXCL, 0644, 1);
        if (semaphore.in_mutex[i] == NULL)
        {
            perror("Load balancer: sem_open in_mutex");
            return 1;
        }
    }

    // Generate a unique key for the message queue
    if ((key_mq = ftok(FILENAME_MQ, 'A')) == -1)
    {
        perror("Load balancer: ftok");
        return 1;
    }

    // Create a message queue
    if ((qd_lb = msgget(key_mq, 0644 | IPC_CREAT)) == -1)
    {
        perror("Load balancer: msgget");
        return 1;
    }

    // Infinite loop
    while (1)
    {
        // Receive a request from a client
        if (msgrcv(qd_lb, &message, sizeof(message.mtext), LOAD_BALANCER_MTYPE, 0) == -1)
        {
            perror("Load balancer: msgrcv");
            return 1;
        }

        // Extracting Sequence_Number & Graph_File_Name
        getTokens(&Sequence_Number, &Operation_Number, &Graph_File_Name, message.mtext);

        // Re-initialising mtext
        // This statement is necessary as using tokenizer changes mtext
        sprintf(message.mtext, "%d %d %s", Sequence_Number, Operation_Number, Graph_File_Name);

        // Terminate
        if (Sequence_Number == -1)
        {
            message.mtype = PRIMARY_SERVER_MTYPE;
            // Primary server
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Load balancer: msgsnd");
                return 1;
            }
            // Even secondary server
            message.mtype = EVEN_SECONDARY_SERVER_MTYPE;
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Load balancer: msgsnd");
                return 1;
            }
            // Odd secondary server
            message.mtype = ODD_SECONDARY_SERVER_MTYPE;
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Load balancer: msgsnd");
                return 1;
            }

            break;
        }
        else
        {
            // Primary server
            if (Operation_Number == 1 || Operation_Number == 2)
            {
                message.mtype = PRIMARY_SERVER_MTYPE;

                // Send message
                if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
                {
                    perror("Load balancer: msgsnd");
                    return 1;
                }
            }
            // Secondary server
            else if (Operation_Number == 3 || Operation_Number == 4)
            {
                // Odd secondary server
                if (Sequence_Number % 2 == 1)
                {
                    message.mtype = ODD_SECONDARY_SERVER_MTYPE;

                    // Send message
                    if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
                    {
                        perror("Load balancer: msgsnd");
                        return 1;
                    }
                }
                // Even secondary server
                else if (Sequence_Number % 2 == 0)
                {
                    message.mtype = EVEN_SECONDARY_SERVER_MTYPE;

                    // Send message
                    if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
                    {
                        perror("Load balancer: msgsnd");
                        return 1;
                    }
                }
            }
        }
    }

    // Cleanup activities
    // Sleep for 5 seconds
    sleep(5);

    if (msgrcv(qd_lb, &message, sizeof(message.mtext), LOAD_BALANCER_MTYPE, 0) == -1)
    {
        perror("Load balancer: msgrcv");
        return 1;
    }
    if (msgrcv(qd_lb, &message, sizeof(message.mtext), LOAD_BALANCER_MTYPE, 0) == -1)
    {
        perror("Load balancer: msgrcv");
        return 1;
    }
    if (msgrcv(qd_lb, &message, sizeof(message.mtext), LOAD_BALANCER_MTYPE, 0) == -1)
    {
        perror("Load balancer: msgrcv");
        return 1;
    }

    // Close semaphores
    for (int i = 0; i < NUM_FILES; ++i)
    {
        if (sem_close(semaphore.mutex[i]) == -1)
        {
            perror("Load balancer: sem_close");
            return 1;
        }

        if (sem_close(semaphore.rw_mutex[i]) == -1)
        {
            perror("Load balancer: sem_close");
            return 1;
        }

        if (sem_close(semaphore.in_mutex[i]) == -1)
        {
            perror("Load balancer: sem_close");
            return 1;
        }
    }

    // Unlink semaphores
    for (int i = 0; i < NUM_FILES; ++i)
    {
        if (sem_unlink((const char *)names[0][i]) == -1)
        {
            perror("Load balancer: sem_unlink");
            return 1;
        }

        if (sem_unlink((const char *)names[1][i]) == -1)
        {
            perror("Load balancer: sem_unlink");
            return 1;
        }

        if (sem_unlink((const char *)names[2][i]) == -1)
        {
            perror("Load balancer: sem_unlink");
            return 1;
        }
    }

    // Remove the message queue
    if (msgctl(qd_lb, IPC_RMID, NULL) == -1)
    {
        perror("Load balancer: msgctl");
        return 1;
    }

    return 0;
}
