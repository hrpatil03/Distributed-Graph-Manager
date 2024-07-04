#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ipc.h>
#include <sys/msg.h>

// Macros
#define MAX_MESSAGES 100
#define MAX_MSG_SIZE 100
#define LOAD_BALANCER_MTYPE 104

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
    key_t key_mq;    // Identifier for message queue of load balancer
    int qd_lb;       // Message queue descriptor
    Message message; // Variable to read message into
    char choice;     // Choice of user

    // Set mtype of message to load balancer mtype
    message.mtype = LOAD_BALANCER_MTYPE;

    // Generate a unique key for the message queue
    if ((key_mq = ftok("load_balancer.c", 'A')) == -1)
    {
        perror("ftok");
        return 1;
    }

    // Connect to the load balancer queue
    if ((qd_lb = msgget(key_mq, 0644)) == -1)
    {
        perror("msgget");
        return 1;
    }

    // Infinite loop
    while (1)
    {
        // Ask the user to terminate load balancer
        printf("\nWant to terminate the application? Press Y (Yes) or N (No)\n");

        // Get user choice
        scanf(" %c", &choice);

        // Inform server to terminate
        if (choice == 'Y' || choice == 'y')
        {
            sprintf(message.mtext, "%s %s %s", "-1", "5", "Terminate");
            if (msgsnd(qd_lb, &message, sizeof(message.mtext), 0) == -1)
            {
                perror("Cleanup: msgsnd");
                return 1;
            }
            break;
        }
        // Continue in infinite loop
        else if (choice == 'N' || choice == 'n')
        {
            continue;
        }
        else
        {
            printf("\nEnter a valid option.\n");
            continue;
        }
    }

    return 0;
}