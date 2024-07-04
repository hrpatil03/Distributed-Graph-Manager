# Distributed-Graph-Manager

## Contributors
| Name |
| :-------- |
| `Pratik Patil` |
| `Himanshu Patil` |
| `Suyash Patil` |
| `Nishit Poddar` |
| `Chinni Vamshi Krushna` |
| `Sarvesh Borole` |
| `Atharva Chikhale` |

This project implements a multi-threaded graph traversal system using Depth-First Search (DFS) and Breadth-First Search (BFS). It includes mechanisms for synchronizing access to shared resources using semaphores and message passing via message queues. The system is designed to handle multiple clients and supports concurrent traversal operations on various graphs.

## Features

- Add new graphs to the database
- Modify existing graphs in the database
- Perform DFS on an existing graph
- Perform BFS on an existing graph

## Structures

### Message Structure
typedef struct Message {
    long mtype;               // Message type (must be greater than 0)
    char mtext[MAX_MSG_SIZE]; // Message data
} Message;

