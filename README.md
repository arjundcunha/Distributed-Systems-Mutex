# Distributed-Systems-Mutex

Today, everything on the internet is inherently distributed due to the massive amounts of data, processing and storage which cannot be done in a single place. With these systems growing in size and number, it is very important that they meet certain requirements.
One of these requirements is Mutual Exclusion.
When multiple systems share a single resource/resources which they all need to utilise, Mutual exclusion is a critical condition for the system to remain free from deadlocks while guaranteeing both safety and liveliness. 
Safety ensures that only one node/process/system can use the resource at a time , i.e., two systems cannot be in the critial section (Resource) at one time. This may lead to change of data, faulty processing and finally the break-down of the system.
Liveliness is condition to ensure that the system is always making progess and never gets stuck in a deadlock.
In addition, the system must also satisfy starvation freedom, where a request from a node to enter critical section is satisfied, i.e, every request is eventually granted.

There are many algorithms which try and achieve mutual exclusion of a distributed system. They try and optimise the number of messages sent for a CS request to be satisfied, reduce the average time taken to enter CS and try to ensure fairness amoung all the requesting nodes.

With respect to Mutual exclusion, we provide a working example of two famous mutual exclusion algorithms
 1) Singhal's Dynamic Information Structure. (https://pdfs.semanticscholar.org/633f/bd8e71a8936e438e2a960d95dd941de872ac.pdf)
 2) Lodha-Kshemkalyani's Fair Mutual Exclusion. (https://www.cs.uic.edu/~ajayk/ext/JKiwdc2004.pdf)
 
The programs are in Java and can run on mutiple systems. We considered the case of a non-fully connected graphs, which requires forwarding tables and certain nodes to act as routers between two different nodes.

As a basic example case, we have considered a system with three nodes connected in the following order : 1-----2-----3
Thuse 2 acts as a router for 1 and 3.
The inp-params.txt file is in the format <number of processes> <max. requests by each node> <mean delay> <cs delay>
The topology.txt file consists of the topology of the system in the format:
If there are n processes, the first n lines are the IP of the systems also with port numbers.
The next n lines is the graph inputted in the form of an adjacency matrix with undirected edges.
The last n lines is the spanning tree of the graph with directed edges.
To run the program, use 3 Terminals/Systems (base case) and input the required information in each of them.
Compile using : javac <Lodha/Singhal>.java
Then on each terminal.system, run the command  java <Lodha/Singhal> <Process ID>
Process ID is unique for each terminal/System.
We then need to input two special commands called Go <Case Sensitive>
The first Go reads all the files as input and establishes connections.
The second Go starts the simulation.

