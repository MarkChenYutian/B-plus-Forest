---
layout: default
title: 15418 Final Project
---

## Summary

{% include NotImplemented.html %}

We would like to implement a distributed B+ tree using 2 different methods:

1. Implementing parallel B+ Tree using lock-free data structure (using C++ Atomic or fine-grained lock)
2. Implementing parallel B+ Tree using OpenMPI. Specifically, for a collection of processes, we want each process to maintain a local B+ Tree and communicate the updates with each other based on lazy update. (Somehow similar to the Cache coherence system, but maintain the coherence of parallel B+ tree across processes)

## Background

### B+ Tree

{% include NotImplemented.html %}
introduce B+ Tree and its application in database management system?


### Lock-free Data Structure

Lock-free data structures enhance concurrent programming by avoiding traditional locks, thus preventing common issues like bottlenecks and deadlocks in multi-threaded environments. They use atomic operations for safe concurrent access, offering significant performance improvements in high-concurrency scenarios. However, their complex design, which addresses challenges like the ABA problem, is a trade-off for these benefits.

### Parallelism with Message Passing Interface (OpenMPI)

OpenMPI is a key implementation of the MPI standard, vital in high-performance computing. It enables efficient parallel computing across multiple nodes by facilitating advanced communication methods like point-to-point and collective messaging. Known for its efficiency, portability, and scalability, OpenMPI is essential for complex computational tasks in distributed computing environments.

## Challenge

1. B+ tree hand-over-hand 
2. Lock free programming is very hard to design and implement correctly, even with the help of `std::atomic<T>`.
3. Though OpenMPI allows us to scale up the parallelism on various hardware foundations (NUMA or single multi-core processor), it also provides a non-negligible overhead on the overall performance of program.

## Resources

1. Experiment Platform
    
    Since we plan to use OpenMPI to construct a large-scale parallelism N-body simulator, we will need to use the Bridge2-Regular Memory (Bridge2-RM) nodes in Pittsburgh Super Computing (PSC) for benchmarking.
    
    For the remaining parts of this project, we can conduct experiment on GHC cluster machines.
    
2. Papers

{% include NotImplemented.html %}
    

## Goals and Deliverables

1. [Plan to have] Implement a sequential version of B+ Tree
2. [Plan to have] Implement concurrency by fine grained lock
3. [Plan to have] Implement lock free B+ tree
4. [Plan to have] Implement a parallel N-body simulator supports efficient particle retrieval and removal based on B+ tree data structure and OpenMPI.
5. [Plan to have] Perform benchmark testing on Synthetic sequence of read/write to a randomly generated B+ tree under parallel accessing.
6. [Plan to have] Perform benchmark testing on Synthetic sequence of read/add/remove of particles during N-body simulation.
7. [Nice to have] Perform benchmark testing on Synthetic sequence of adversarial (worst-case) read/write to a manually-designed B+ tree under parallel accessing.

## Platform Choice

We decided to conduct our experiments and development on Linux platform using C++ language. Many important high performance computing (HPC) infrastructures like OpenMPI are easily available on Linux platform. C++ language provides a balanced layer of abstraction where developers have fine-grain control on the exact behavior of program while providing useful abstractions like smart pointer, and `std::atomic<T>`, which can significantly decrease the workload for developers.

## Schedule

{% include NotImplemented.html %}
