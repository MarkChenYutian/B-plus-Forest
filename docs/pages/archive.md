---
layout: page
title: 15418 Final Project Proposal (Archive)
---

<div style="background-color: rgb(254, 224, 125); padding: .5rem .5rem .5rem .25rem; border-radius: .5rem;">
<span style="font-size: x-large;">🚧</span> 
This is an historical archive page. For the latest version of project report, please refer to this <a href="/15-418-Final-Project/">Link</a>
</div>


## Summary

We would like to implement parallel and distributed B+ tree using 2 different methods:
1. Implementing parallel B+ Tree using fine-grained locking and lock-free data structure (using C++ Atomic)
2. Implementing a parallel N-body simulator supports efficient particle retrieval, insertion, and removal based on B+ tree data structure and OpenMPI. Specifically, for a collection of processes, we want each process to maintain a local B+ Tree and communicate the updates through MPI. The simulator will also support efficient query, addition and removal of certain particle(s). <small class="side" id="container">Something</small>

## Background

### B+ Tree

A B+ tree is a self-balancing tree data structure that maintains sorted data and allows searches, insertions, deletions, and sequential access in logarithmic time. Binary search tree is a specialization of B+ Tree. The B+ tree structure is well-suited for relational database management systems due to its balanced nature and ability to provide efficient range queries when it is used as index. In DBMS context, each internal node contains key ranges, which point to subtrees that contains data; each leaf node contains key/value pairs.


### Lock-free Data Structure

Lock-free data structures enhance concurrent programming by avoiding traditional locks, thus preventing common issues like bottlenecks and deadlocks in multi-threaded environments. They use atomic operations for safe concurrent access, offering significant performance improvements in high-concurrency scenarios. However, their complex design, which addresses challenges like the ABA problem, is a trade-off for these benefits.

### Parallelism with Message Passing Interface (OpenMPI)

OpenMPI is a key implementation of the MPI standard, vital in high-performance computing. It enables efficient parallel computing across multiple nodes by facilitating advanced communication methods like point-to-point and collective messaging. Known for its efficiency, portability, and scalability, OpenMPI is essential for complex computational tasks in distributed computing environments.

## Challenge

1. Implementing hand-over-hand locking on B+ tree is hard by itself as the protocol allows multiple threads to access/modify B+ Tree at the same time. When navigating down the tree, we need to get lock for parent, and get lock for child, and release lock for parent if “safe”, which means that no split or merge when updated (i.e. not full on insertion, more than half-full on deletion).
2. Lock free programming is very hard to design and implement correctly, even with the help of `std::atomic<T>`.
3. Though OpenMPI allows us to scale up the parallelism on various hardware foundations (NUMA or single multi-core processor), it also provides a non-negligible overhead on the overall performance of program.

## Resources

1. Experiment Platform
    
    Since we plan to use OpenMPI to construct a large-scale parallelism N-body simulator, we will need to use the Bridge2-Regular Memory (Bridge2-RM) nodes in Pittsburgh Super Computing (PSC) for benchmarking.
    
    For the remaining parts of this project, we can conduct experiment on GHC cluster machines.
    
2. Papers on B+ tree concurrency
    
    [1] Bayer, R., Schkolnick, M. Concurrency of operations on *B*-trees. *Acta Informatica* **9**, 1–21 (1977). https://doi.org/10.1007/BF00263762
    
    [2] Sewall, J., Chhugani, J., Kim, C., Satish, N., & Dubey, P. (2011). PALM: Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors. Proceedings of the VLDB Endowment, 4(11), 795–806. https://doi.org/10.14778/3402707.3402719
    
    [3] Srinivasan, V., Carey, M.J. Performance of B+ tree concurrency control algorithms. *VLDB Journal* **2**, 361–406 (1993). https://doi.org/10.1007/BF01263046
    

## Goals and Deliverables

1. [Plan to have] Implement a sequential version of B+ Tree
2. [Plan to have] Implement concurrent B+ tree by coarse grained and fine grained lock
3. [Plan to have] Implement lock free B+ tree
4. [Plan to have] Implement a parallel N-body simulator supports efficient particle retrieval and removal based on B+ tree data structure and OpenMPI. (each process has a "local" B+ tree)
6. [Plan to have] Perform benchmark testing on Synthetic sequence of read/write to a randomly generated B+ tree under parallel accessing.
7. [Plan to have] Perform benchmark testing on Synthetic sequence of read/add/remove of particles during N-body simulation.
8. [Nice to have] Perform benchmark testing on Synthetic sequence of adversarial (worst-case) read/write to a manually-designed B+ tree under parallel accessing.

## Platform Choice

We decided to conduct our experiments and development on Linux platform using C++ language. Many important high performance computing (HPC) infrastructures like OpenMPI are easily available on Linux platform. C++ language provides a balanced layer of abstraction where developers have fine-grain control on the exact behavior of program while providing useful abstractions like smart pointer, and `std::atomic<T>`, which can significantly decrease the workload for developers.

## Schedule

* 19th Nov, 2023 - Fine grained B+ tree implementation
* 26th Nov, 2023 - Lock free B+ tree implementation
* 3rd Dec, 2023  - MPI N-body simulator with distributed B+ tree
* 10th Dec, 2023 - Benchmarking the existing modules.

