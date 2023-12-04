---
layout: default
title: "15418 Final Project: B+ Forest"
---

## Schedule

<div style="display: flex; flex-wrap: wrap">
<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>

* ✅ **19th Nov, 2023 - Sequential B+ tree implementation**

    Implemented a sequential version of B+ tree data structure and the corresponding infrastructures like test runner, unit test, etc.
    
* ✅ **22th Nov, 2023 - Coarse grained B+ tree implementation**

    Based on the sequential version, implemented a coarse-grain B+ tree with a global mutex lock, also implemented multi-thread benchmarking & testing modules.

* ✅ **25th Nov, 2023 - Fine grained B+ tree implementation**

    Based on the sequential version of B+ tree, implemented a fine-grained lock

* ✅ **3th Dec, 2023 - Lock free B+ tree (PALM Tree) implementation**

    Based on the intel paper of PALM algorithm, we implemented the lock-free B+ tree.
</div>
<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>
* 7th Dec, 2023 - Distributed B+ tree (OpenMPI Lazy-tree) implementation

    Based on the OpenMPI, implement a distributed B+ tree, where each process own some nodes (with values in local memory) and contain some shadow-nodes (which are reference to other processes' tree nodes in other memory space).

* 7th Dec, 2023 - Benchmark infrastructure (timing, statistics) implementation

    Implement benchmark infrastructure (test-case generator, timing, performance evaluation, profiling, access pattern statistics, etc.) for all 5 different types of B+ tree implementations.

* 10th Dec, 2023 - Benchmarking the existing modules.

    Perform benchmarking, collect and analyze the benchmarking result.
</div>
</div>

<hr/>

<div style="display: flex; flex-wrap: wrap">
<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>

## Summary

In this final project, we plan to implement a series of B+ trees, including the sequential version, coarse-grain lock version, fine-grain lock version, the lock-free version (PALM Tree), and a distributed version based on OpenMPI. We will then conduct a comprehensive benchmarking on the performance of each data structure.

<img src="./assets/Trees.png" style="max-width:400px; width:40vw;"/>

**Current Progress** - Up to now, we have successfully implemented the sequential, lock-based parallel and lock-free parallel B+ trees and prepared some infrastructure for the future benchmarking work. <mark>Need further expansion</mark>

</div>

<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>

## Preliminary Results

Will be here soon... <mark>Need further expansion</mark>

</div>
</div>
<hr/>

<div style="display: flex; flex-wrap: wrap">
<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>

## Background

### B+ Tree

A B+ tree is a self-balancing tree data structure that maintains sorted data and allows searches, insertions, deletions, and sequential access in logarithmic time. Binary search tree is a specialization of B+ Tree. The B+ tree structure is well-suited for relational database management systems due to its balanced nature and ability to provide efficient range queries when it is used as index. In DBMS context, each internal node contains key ranges, which point to subtrees that contains data; each leaf node contains key/value pairs.


### Lock-free Data Structure

Lock-free data structures enhance concurrent programming by avoiding traditional locks, thus preventing common issues like bottlenecks and deadlocks in multi-threaded environments. They use atomic operations for safe concurrent access, offering significant performance improvements in high-concurrency scenarios. However, their complex design, which addresses challenges like the ABA problem, is a trade-off for these benefits.

### Parallelism with Message Passing Interface (OpenMPI)

OpenMPI is a key implementation of the MPI standard, vital in high-performance computing. It enables efficient parallel computing across multiple nodes by facilitating advanced communication methods like point-to-point and collective messaging. Known for its efficiency, portability, and scalability, OpenMPI is essential for complex computational tasks in distributed computing environments.

## Platform Choice

We decided to conduct our experiments and development on Linux platform using C++ language. Many important high performance computing (HPC) infrastructures like OpenMPI are easily available on Linux platform. C++ language provides a balanced layer of abstraction where developers have fine-grain control on the exact behavior of program while providing useful abstractions like smart pointer, and `std::atomic<T>`, which can significantly decrease the workload for developers.

## Challenge

1. Implementing hand-over-hand locking on B+ tree is hard by itself as the protocol allows multiple threads to access/modify B+ Tree at the same time. When navigating down the tree, we need to get lock for parent, and get lock for child, and release lock for parent if “safe”, which means that no split or merge when updated (i.e. not full on insertion, more than half-full on deletion).
2. Lock free programming is very hard to design and implement correctly, even with the help of `std::atomic<T>`.
3. Though OpenMPI allows us to scale up the parallelism on various hardware foundations (NUMA or single multi-core processor), it also provides a non-negligible overhead on the overall performance of program.

</div>

<div style="flex: 1; padding: 1rem; min-width: 400px;" markdown=1>

## Resources

1. Experiment Platform
    
    Since we plan to use OpenMPI to construct a large-scale parallelism N-body simulator, we will need to use the Bridge2-Regular Memory (Bridge2-RM) nodes in Pittsburgh Super Computing (PSC) for benchmarking.
    
    For the remaining parts of this project, we can conduct experiment on GHC cluster machines.
    
2. Papers on B+ tree concurrency
    
    [1] Bayer, R., Schkolnick, M. Concurrency of operations on *B*-trees. *Acta Informatica* **9**, 1–21 (1977). https://doi.org/10.1007/BF00263762
    
    [2] Sewall, J., Chhugani, J., Kim, C., Satish, N., & Dubey, P. (2011). PALM: Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors. Proceedings of the VLDB Endowment, 4(11), 795–806. https://doi.org/10.14778/3402707.3402719
    
    [3] Srinivasan, V., Carey, M.J. Performance of B+ tree concurrency control algorithms. *VLDB Journal* **2**, 361–406 (1993). https://doi.org/10.1007/BF01263046
    

## Goals and Deliverables

1. ✅ Implement a sequential version of B+ Tree
2. ✅ Implement concurrent B+ tree by coarse grained lock
3. ✅ Implement concurrent B+ tree by fine grained lock
4. ✅ Implement lock free B+ tree
5. ⏳ Implement distributed B+ tree via OpenMPI
6. ⏳ Perform benchmark testing on Synthetic sequence of read/write to a randomly generated B+ tree under parallel accessing.
7. ⏳ Analyze the benchmarking result, calculate speedup and other statistics 
8. ⏳ Perform benchmark testing on Synthetic sequence of adversarial (worst-case) read/write to a manually-designed B+ tree under parallel accessing.

</div>
</div>


<hr/>

> For historical archive version of this page, please refer to this page <a href="/15-418-Final-Project/pages/archive">Link</a>.
