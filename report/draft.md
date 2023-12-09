1. producer & consumer both lock free
2. better than PALM tree because we dont reinsert
3. BATCHSIZE can modify for different use scenario
4. Scalability <-- need to explore in the report.


5. Using multithreading with pthread_create in an OpenMPI program involves combining distributed memory parallelism (via MPI) with shared memory parallelism (via POSIX Threads or pthreads). This approach can be powerful for exploiting both inter-node and intra-node parallelism in high-performance computing applications. Here's a basic outline of how to integrate pthread_create with OpenMPI:

---

# Optimization Tricks

1. Use std::vector - we can accept O(n) cost for push_front and pop_front, since they are not common in the operation sequence
    Most of the program only need to find leaf
2. Use std::vector - deque is not using contiguous memory. So cannot apply SIMD acceleration on it.
3. Still using deque in children - when manipulating children, approx. 1/2 prob to have push_front, pop_front, this is too costly. 
4. Optimize fine-lock - refactor LockDeque<T> - using statically allocated array instead of deque to speed up.
5. Use a customized Barrier - Spin lock to reduce system call to Linux kernel, significant speedup for palm tree!
6. Prefill - 
