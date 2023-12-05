1. producer & consumer both lock free
2. better than PALM tree because we dont reinsert
3. BATCHSIZE can modify for different use scenario
4. Scalability <-- need to explore in the report.


5. Using multithreading with pthread_create in an OpenMPI program involves combining distributed memory parallelism (via MPI) with shared memory parallelism (via POSIX Threads or pthreads). This approach can be powerful for exploiting both inter-node and intra-node parallelism in high-performance computing applications. Here's a basic outline of how to integrate pthread_create with OpenMPI:
