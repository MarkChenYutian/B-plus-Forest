/**
 * @brief Implement lock-free B+ tree according to Intel's PALM Tree paper
 * https://dl.acm.org/doi/pdf/10.14778/3402707.3402719
 * 
 * STAGE 1 - Batching & Parallel Search
 * 
 * Collect a batch of requests - GET/DELETE/INSERT (LeafNodeOP)
 * Distribute requests to all workers, retrieve the leaf node each request 
 * will need to access.
 * 
 * STAGE 2 - Redistribute Work & Modify Leaves
 * 
 * Based on the leaves retrieved in stage 1, bundle the tasks s.t. threads
 * do not interfere with each other (every leaf node is accessed by at most
 * one thread).
 * 
 * Before executing on leaf node, resolve the GET requests first based on leaf 
 * node and current batch requests.
 * 
 * STAGE 3 - Modify Internal Nodes, Redistribute Work, recursive until Root
 * 
 * Redistribute the modification requests from previous layer (REDISTRIBUTE)
 * 
 * STAGE 4 - A single thread modify the root node
 * 
 * TODO: async, future, promise API
 */
#include "freetree.h"

namespace Tree {
    template<typename T>
    FreeBPlusTree<T>::FreeBPlusTree(int order, int num_worker, SeqNode<T> *rootPtr): 
        // ORDER_(order), size_(0), rootPtr(SeqNode<T>(true, true)), 
        ORDER_(order), size_(0), rootPtr(rootPtr), 
        scheduler_(Scheduler(num_worker, rootPtr)) 
        {}

    template<typename T>
    void FreeBPlusTree<T>::get(T key) {
        Request request{FreeBPlusTree<T>::TreeOp::GET, key};
        scheduler_.submit_request(request);
    }

    template<typename T>
    void FreeBPlusTree<T>::insert(T key) {
        Request request{FreeBPlusTree<T>::TreeOp::INSERT, key};
        scheduler_.submit_request(request);
    }

    template<typename T>
    void FreeBPlusTree<T>::remove(T key) {
        Request request{FreeBPlusTree<T>::TreeOp::DELETE, key};
        scheduler_.submit_request(request);
    }
}
