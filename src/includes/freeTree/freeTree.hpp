#pragma once
#include "../tree.h"
#include "timing.h"
#include "freeTree/scheduler.hpp"
#include "freeTree/background.hpp"
#include "freeTree/worker.hpp"


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
 * 
 * Using lockfree queue from Boost lilbrary
 *  https://www.boost.org/doc/libs/1_76_0/doc/html/boost/lockfree/queue.html
 * 
 * Potential source of Data Racing reported by Thread Sanitizer caused by lockfree queue:
 *  https://github.com/boostorg/lockfree/issues/78
 */


///////////////////////////////

namespace Tree {
    template <typename T>
    class Scheduler;

    template <typename T>
    class FreeBPlusTree {
        public:
            /**
             * NOTE: These are all async APIs since the lock-free B+ tree
             * will execute all the requests in an asynchronous batch operation
             */
            explicit FreeBPlusTree(int order, int numWorker=2):
                ORDER_(order), size_(0), rootPtr(FreeNode<T>(true))
            {
                scheduler_ = new Scheduler(numWorker, &rootPtr, order);
            }


            ~FreeBPlusTree() {
                scheduler_->waitToExit();
                delete scheduler_;
                
                DBG_PRINT(std::cout << "Really Exited" << std::endl;);

                #ifdef DEBUG
                if (!rootPtr.isLeaf) {
                    assert(rootPtr.children[0]->debug_checkChildCnt(ORDER_, true));
                    assert(rootPtr.children[0]->debug_checkOrdering(std::nullopt, std::nullopt));
                    assert(rootPtr.children[0]->debug_checkParentPointers());
                }
                #endif

                if (!rootPtr.isLeaf) rootPtr.children[0]->releaseAll();
            }

            void insert(T key) {
                scheduler_->submit_request({Scheduler<T>::TreeOp::INSERT, key});
            }

            void remove(T key) {
                scheduler_->submit_request({Scheduler<T>::TreeOp::DELETE, key});
            }

            void get(T key) {
                scheduler_->submit_request({Scheduler<T>::TreeOp::GET, key});
            }

        private:
            Scheduler<T> *scheduler_;
            FreeNode<T> rootPtr;
            int ORDER_;
            int size_;
        };            
}
