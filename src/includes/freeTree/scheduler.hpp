#pragma once
#include "tree.h"
#include "freeNode.hpp"
#include "freeTree.hpp"
#include "utility/Sync.h"

namespace Tree {
    /**
     * NOTE:
     * Will spawn 1 background thread monitoring the Request queue
     *      spawn n worker threads executing the Request queue
     */
    template <typename T>
    Scheduler<T>::Scheduler(int numWorker, FreeNode<T> *rootPtr, int order):
            numWorker_(numWorker), rootPtr(rootPtr), ORDER_(order),
            syncBarrierA(numWorker + 1),
            syncBarrierB(numWorker + 1),
            request_queue(boost::lockfree::queue<Request>(QUEUE_SIZE)),
            internal_request_queue(boost::lockfree::queue<Request>(BATCHSIZE)),
            internal_release_queue(boost::lockfree::queue<FreeNode<T>*>(BATCHSIZE * numWorker * 4))
    {
        assert (numWorker_ < MAXWORKER);
        setStage(flag, PalmStage::COLLECT);

        workers_args[numWorker_].scheduler = this;
        workers_args[numWorker_].threadID = numWorker_;
        workers_args[numWorker_].node = rootPtr;
        pthread_create(&workers[numWorker_], NULL, PrivateBackground::background_loop, &workers_args[numWorker_]);

        // launch threads
        for (size_t idx = 0; idx < numWorker_; idx ++) {
            workers_args[idx].scheduler = this;
            workers_args[idx].threadID  = idx;
            workers_args[idx].node      = rootPtr;
            pthread_create(&workers[idx], NULL, PrivateWorker::worker_loop, &workers_args[idx]);
        }
    }

    template <typename T>
    void Scheduler<T>::waitToExit() {
        DBG_PRINT(std::cout << "Scheduler get Terminate signal, will exit after current batch" << std::endl);
        while (!request_queue.empty());
        PrivateBackground::release_nodes(this);

        setTerminate(flag);
        for (size_t i = 0; i < numWorker_ + 1; i ++) {
            pthread_join(workers[i], NULL);
        }
    }

    template <typename T>
    void Scheduler<T>::submit_request(Tree::Scheduler<T>::Request request) {
        /**
        * LOCK FREE REQUEST_QUEUE
        *
        * NOTE: the submid_request(...) API may be called by multiple threads
        * in the client, we use the while loop below to ensure that no conflict
        * write will occur on the request_queue.
        */
        while (!request_queue.bounded_push(request)) {};
    }

    template <typename T>
    inline bool Scheduler<T>::isTerminate(int &flag) {
        return flag & TERMINATE_FLAG;
    }

    template <typename T>
    inline PalmStage Scheduler<T>::getStage(int &flag) {
        return PalmStage(flag & (~TERMINATE_FLAG));
    }

    template <typename T>
    inline void Scheduler<T>::setTerminate(int &flag) {
        flag |= TERMINATE_FLAG;
    }

    template <typename T>
    inline void Scheduler<T>::setStage(int &flag, PalmStage stage) {
        flag = (flag & (TERMINATE_FLAG)) | stage;
    }

    template <typename T>
    void Scheduler<T>::debugPrint() {
        std::cout << "[Free B+ Tree]" << std::endl;
        if (rootPtr->numChild() == 0) {
            std::cout << "(Empty)" << std::endl;
            return;
        }
        FreeNode<T>* src = rootPtr;
        int level_cnt = 0;
        do {
            FreeNode<T>* ptr = src;
            std::cout << level_cnt << "\t| ";
            while (ptr != nullptr) {
                ptr->printKeys();
                std::cout << "<->";
                ptr = ptr->next;
            }
            level_cnt ++;
            std::cout << std::endl;
            if (src->numChild() == 0) break;
            src = src->children[0];
        } while (true);

        std::cout << std::endl;
    }
};
