#pragma once
#include "tree.h"
#include "scheduler.hpp"

namespace Tree {
    template <typename T>
    struct Scheduler<T>::PrivateWorker {


    static void *worker_loop(void* args) {
        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
        const int threadID = wargs->threadID;
        Scheduler *scheduler = wargs->scheduler;
        const int numWorker = scheduler->numWorker_;
        /**
         * CAUTION: The rootPtr is dynamic and subject to change (B+tree depth may increase)
         */
        SeqNode<T> *rootPtr = wargs->node;

        std::vector<Request> privateQueue;

        while (!isTerminate(scheduler->flag)) {
            // TODO: CHANGE rootPtr
            
            privateQueue.clear();
            {
                while (getStage(scheduler->flag) != PalmStage::SEARCH) {}
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    if (scheduler->curr_batch[i].op == TreeOp::NOP) {
                        continue;
                    }
                    privateQueue.push_back(scheduler->curr_batch[i]);
                }
                assert(privateQueue.size() <= BATCHSIZE / numWorker + 1);
                // Searching leaf node for each Request. All read operations so no lock required
                search(privateQueue, rootPtr, scheduler);
                scheduler->barrier_cnt ++;
                // while(scheduler->barrier_cnt < numWorker);
            }
            
            {
                while (getStage(scheduler->flag) != PalmStage::EXEC_LEAF) {}
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    leaf_execute(scheduler, scheduler->request_assign[i]);
                }
                scheduler->barrier_cnt ++;
                
            }

            // 
            while(scheduler->barrier_cnt < numWorker);
        }
        
        return nullptr;
    }

    /**
     * Search leaves in lock-free pattern since all threads are only reading at this time.
     */
    inline static void search(
        std::vector<Request> &privateQueue, SeqNode<T> *rootPtr, Scheduler *scheduler
    ) {
        for (Request &request : privateQueue) {
            SeqNode<T>* leafNode = lockFreeFindLeafNode(rootPtr, request.key.value());
            scheduler->curr_batch[request.idx].curr_node = leafNode;
        }
    }

    inline static void leaf_execute(Scheduler *scheduler, std::vector<Request> &requests_in_the_same_node) {
        for (Request &req : requests_in_the_same_node) {
            SeqNode<T> *leafNode = req.curr_node;
            assert(req.key.has_value());
            T key = req.key.value();
            auto it = std::lower_bound(leafNode->keys.begin(), leafNode->keys.end(), key);
            
            switch (req.op)
            {
            case TreeOp::INSERT:
                insertKeyToLeaf(leafNode, key);
                /**
                 * NOTE: If the leaf is full (numKeys() >= ORDER_), need to create a new Request
                 * for parent node to re-scale (TreeOp::UPDATE)
                 */
                if (leafNode->numKeys() >= scheduler->ORDER_) {
                    scheduler->internal_request_queue.push(
                        Request{TreeOp::UPDATE, std::nullopt, -1, leafNode->parent}
                    );
                }
                break;
            case TreeOp::GET:
                break;
            case TreeOp::DELETE:
                assert(false);  // TODO: Not implemented
                // if (it != leafNode->keys.end() && *it == key) {
                //     leafNode->keys.erase(it);
                // }
                // break;
            case TreeOp::NOP:
                assert(false);
            case TreeOp::UPDATE:
                assert(false);
            default:
                assert(false);
            }
        }
        
        if (requests_in_the_same_node.size() > 0) {
            DBG_PRINT(
                requests_in_the_same_node[0].curr_node -> printKeys();
            );
        }
    }

    inline static void internal_execute(std::vector<Request> &requests_in_the_same_node) {

    }

    static SeqNode<T>* lockFreeFindLeafNode(SeqNode<T>* node, T key) {
        while (!node->isLeaf) {
            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            node = node->children[index];
        }
        return node;
    }

    static void insertKeyToLeaf(SeqNode<T>* node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

};    
}
