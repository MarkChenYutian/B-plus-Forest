#pragma once
#include "tree.h"
#include "scheduler.hpp"

namespace Tree {
    template <typename T>
    struct Scheduler<T>::PrivateBackground {
    

    static void *background_loop(void *args) {
        /**
         * This map is used by background thread to distribute requests to each leaf node in freeTree
         */
        std::unordered_map<SeqNode<T> *, std::vector<Request>> assign_node_to_thread;

        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
        const int threadID = wargs->threadID;
        Scheduler *scheduler = wargs->scheduler;
        SeqNode<T> *rootPtr = wargs->node;
        const int numWorker = scheduler->numWorker_;

        while (!isTerminate(scheduler->flag)) {
            {
                setStage(scheduler->flag, PalmStage::COLLECT);

                Timer timer = Timer();
                size_t request_idx = 0;
                while (request_idx < BATCHSIZE) {
                    Request req = {TreeOp::NOP};
                    while (!scheduler->request_queue.pop(req) && timer.elapsed() < COLLECT_TIMEOUT){};
                    req.idx = request_idx;
                    scheduler->curr_batch[request_idx++] = req;
                }
            }

            {
                scheduler->barrier_cnt = 0;
                setStage(scheduler->flag, PalmStage::SEARCH);
                while (scheduler->barrier_cnt < numWorker) {}

                setStage(scheduler->flag, PalmStage::DISTRIBUTE);
                assign_node_to_thread.clear();
                distribute(scheduler, assign_node_to_thread);

                /**
                 * NOTE: Since distribute has distribute all tasks from curr_batch
                 * to the request_assign array, we can now clear up the curr_batch
                 * array to store pending requests (of update) for internal nodes
                */
                
            }
            
            {
                scheduler->barrier_cnt = 0;
                setStage(scheduler->flag, PalmStage::EXEC_LEAF);
                while(scheduler->barrier_cnt < numWorker) {}
            }
        }
        return nullptr;
    }

    inline static void distribute(
        Scheduler *scheduler, 
        std::unordered_map<SeqNode<T> *, std::vector<Request>> &assign_node_to_thread
    ) {
        for (size_t i = 0; i < BATCHSIZE; i++) {
            Request req = scheduler->curr_batch[i];
            if (req.op == TreeOp::NOP) continue;

            SeqNode<T> *node = req.curr_node;
            assign_node_to_thread[node].push_back(req);
        }
        
        assert(assign_node_to_thread.size() < BATCHSIZE);
        
        size_t idx = 0;
        // Fill the slots with Request (task) queue
        for (auto elem : assign_node_to_thread) {
            scheduler->request_assign[idx] = elem.second;
            idx ++;
        }
        // For remaining slots, clear them up
        while (idx < BATCHSIZE) {
            scheduler->request_assign[idx].clear();
            idx ++;
        }
    }
    };
}
