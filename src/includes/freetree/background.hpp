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

        while (!isTerminate(scheduler->flag) || getStage(scheduler->flag) != PalmStage::COLLECT) {
            if (!scheduler->bg_move) continue;
            PalmStage currentState = getStage(scheduler->flag);
            
            Timer timer;
            size_t request_idx;
            Request req = {TreeOp::NOP};

            switch (currentState)
            {
            case PalmStage::COLLECT:
                request_idx = 0;
                timer = Timer();
                while (request_idx < BATCHSIZE) {
                    req = {TreeOp::NOP};
                    while (!scheduler->request_queue.pop(req) && timer.elapsed() < COLLECT_TIMEOUT){};
                    req.idx = request_idx;
                    scheduler->curr_batch[request_idx++] = req;
                }
                
                scheduler->barrier_cnt = 0;
                scheduler->bg_move = false;
                setStage(scheduler->flag, PalmStage::SEARCH);
                for (size_t i = 0; i < numWorker; i++) {
                    scheduler->worker_move[i] = true;
                }
                break;
            
            case PalmStage::SEARCH:
                setStage(scheduler->flag, PalmStage::DISTRIBUTE);
                break;
            
            case PalmStage::DISTRIBUTE:
                assign_node_to_thread.clear();
                distribute(scheduler, assign_node_to_thread);

                
                scheduler->barrier_cnt = 0;
                scheduler->bg_move = false; 
                setStage(scheduler->flag, PalmStage::EXEC_LEAF);
                for (size_t i = 0; i < numWorker; i++) {
                    scheduler->worker_move[i] = true;
                }
                break;
            
            case PalmStage::EXEC_LEAF:
                setStage(scheduler->flag, PalmStage::REDISTRIBUTE);
                break;
            
            case PalmStage::REDISTRIBUTE:
                assign_node_to_thread.clear();
                redistribute(scheduler, assign_node_to_thread);
                assert(scheduler->internal_request_queue.empty());
                if (assign_node_to_thread.size() == 0) {
                    setStage(scheduler->flag, PalmStage::COLLECT);
                } else if (assign_node_to_thread.size() == 1) {
                    setStage(scheduler->flag, PalmStage::EXEC_ROOT);
                } else {
                    
                    scheduler->barrier_cnt = 0;
                    scheduler->bg_move = false;
                    setStage(scheduler->flag, PalmStage::EXEC_INTERNAL);
                    for (size_t i = 0; i < numWorker; i++) {
                        scheduler->worker_move[i] = true;
                    }
                    
                }
                break;
            
            case PalmStage::EXEC_INTERNAL:
                setStage(scheduler->flag, PalmStage::REDISTRIBUTE);
                break;
            
            case PalmStage::EXEC_ROOT:
                // TODO: Implement execute root.
                setStage(scheduler->flag, PalmStage::COLLECT);
                break;

            default:
                break;
            }
        }
        scheduler->bg_notify_worker_terminate = true;
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

    inline static void redistribute(
        Scheduler *scheduler,
        std::unordered_map<SeqNode<T> *, std::vector<Request>> &assign_node_to_thread
    ) {
        Request update_req;
        while (scheduler->internal_request_queue.pop(update_req)) {
            if (assign_node_to_thread.find(update_req.curr_node) != assign_node_to_thread.end()) continue;
            assign_node_to_thread[update_req.curr_node].push_back(update_req);
        }

        assert(assign_node_to_thread.size() < BATCHSIZE);

        size_t idx = 0;
        // Fill the slots with Request (task) queue for updating.
        for (auto elem : assign_node_to_thread) {
            assert(elem.second.size() == 1);
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
