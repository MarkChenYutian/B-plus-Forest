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
            bool isRootUpdate;

            switch (currentState)
            {
            case PalmStage::COLLECT:
                DBG_PRINT(std::cout << "BG: COLLECT" << std::endl;);
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

                for (size_t i = 0; i < numWorker; i++) scheduler->worker_move[i] = true;
                break;
            
            case PalmStage::SEARCH:
                DBG_PRINT(std::cout << "BG: SEARCH" << std::endl);
                setStage(scheduler->flag, PalmStage::DISTRIBUTE);
                break;
            
            case PalmStage::DISTRIBUTE:
                DBG_PRINT(std::cout << "BG: DISTRIBUTE" << std::endl;);
                assign_node_to_thread.clear();
                distribute(scheduler, assign_node_to_thread);


                DBG_PRINT(
                std::cout << "BG | DISTRIBUTE : assign_node_to_thread" << std::endl;
                for (int i = 0; i < BATCHSIZE; i ++) {
                    for (Request &r : scheduler->request_assign[i]) {
                        r.print();
                        std::cout << std::endl;
                    }
                }
                std::cout << std::endl;
                );
                
                scheduler->barrier_cnt = 0;
                scheduler->bg_move = false; 
                setStage(scheduler->flag, PalmStage::EXEC_LEAF);
                for (size_t i = 0; i < numWorker; i++) {
                    scheduler->worker_move[i] = true;
                }
                break;
            
            case PalmStage::EXEC_LEAF:
                DBG_PRINT(std::cout << "BG: EXEC_LEAF" << std::endl;);
                setStage(scheduler->flag, PalmStage::REDISTRIBUTE);
                break;
            
            case PalmStage::REDISTRIBUTE:
                DBG_PRINT(std::cout << "BG: REDISTRIBUTE" << std::endl;);
                assign_node_to_thread.clear();
                isRootUpdate = redistribute(scheduler, assign_node_to_thread);

                // DBG_PRINT(
                //     std::cout << "BG";
                //     scheduler->rootPtr->printKeys();
                //     scheduler->rootPtr->children[0]->printKeys();
                //     std::cout << std::endl;  
                // );
                DBG_PRINT(
                std::cout << "BG | REDISTRIBUTE : assign_node_to_thread" << std::endl;
                for (int i = 0; i < BATCHSIZE; i ++) {
                    for (Request &r : scheduler->request_assign[i]) {
                        r.print();
                        std::cout << std::endl;
                    }
                }
                std::cout << std::endl;
                );

                assert(scheduler->internal_request_queue.empty());
                if (assign_node_to_thread.size() == 0) {
                    // Case 1: worker finds that none of their parents need update
                    // Case 2: background done dealing root
                    DBG_PRINT(std::cout << "BG: assign_node_to_thread.size() == 0" << std::endl;);
                    setStage(scheduler->flag, PalmStage::COLLECT);
                } else if (isRootUpdate) {
                    assert(assign_node_to_thread.size() == 1);
                    setStage(scheduler->flag, PalmStage::EXEC_ROOT);
                } else {
                    // Internal update, done by workers
                    scheduler->barrier_cnt = 0;
                    scheduler->bg_move = false;
                    setStage(scheduler->flag, PalmStage::EXEC_INTERNAL);
                    for (size_t i = 0; i < numWorker; i++) {
                        scheduler->worker_move[i] = true;
                    }
                }
                break;
            
            case PalmStage::EXEC_INTERNAL:
                DBG_PRINT(std::cout << "BG: EXEC_INTERNAL" << std::endl;);
                setStage(scheduler->flag, PalmStage::REDISTRIBUTE);
                break;
            
            case PalmStage::EXEC_ROOT:
                DBG_PRINT(std::cout << "BG: EXEC_ROOT" << std::endl;);
                root_execute(scheduler, scheduler->request_assign[0]);
                setStage(scheduler->flag, PalmStage::COLLECT);
                break;

            default:
                break;
            }
        }
        scheduler->bg_notify_worker_terminate = true;
        return nullptr;
    }

    static void distribute(
        Scheduler *scheduler, 
        std::unordered_map<SeqNode<T> *, std::vector<Request>> &assign_node_to_thread
    ) {
        for (size_t i = 0; i < BATCHSIZE; i++) {
            Request req = scheduler->curr_batch[i];
            if (req.op == TreeOp::NOP) continue;

            SeqNode<T> *node = req.curr_node;
            assign_node_to_thread[node].push_back(req);
        }
        DBG_PRINT(std::cout << "assign_node_to_thread.size() = " << assign_node_to_thread.size() << " | BATCHSIZE : " << BATCHSIZE << std::endl;);
        assert(assign_node_to_thread.size() <= BATCHSIZE);
        
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

    static bool redistribute(
        Scheduler *scheduler,
        std::unordered_map<SeqNode<T> *, std::vector<Request>> &assign_node_to_thread
    ) {
        Request update_req;
        while (scheduler->internal_request_queue.pop(update_req)) {
            if (assign_node_to_thread.find(update_req.curr_node) != assign_node_to_thread.end()) continue;
            assign_node_to_thread[update_req.curr_node].push_back(update_req);
        }

        assert(assign_node_to_thread.size() <= BATCHSIZE);

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

        if (assign_node_to_thread.size() == 1) {
            Request req = scheduler->request_assign[0][0];
            DBG_PRINT(std::cout << "BG | redistribute | assign_node_to_thread.size() == 1\n";);
            if (req.curr_node == scheduler->rootPtr) return true;
        }
        return false;
    }
    
    static void root_execute(Scheduler *scheduler, std::vector<Request> &requests_in_the_same_node) {
        // assert(requests_in_thesam
        Request rootUpdateRequest = requests_in_the_same_node[0];
        int order = scheduler->ORDER_;

        assert(rootUpdateRequest.curr_node == scheduler->rootPtr);
        SeqNode<T> *root_node = rootUpdateRequest.curr_node->children[0];

        if (root_node->numKeys() == 0) {
            while (root_node->numKeys() == 0) {
                DBG_PRINT(std::cout << "空了！啊？？？？\n";);
                if (root_node->isLeaf) {
                    scheduler->rootPtr->children.clear();
                    scheduler->rootPtr->isLeaf = true;
                }
                assert(root_node->children.size() == 1);
                root_node = root_node->children[0];
                scheduler->rootPtr->children[0] = root_node;
                scheduler->rootPtr->consolidateChild();
            }
        } else if (root_node->numKeys() >= order) {
            while (root_node->numKeys() >= order) {
                assert(root_node->isLeaf || root_node->numKeys() == root_node->numChild() - 1);

                DBG_PRINT(std::cout << "啊？还要split几次？？？？\n";);

                SeqNode<T> *new_root_node = new SeqNode<T>(false);
                scheduler->rootPtr->children[0] = new_root_node;
                scheduler->rootPtr->consolidateChild();

                /** Insert a new layer */
                new_root_node->children.push_back(root_node);
                root_node->parent = new_root_node;
                root_node->childIndex = 0;

                while (root_node->numKeys() >= order) {
                    PrivateWorker::bigSplitToRight(order, root_node);
                    PrivateWorker::rebuildChildren(new_root_node, nullptr);
                }

                root_node = new_root_node;
            }
        }
    }

    };
}
