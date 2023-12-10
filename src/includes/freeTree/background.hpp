#pragma once
#include <set>
#include "tree.h"
#include "freeNode.hpp"
#include "scheduler.hpp"

namespace Tree {
    template <typename T>
    struct Scheduler<T>::PrivateBackground {
    

    static void *background_loop(void *args) {
        /**
         * This map is used by background thread to distribute requests to each leaf node in freeTree
         */
        std::unordered_map<FreeNode<T> *, std::vector<uint32_t>> assign_node_to_thread;

        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
        const int threadID = wargs->threadID;
        Scheduler *scheduler = wargs->scheduler;
        FreeNode<T> *rootPtr = wargs->node;
        const int numWorker = scheduler->numWorker_;

        PalmStage nextStage = PalmStage::COLLECT;

        while (getStage(scheduler->flag) != PalmStage::COLLECT ||!isTerminate(scheduler->flag)) {
            setStage(scheduler->flag, nextStage);

            scheduler->syncBarrierA.wait();
            PalmStage currentState = getStage(scheduler->flag);

            Timer timer;
            size_t request_idx;
            Request req = {TreeOp::NOP};
            bool isRootUpdate;

            switch (currentState)
            {
            case PalmStage::COLLECT:
                // DBG_PRINT(std::cout << "BG: COLLECT" << std::endl;);
                request_idx = 0;
                timer = Timer();
                FreeNode<T> *node;
                while (request_idx < BATCHSIZE) {
                    req = {TreeOp::NOP};
                    while (!scheduler->request_queue.pop(req) && timer.elapsed() < COLLECT_TIMEOUT){
                        if (scheduler->internal_release_queue.pop(node)) delete node;
                    };
                    req.idx = request_idx;
                    scheduler->curr_batch[request_idx++] = req;
                }
                
                nextStage = PalmStage::SEARCH;
                break;
            
            case PalmStage::SEARCH:
                // DBG_PRINT(std::cout << "BG: SEARCH" << std::endl);
                nextStage = PalmStage::DISTRIBUTE;
                break;
            
            case PalmStage::DISTRIBUTE:
                // DBG_PRINT(std::cout << "BG: DISTRIBUTE" << std::endl;);
                assign_node_to_thread.clear();
                distribute(scheduler, assign_node_to_thread);
                nextStage = PalmStage::EXEC_LEAF;
                break;
            
            case PalmStage::EXEC_LEAF:
                nextStage = PalmStage::REDISTRIBUTE;
                break;
            
            case PalmStage::REDISTRIBUTE:
                // DBG_PRINT(std::cout << "BG: REDISTRIBUTE" << std::endl;);
                assign_node_to_thread.clear();
                isRootUpdate = redistribute(scheduler, assign_node_to_thread);

                DBG_ASSERT(scheduler->internal_request_queue.empty());
                if (assign_node_to_thread.empty()) {
                    // Case 1: worker finds that none of their parents need update
                    // Case 2: background done dealing root
                    nextStage = PalmStage::COLLECT;
                } else if (isRootUpdate) {
                    DBG_ASSERT(assign_node_to_thread.size() == 1);
                    nextStage = PalmStage::EXEC_ROOT;
                } else {
                    // Internal update, done by workers
                    nextStage = PalmStage::EXEC_INTERNAL;
                    /**
                     * NOTE: We put release nodes here since we can run in parallel with worker
                     * to maximize the throughput of the entire data structure.
                     * 
                     * (! notice that the worker should be at least one-level above the nodes in
                     * release queue, and non of the tree node has pointer pointing to the to-be-removed
                     * nodes, so this will not cause data racing.)
                     */
                    release_nodes(scheduler);
                }
                break;
            
            case PalmStage::EXEC_INTERNAL:
                // DBG_PRINT(std::cout << "BG: EXEC_INTERNAL" << std::endl;);
                nextStage = PalmStage::REDISTRIBUTE;
                break;
            
            case PalmStage::EXEC_ROOT:
                // DBG_PRINT(std::cout << "BG: EXEC_ROOT" << std::endl;);
                root_execute(scheduler, scheduler->request_assign[0]);
                nextStage = PalmStage::COLLECT;
                break;

            default:
                break;
            }
            scheduler->syncBarrierB.wait();
        }
        scheduler->bg_notify_worker_terminate = true;
        scheduler->syncBarrierA.wait();
        return nullptr;
    }
    

    static void distribute(
        Scheduler *scheduler, 
        std::unordered_map<FreeNode<T> *, std::vector<uint32_t>> &assign_node_to_thread
    ) {
        for (uint32_t i = 0; i < BATCHSIZE; i++) {
            Request req = scheduler->curr_batch[i];
            if (req.op == TreeOp::NOP) continue;

            FreeNode<T> *node = req.curr_node;
            DBG_ASSERT(node != nullptr);
            assign_node_to_thread[node].push_back(i);
            scheduler->request_assign_all[i] = req;
        }
        DBG_ASSERT(assign_node_to_thread.size() <= BATCHSIZE);

        SIMDOptimizer<T>::processAssignments(assign_node_to_thread, scheduler, BATCHSIZE);
        size_t widx = assign_node_to_thread.size();

        // For remaining slots, clear them up
        while (widx < BATCHSIZE) {
            scheduler->request_assign_len[widx] = 0;
            widx++;
        }
    }

    static bool redistribute(
        Scheduler *scheduler,
        std::unordered_map<FreeNode<T> *, std::vector<uint32_t>> &assign_node_to_thread
    ) {
        Request update_req;
        std::set<FreeNode<T>*> parent_update_min;
        size_t i = 0;
        while (scheduler->internal_request_queue.pop(update_req)) {
            if (assign_node_to_thread.find(update_req.curr_node) != assign_node_to_thread.end()) {
                continue;
            }
            assign_node_to_thread[update_req.curr_node].push_back(i);
            scheduler->request_assign_all[i++] = update_req;
        }

        DBG_ASSERT(assign_node_to_thread.size() <= BATCHSIZE);

        SIMDOptimizer<T>::processAssignments(assign_node_to_thread, scheduler, BATCHSIZE);
        size_t widx = assign_node_to_thread.size();

        // For remaining slots, clear them up
        while (widx < BATCHSIZE) {
            scheduler->request_assign_len[widx] = 0;
            widx ++;
        }

        if (assign_node_to_thread.size() == 1) {
            Request req = scheduler->request_assign_all[scheduler->request_assign[0][0]];
            if (req.curr_node == scheduler->rootPtr) return true;
        }
        return false;
    }
    
    static void root_execute(Scheduler *scheduler, uint32_t (&requests_in_the_same_node)[BATCHSIZE]) {
        Request rootUpdateRequest = scheduler->request_assign_all[requests_in_the_same_node[0]];
        int order = scheduler->ORDER_;

        DBG_ASSERT(rootUpdateRequest.curr_node == scheduler->rootPtr);
        FreeNode<T> *root_node = rootUpdateRequest.curr_node->children[0];
        
        if (root_node->numKeys() == 0) {
            while (root_node->numKeys() == 0) {
                // DBG_PRINT(std::cout << "空了！啊？？？？\n";);
                if (root_node->isLeaf) {
                    scheduler->rootPtr->children.clear();
                    scheduler->rootPtr->isLeaf = true;
                }
                if (root_node->parent == scheduler->rootPtr) {
                    // DBG_PRINT(std::cout << "啊？？？彻底空了，漏点内存也没事吧……\n";);
                    scheduler->rootPtr->isLeaf = true;
                    scheduler->rootPtr->children.clear();
                    break;
                }
                DBG_ASSERT(root_node->children.size() == 1);
                FreeNode<T> *new_root_node = root_node->children[0];
                scheduler->rootPtr->children[0] = new_root_node;
                scheduler->rootPtr->consolidateChild();
                delete root_node;
            }
        } else if (root_node->numKeys() >= order) {
            while (root_node->numKeys() >= order) {
                DBG_ASSERT(root_node->isLeaf || root_node->numKeys() == root_node->numChild() - 1);

                // DBG_PRINT(std::cout << "啊？还要split几次？？？？\n";);

                FreeNode<T> *new_root_node = new FreeNode<T>(false);
                scheduler->rootPtr->children[0] = new_root_node;
                scheduler->rootPtr->consolidateChild();

                /** Insert a new layer */
                new_root_node->children.push_back(root_node);
                root_node->parent = new_root_node;
                root_node->childIndex = 0;

                while (root_node->numKeys() >= order) {
                    PrivateWorker::bigSplitToRight(order, root_node, false);
                    // PrivateWorker::rebuildChildren(new_root_node, nullptr);
                    new_root_node->consolidateChild();
                }
                root_node = new_root_node;
            }
        }
    }

    static void release_nodes(Scheduler *scheduler) {
        FreeNode<T> *node;
        while (scheduler->internal_release_queue.pop(node)) {
            delete node;
        }
    }
    
    };
}
