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
        while (!isTerminate(scheduler->flag) || getStage(scheduler->flag) != PalmStage::COLLECT) {
            // while (scheduler->barrier_cnt != 0 && !isTerminate(scheduler->flag)){}
            // while (scheduler->barrier_cnt != 0){}
            if (!scheduler->worker_move[threadID]) continue;
            while (scheduler->bg_move) {};
            
            PalmStage currentState = getStage(scheduler->flag);
            // DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);

            switch (currentState)
            {
            case PalmStage::SEARCH:
                // TODO: update root
                DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                privateQueue.clear();
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    if (scheduler->curr_batch[i].op == TreeOp::NOP) {
                        continue;
                    }
                    privateQueue.push_back(scheduler->curr_batch[i]);
                }
                search(scheduler, privateQueue, rootPtr);
                // scheduler->barrier_cnt ++; /////
                break;
            
            case PalmStage::EXEC_LEAF:
                DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    DBG_PRINT(std::cout << "i: " << i << " | size: " << scheduler->request_assign[i].size() << std::endl;);
                    leaf_execute(scheduler, scheduler->request_assign[i]);
                }
                // scheduler->barrier_cnt ++; /////
                break;

            case PalmStage::EXEC_INTERNAL:
                DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    internal_execute(scheduler, scheduler->request_assign[i]);
                }
                // scheduler->barrier_cnt ++; /////
                break;
            default:
                assert(false);
                break;
            }

            scheduler->worker_move[threadID] = false;
            scheduler->barrier_cnt ++;
            if (scheduler->barrier_cnt == numWorker) scheduler->bg_move = true;    
        }
        return nullptr;
    }

    /**
     * Search leaves in lock-free pattern since all threads are only reading at this time.
     */
    inline static void search(Scheduler *scheduler, std::vector<Request> &privateQueue, SeqNode<T> *rootPtr) {
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
                // TODO: Not implemented
                break;
            case TreeOp::DELETE:
                // TODO: Not implemented
                // if (it != leafNode->keys.end() && *it == key) {
                //     leafNode->keys.erase(it);
                // }
                break;
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

    inline static void internal_execute(Scheduler *scheduler, std::vector<Request> &requests_in_the_same_node) {
        if (requests_in_the_same_node.empty()) return;
        assert(requests_in_the_same_node.size() == 1);
        assert(requests_in_the_same_node[0].op == TreeOp::UPDATE);

        Request update_req = requests_in_the_same_node[0];
        SeqNode<T> *node = update_req.curr_node;

        assert(node->children.size() >= 2);
        for (SeqNode<T> *child : node->children) {
            if (child->numKeys() >= scheduler->ORDER_)  {
                /**
                 * We would like to guarentee that the splitting operation is constrained in 
                 * the current subtree (with node as root).
                 */
                if (child->childIndex < child->numKeys()) bigSplitLeafToRight(child, scheduler->ORDER_);
                else bigSplitLeafToLeft(child, scheduler->ORDER_);
            }
        }
        
        // consolidate children
        node->keys.clear();
        size_t childIdx = 0;
        SeqNode<T> *rightMost = node->children.back();
        for (SeqNode<T> *ptr = node->children[0]; ptr != rightMost->next; ptr = ptr->next) {
            ptr->childIndex = childIdx;
            ptr->parent = node;

            if (childIdx != 0) node->keys.push_back(ptr->keys[0]);

            childIdx ++;
        }

        // If current node is filled up, request further update on parent layer
        if (node->numKeys() < scheduler->ORDER_) return;
        scheduler->internal_request_queue.push(
            Request{TreeOp::UPDATE, std::nullopt, -1, node->parent}
        );
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

    static void bigSplitLeafToRight(SeqNode<T> *child, int order) {
        SeqNode<T> *rightNode = child->next;
        SeqNode<T> *prevNode = child;
        assert(child->next->parent == child->parent);
        assert(child->numKeys() >= order);
        size_t len = child->keys.size();
        size_t cnt = 0;
        
        /**
         * Since we are doing insert/delete in batch, a single node may split
         * into multiple nodes, we use this for loop to split nodes compeletely
         * (a.k.a. "Big Split" in PALM paper)
        */

        std::vector<std::deque<size_t>> vec;
        for (size_t i = 0; i < child->numKeys(); i += order - 1) {
            std::deque<size_t> tmp;
            for (size_t j = i; j < std::min(i + order - 1, child->numKeys()); j++) {
                tmp.push_back(j);
            }
            vec.push_back(tmp);
        }

        /**
         * Since the keys may have remainder < order/2, the last node splitted by the
         * for loop above may need to borrow keys / merge with other (i.e. having at
         * least as only 1 key). We want to manually balance the last node with -2 node.
         */
        while (vec[vec.size() - 1].size() < (order / 2)) {
            vec[vec.size() - 1].push_front(vec[vec.size() - 2].back());
            vec[vec.size() - 2].pop_back();
        }

        /**
         * Use the index map built above (vec), rebuild the linked list.
         */
        for (size_t i = 1; i < vec.size(); i ++) {
            SeqNode<T> *newNode = new SeqNode<T>(true);
            for (size_t idx : vec[i]) {
                newNode->keys.push_back(child->keys[idx]);
            }
            assert(newNode->keys.size() <= order - 1);
            cnt += newNode->keys.size();
            newNode->prev = prevNode;
            newNode->next = rightNode;
            prevNode->next = newNode;
            rightNode->prev = newNode;
            prevNode = newNode;
        }

        child->keys.erase(child->keys.begin() + vec[0].back(), child->keys.end());
        cnt += child->keys.size();
        assert(cnt == len);
    }

    static void bigSplitLeafToLeft(SeqNode<T> *child, int order) {
        SeqNode<T> *prevNode = child->prev;
        SeqNode<T> *rightNode = child;
        assert(prevNode->parent == child->parent);
        assert(child->numKeys() >= order);
        size_t len = child->keys.size();
        size_t cnt = 0;

        std::vector<std::deque<size_t>> vec;
        for (size_t i = 0; i < child->numKeys(); i += order - 1) {
            std::deque<size_t> tmp;
            for (size_t j = i; j < std::min(i + order - 1, child->numKeys()); j++) {
                tmp.push_back(j);
            }
            vec.push_back(tmp);
        }
        
        while (vec[vec.size() - 1].size() < (order / 2)) {
            vec[vec.size() - 1].push_front(vec[vec.size() - 2].back());
            vec[vec.size() - 2].pop_back();
        }
        
        
        for (size_t i = 0; i < vec.size() - 1; i ++) {
            SeqNode<T> *newNode = new SeqNode<T>(true);
            for (size_t idx : vec[i]) {
                newNode->keys.push_back(child->keys[idx]);
            }
            assert(newNode->keys.size() <= order - 1);
            cnt += newNode->keys.size();
            newNode->prev = prevNode;
            newNode->next = rightNode;
            prevNode->next = newNode;
            rightNode->prev = newNode;
            prevNode = newNode;
        }
        child->keys.erase(child->keys.begin(), child->keys.begin() + vec[vec.size() - 1].front());
        cnt += child->keys.size();
        assert(cnt == len);
    }


};    
}
