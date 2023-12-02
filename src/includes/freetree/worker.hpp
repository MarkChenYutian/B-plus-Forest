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
        while (true) {
            // DBG_PRINT(std::cout << "W : bg_move: " << scheduler->bg_move << std::endl; 
            //           std::cout << "W : Stage: " << getStage(scheduler->flag) << std::endl;
            //           std::cout << "W : scheduler->worker_move[threadID]: " << scheduler->worker_move[threadID] << std::endl;);
            if (scheduler->bg_notify_worker_terminate) break;
            if (scheduler->bg_move || !scheduler->worker_move[threadID]) continue;
            
            PalmStage currentState = getStage(scheduler->flag);
            // DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);

            switch (currentState)
            {
            case PalmStage::SEARCH:
                // TODO: update root
                // DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                privateQueue.clear();
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    if (scheduler->curr_batch[i].op == TreeOp::NOP) {
                        continue;
                    }
                    privateQueue.push_back(scheduler->curr_batch[i]);
                }
                search(scheduler, privateQueue, rootPtr);
                break;
            
            case PalmStage::EXEC_LEAF:
                // DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    // DBG_PRINT(std::cout << "i: " << i << " | size: " << scheduler->request_assign[i].size() << std::endl;);
                    leaf_execute(scheduler, scheduler->request_assign[i]);
                }
                break;

            case PalmStage::EXEC_INTERNAL:
                // DBG_PRINT(std::cout << "Worker Stage: " << currentState << std::endl;);
                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                    internal_execute(scheduler, scheduler->request_assign[i]);
                }
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
        if (requests_in_the_same_node.empty()) return;

        SeqNode<T> *leafNode = requests_in_the_same_node[0].curr_node;
        for (Request &req : requests_in_the_same_node) {
            assert(req.key.has_value());
            assert(req.curr_node == leafNode);

            T key = req.key.value();
            auto it = std::lower_bound(leafNode->keys.begin(), leafNode->keys.end(), key);
            
            switch (req.op) {
            case TreeOp::INSERT:
                insertKeyToLeaf(leafNode, key);
                break;
            case TreeOp::GET:
                getFromLeaf(leafNode, key);
                break;
            case TreeOp::DELETE:
                removeFromLeaf(leafNode, key);
                break;
            default:
                // NOP, UPDATE should not occur in this stage!
                assert(false);
            }
        }

        /**
         * NOTE: If the leaf is full / less full (numKeys() >= ORDER_), 
         * need to create a new Request for parent node to re-scale (TreeOp::UPDATE)
         */
        if (leafNode->numKeys() >= scheduler->ORDER_ || leafNode->numKeys() < scheduler->ORDER_ / 2) {
            scheduler->internal_request_queue.push(
                Request{TreeOp::UPDATE, std::nullopt, -1, leafNode->parent}
            );
        }
        leafNode->updateMin();
    }

    inline static void internal_execute(Scheduler *scheduler, std::vector<Request> &requests_in_the_same_node) {
        if (requests_in_the_same_node.empty()) return;
        assert(requests_in_the_same_node.size() == 1);
        assert(requests_in_the_same_node[0].op == TreeOp::UPDATE);

        Request update_req = requests_in_the_same_node[0];
        SeqNode<T> *node = update_req.curr_node;
        node->updateMin();

        assert(node->children.size() >= 2);

        /**
         * NOTE: Since we are updating node->children iteratively (due to batch operation)
         * the node->children is subject to change. However, we make sure the start and end of children
         * does not change during all operations. So the boundary is valid.
         * 
         */
        for (SeqNode<T> *child = node->children[0]; child != node->children.back()->next; child=child->next) { 
            if (child->numKeys() >= scheduler->ORDER_)  {
                /**
                 * We would like to guarentee that the splitting operation is constrained in 
                 * the current subtree (with node as root).
                 */
                
                if (child->isLeaf) {
                    if (child->childIndex < child->numKeys()) bigSplitLeafToRight(child, scheduler->ORDER_);
                    else bigSplitLeafToLeft(child, scheduler->ORDER_);
                } else {
                    /**
                     * NOTE: Edge case - when we can split the child, we want to split them completely.
                     */
                    while (child->numKeys() >= scheduler->ORDER_) {
                        if (child->childIndex < child->numKeys()) bigSplitInternalToRight(scheduler->ORDER_, child);
                        else bigSplitInternalToLeft(scheduler->ORDER_, child);

                        consolidateChildren(node);
                    }
                }
            } else if (child->numKeys() < scheduler->ORDER_ / 2) {
                if (child->childIndex == 0) { // leftmost child
                    // try borrow from right
                    if (tryBorrow(scheduler->ORDER_, child, child->next, false)) continue;
                    // right merge to itself
                    merge(scheduler->ORDER_, child, child->next, false);
                } else if (child->childIndex < child->numKeys()) { // middle 
                    // try borrw from left
                    if (tryBorrow(scheduler->ORDER_, child->prev, child, true)) continue;
                    // try borrow from right
                    if (tryBorrow(scheduler->ORDER_, child, child->next, false)) continue;
                    // merge with right
                    merge(scheduler->ORDER_, child, child->next, true);
                } else { // rightmost child
                    // try borrow from left
                    if (tryBorrow(scheduler->ORDER_, child->prev, child, true)) continue;
                    // left merge to itself
                    merge(scheduler->ORDER_, child->prev, child, true);
                }
            } else {
                continue;
            }
            
            consolidateChildren(node);
        }

        // If current node is filled up, request further update on parent layer
        if (node->numKeys() >= scheduler->ORDER_ || node->numKeys() < scheduler->ORDER_ / 2) {
            scheduler->internal_request_queue.push(
                Request{TreeOp::UPDATE, std::nullopt, -1, node->parent}
            );
        }
    }

    static SeqNode<T>* lockFreeFindLeafNode(SeqNode<T>* node, T key) {
        while (!node->isLeaf) {
            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            node = node->children[index];
        }
        return node;
    }   
    
    static std::optional<T> getFromLeaf(SeqNode<T> *node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            return *it;
        }
        return std::nullopt;
    }

    // leaf_execute call insertKeyToLeaf
    static void insertKeyToLeaf(SeqNode<T> *node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

    // internal_execute call bigSplitLeafToRight
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
            newNode->updateMin();

            assert(newNode->keys.size() <= order - 1);
            cnt += newNode->keys.size();
            newNode->prev = prevNode;
            newNode->next = rightNode;
            prevNode->next = newNode;
            rightNode->prev = newNode;
            prevNode = newNode;
        }

        child->keys.erase(child->keys.begin() + vec[0].back(), child->keys.end());
        child->updateMin(); // Technically unnecessary, but still put it here just in case

        cnt += child->keys.size();
        assert(cnt == len);
    }

    // internal_execute call bigSplitLeafToLeft
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
            newNode->updateMin();

            assert(newNode->keys.size() <= order - 1);
            cnt += newNode->keys.size();
            newNode->prev = prevNode;
            newNode->next = rightNode;
            prevNode->next = newNode;
            rightNode->prev = newNode;
            prevNode = newNode;
        }

        child->keys.erase(child->keys.begin(), child->keys.begin() + vec[vec.size() - 1].front());
        child->updateMin();

        cnt += child->keys.size();
        assert(cnt == len);
    }

    // leaf_execute call removeFromLeaf
    static bool removeFromLeaf(SeqNode<T> *node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            node->updateMin();
            return true;
        }
        return false;
    }

    /**
     * @return true if the (right node if borrowFromLeft) | (left node if !borrowFromLeft) get
     * enough key to be at least half-full and do not need further modification.
     */
    static bool tryBorrow(int order, SeqNode<T> *left, SeqNode<T> *right, bool borrowFromLeft) {
        SeqNode<T>* parent = right->parent;
        size_t index = left->childIndex;

        if (borrowFromLeft) {
            /**
             * Case 1. Borrow from left node
             * Since execute in batch, may borrow multiple keys in one operation. Use
             * while loop
             *      left->numKeys() > order / 2     Left have excessive key
             *      right->numKeys() < order / 2    Right need borrow key
             */
            while (left->numKeys() > order / 2 && right->numKeys() < order / 2) {
                T keyParentMove = parent->keys[index],
                  keySiblingMove = left->keys.back();

                parent->keys[index] = keySiblingMove;
                right->keys.push_front(keyParentMove);
                left->keys.pop_back();

                assert(left->isLeaf == right->isLeaf);
                if (!right->isLeaf) {
                    /**
                     * Case 1a. Borrow from left where both are internal nodes
                     */
                    right->children.push_front(left->children.back());
                    left->children.pop_back();
                    right->consolidateChild();
                }
                right->updateMin();
            }
        } else {
            /**
             * Case 3. Left borrow from right.
             * Since execute in batch, may borrow multiple keys in one operation. Use
             * while loop
             *      right->numKeys() > order / 2     Right have excessive key
             *      left->numKeys() < order / 2      Left need borrow key
             */
            assert(left->isLeaf == right->isLeaf);

            if (left->isLeaf) {
                /**
                 * Case 3.b. Borrow from right where both are leaves
                 */
                while (right->numKeys() > order / 2 && left->numKeys() < order / 2) {
                    T keySiblingMove = right->keys.front();

                    left->keys.push_back(keySiblingMove);
                    right->keys.pop_front();
                    parent->keys[index] = right->keys.front();

                    right->updateMin();
                }
            } else {
                /**
                 * Case 3.a. Borrow from right where both are internal nodes
                 */
                while (right->numKeys() > order / 2 && left->numKeys() < order / 2) {
                    T keyParentMove = parent->keys[index],
                    keySiblingMove = right->keys.front();
                    
                    parent->keys[index] = keySiblingMove;
                    left->keys.push_back(keyParentMove);
                    right->keys.pop_front();
                    
                    left->children.push_back(right->children.front());
                    right->children.pop_front();
                    left->consolidateChild();
                    right->consolidateChild();

                    right->updateMin();
                }
            }
        }

        if (borrowFromLeft) {
            return right->numKeys() >= order / 2;
        } else {
            return left->numKeys() >= order / 2;
        }
    }

    /**
     * Just merge.
     */
    static void merge(int order, SeqNode<T> *left, SeqNode<T> *right, bool leftMergeToRight) {
        SeqNode<T> *parent = left->parent;
        size_t index = left->childIndex;

        if (leftMergeToRight) {
            if (!left->isLeaf) {
                /**
                 * Case 3.a Merge with right where both nodes are internal nodes
                 * */
                right->keys.push_front(parent->keys[index]);
                right->children.insert(
                    right->children.begin(), left->children.begin(), left->children.end()
                );
            } else {
                /**
                 * Case 3.b Merge with right where both are leaves, nothing to do here.
                 */
            }

            parent->keys.erase(parent->keys.begin() + index);
            parent->children.erase(parent->children.begin() + left->childIndex);

            right->keys.insert(right->keys.begin(), left->keys.begin(), left->keys.end());
            left->keys.clear();
            right->consolidateChild();

            /** Fix linked list */
            right->prev = left->prev;
            if (left->prev != nullptr) left->prev->next = right;

            right->updateMin();
            delete left;
        } else {
            /** Right merge to left */
            if (!right->isLeaf) {
                /**
                 * Case 1a. Merge with left where both are internal nodes
                 * 
                 * First, we want to find the key in parent that is larger then node->prev
                 * (the key in between of node -> prev and node)
                 * */
                left->keys.push_back(parent->keys[index]);
                left->children.insert(left->children.end(), right->children.begin(), right->children.end());
            } else {
                /** Case 1b. if are leaves, don't need to do operations above */
            }

            parent->keys.erase(parent->keys.begin() + index);
            parent->children.erase(parent->children.begin() + right->childIndex);

            left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
            right->keys.clear();

            left->consolidateChild();

            /** Fix linked list */
            left->next = right->next;
            if (right->next != nullptr) right->next->prev = left;

            left->updateMin();
            delete right;
        }
    }

    // internal_execute call bigSplitInternalToLeft
    static void bigSplitInternalToLeft(int order, SeqNode<T> *child) {
        assert (child->numChild() > order);
        SeqNode<T> *parent = child->parent,
                   *new_node = new SeqNode<T>(false);

        size_t numToSplitLeft;
        if ((child->numKeys() - order) >= (order / 2)) { 
            // Recursive case: splitted new left node gets exactly (order - 1) keys
            numToSplitLeft = order-1;
        } else {
            // Base Case: splitted new left node gets exactly (order / 2) keys
            numToSplitLeft = (order/2);
        }

        new_node->keys.insert(new_node->keys.begin(), child->keys.begin(), child->keys.begin()+numToSplitLeft);
        child->keys.erase(child->keys.begin(), child->keys.begin()+numToSplitLeft);

        new_node->children.insert(new_node->children.begin(), child->children.begin(), child->children.begin()+numToSplitLeft+1);
        child->children.erase(child->children.begin(), child->children.begin()+numToSplitLeft+1);

        /**
         * We can do this because consolidateChild will cover everything for us!!!
         * we always have minElem in node : )
         */
        child->keys.pop_front();

        /** Fix linked list */
        assert(child->prev != nullptr && child->prev->parent == child->parent);

        new_node->prev = child->prev;
        new_node->next = child;
        child->prev    = new_node;
        new_node->prev->next = new_node;

        new_node->consolidateChild();
        child->consolidateChild();
    }

    // internal_execute call bigSplitInternalToRight
    static void bigSplitInternalToRight(int order, SeqNode<T> *child) {
        assert (child->numChild() > order);

        SeqNode<T> *parent = child->parent,
                   *new_node = new SeqNode<T>(false);

        size_t numToSplitRight;
        if ((child->numKeys() - order) >= (order / 2)) { 
            // Recursive case: splitted new right node gets exactly (order - 1) keys
            numToSplitRight = order-1;
        } else {
            // Base Case: splitted new right node gets exactly (order / 2) keys
            numToSplitRight = (order/2);
        }

        new_node->keys.insert(new_node->keys.begin(), child->keys.end() - numToSplitRight, child->keys.end());
        child->keys.erase(child->keys.end() - numToSplitRight, child->keys.end());

        new_node->children.insert(new_node->children.begin(), child->children.end() - (numToSplitRight+1), child->children.end());
        child->children.erase(child->children.end() - (numToSplitRight+1),child->children.end());

        /**
         * We can do this because consolidateChild will cover everything for us!!!
         * we always have minElem in node : )
         */
        child->keys.pop_back();

        /** Fix linked list */
        assert(child->next != nullptr && child->parent == child->next->parent);
        
        new_node->prev = child;
        new_node->next = child->next;
        new_node->next->prev = new_node;
        child->next = new_node;
        
        child->consolidateChild();
        new_node->consolidateChild();
    }

    static void consolidateChildren(SeqNode<T> * node) {
        size_t childIdx = 0;
        SeqNode<T> *rightMost = node->children.back();
        node->keys.clear();
        node->children.clear();

        for (SeqNode<T> *ptr = node->children[0]; ptr != rightMost->next; ptr = ptr->next) {
            ptr->childIndex = childIdx;
            ptr->parent = node;
            ptr->updateMin();
            if (childIdx != 0) node->keys.push_back(ptr->minElem);
            node->children.push_back(ptr);
            childIdx++;
        }

        node->updateMin();
    }
};
}
