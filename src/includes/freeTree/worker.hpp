#pragma once
#include "tree.h"
#include "scheduler.hpp"


namespace Tree {
    template <typename T>
    struct Scheduler<T> ::PrivateWorker {

    static inline bool isHalfFull(FreeNode<T> *node, int order) {
        return node->numKeys() >= ((order - 1) / 2);
    }

    static inline bool moreHalfFull(FreeNode<T> *node, int order) {
        return node->numKeys() > ((order - 1) / 2);
    }

    static void *worker_loop(void* args) {
        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
        const int threadID = wargs->threadID;
        Scheduler *scheduler = wargs->scheduler;
        const int numWorker = scheduler->numWorker_;
        /**
         * CAUTION: The rootPtr is dynamic and subject to change (B+tree depth may increase)
         */
        FreeNode<T> *rootPtr = wargs->node;
        std::vector<Request> privateQueue;
        // Request privateQueue[BATCHSIZE];
        while (true) {
            scheduler->syncBarrierA.wait();
            if (scheduler->bg_notify_worker_terminate) break;

            PalmStage currentState = getStage(scheduler->flag);
            switch (currentState)
            {
                case PalmStage::SEARCH:
                    privateQueue.clear();
                    for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                        DBG_ASSERT(scheduler->curr_batch[i].op != TreeOp::UPDATE);
                        if (scheduler->curr_batch[i].op == TreeOp::NOP) continue;
                        privateQueue.push_back(scheduler->curr_batch[i]);
                    }
                    search(scheduler, privateQueue, rootPtr);
                    break;

                case PalmStage::EXEC_LEAF:
                    for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                        leaf_execute(scheduler, scheduler->request_assign[i], i);
                    }
                    break;

                case PalmStage::EXEC_INTERNAL:
                    for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                        internal_execute(scheduler, scheduler->request_assign[i], i);
                    }
                    break;
                default:
                    break;
            }
            scheduler->syncBarrierB.wait();
        }
        return nullptr;
    }

    /**
     * Search leaves in lock-free pattern since all threads are only reading at this time.
     */
    inline static void search(Scheduler *scheduler, std::vector<Request> &privateQueue, FreeNode<T> *rootPtr) {
        for (Request &request : privateQueue) {
            FreeNode<T>* leafNode = lockFreeFindLeafNode(rootPtr, request.key.value());
            DBG_ASSERT(leafNode != nullptr);
            scheduler->curr_batch[request.idx].curr_node = leafNode;
        }
    }

    inline static void leaf_execute(Scheduler *scheduler, uint32_t (&requests_in_the_same_node)[BATCHSIZE], int slot_idx) {
    // inline static void leaf_execute(Scheduler *scheduler, Request (&requests_in_the_same_node)[BATCHSIZE], int slot_idx) {
        int order = scheduler->ORDER_;
        size_t numRequest = scheduler->request_assign_len[slot_idx];

        if (numRequest == 0) return;
        // leafNode could be root_node, or rootPtr
        FreeNode<T> *leafNode = scheduler->request_assign_all[requests_in_the_same_node[0]].curr_node;
        
        /**
         * NOTE: Special case: the tree is originally empty, and we are insert the first few
         * elements of the tree. In this case there is only one worker since all the leaf nodes
         * are the rootPtr.
         */
        bool doCheck = true;
        if (leafNode == scheduler->rootPtr) {
            doCheck = false;
            leafNode = new FreeNode<T>(true);
            scheduler->rootPtr->children.push_back(leafNode);
            scheduler->rootPtr->consolidateChild();
            scheduler->rootPtr->isLeaf = false;
        }

        
        // for (Request &req : requests_in_the_same_node) {
        for (int i = 0; i < numRequest; i ++) {
            // Request req = std::move(requests_in_the_same_node[i]);
            // Request req = requests_in_the_same_node[i];
            Request req = scheduler->request_assign_all[requests_in_the_same_node[i]];

            DBG_ASSERT(req.key.has_value());
            DBG_ASSERT(req.op == TreeOp::DELETE || req.op == TreeOp::GET || req.op == TreeOp::INSERT);
            DBG_ASSERT(!doCheck || req.curr_node == leafNode);

            T key = req.key.value();
            if (leafNode == nullptr) {
                DBG_PRINT(scheduler->debugPrint(););
                DBG_ASSERT(false);
            }
            
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
                DBG_ASSERT(false);
            }
        }

        /**
         * NOTE: If the leaf is full / less full (numKeys() >= ORDER_), 
         * need to create a new Request for parent node to re-scale (TreeOp::UPDATE)
         */
        if (leafNode->numKeys() >= order || !isHalfFull(leafNode, order)) {
            scheduler->internal_request_queue.push(
                Request{TreeOp::UPDATE, std::nullopt, -1, leafNode->parent}
            );
        }
    }

    inline static void internal_execute(Scheduler *scheduler, uint32_t (&requests_in_the_same_node)[BATCHSIZE], int slot_idx) {
    // inline static void internal_execute(Scheduler *scheduler, Request (&requests_in_the_same_node)[BATCHSIZE], int slot_idx) {
        // if (requests_in_the_same_node.empty()) return;
        if (scheduler->request_assign_len[slot_idx] == 0) return;
        
        // DBG_ASSERT(requests_in_the_same_node.size() == 1);
        // DBG_ASSERT(requests_in_the_same_node[0].op == TreeOp::UPDATE);
        DBG_ASSERT(scheduler->request_assign_len[slot_idx] == 1);

        // Request update_req = requests_in_the_same_node[0];
        Request update_req = scheduler->request_assign_all[requests_in_the_same_node[0]];
        FreeNode<T> *node = update_req.curr_node;

        // assert(node->children.size() >= 2);
        if (node->children.size() < 2) {
            DBG_PRINT(scheduler->debugPrint());
            DBG_PRINT(node->printKeys(););
            DBG_ASSERT(false);
        }
        /**
         * NOTE: Since we are updating node->children iteratively (due to batch operation)
         * the node->children is subject to change. However, we make sure the start and end of children
         * does not change during all operations. So the boundary is valid.
         * 
         */
        FreeNode<T> *next_child;
        FreeNode<T> *child = node->children[0];
        int child_num = node->numChild();
        int curr = 0;
        while (curr++ < child_num) {
            FreeNode<T> *rightmost = node->children.back();
            if (child->numKeys() >= scheduler->ORDER_)  {
                /**
                 * We would like to guarentee that the splitting operation is constrained in 
                 * the current subtree (with node as root).
                 *
                 * NOTE: Edge case - when we can split the child, we want to split them completely.
                 */
                
                while (child->numKeys() >= scheduler->ORDER_) {
                    if (child->childIndex < node->numKeys()) {
                        bigSplitToRight(scheduler->ORDER_, child, node->numChild() <= 2);
                        child_num ++;
                    }
                    else bigSplitToLeft(scheduler->ORDER_, child, node->numChild() <= 2);
                    
                }
                DBG_ASSERT(!node->children.empty());
                DBG_ASSERT(node->children.back() != nullptr);
                node->consolidateChild();

            } else if (!isHalfFull(child, scheduler->ORDER_)) {
                if (child->childIndex == 0) { // leftmost child
                    // try borrow from right
                    if (tryBorrow(scheduler->ORDER_, child, child->next, false)) continue;
                    // right merge to itself
                    merge(scheduler, scheduler->ORDER_, child, child->next, false, node->numChild() == 2);
                    child_num --;
                } else if (child->childIndex < node->numKeys()) { // middle 
                    // try borrw from left
                    if (tryBorrow(scheduler->ORDER_, child->prev, child, true)) continue;
                    // try borrow from right
                    if (tryBorrow(scheduler->ORDER_, child, child->next, false)) continue;
                    // merge with right
                    merge(scheduler, scheduler->ORDER_, child, child->next, true, node->numChild() == 2);
                    /**
                     * merge(...) will delete child, which makes it use-after-free to get child->next
                     * So we want to use the previous child->next (old_child_next) in this case.
                     */
                } else { // rightmost child
                    // try borrow from left
                    if (tryBorrow(scheduler->ORDER_, child->prev, child, true)) continue;
                    // left merge to itself
                    merge(scheduler, scheduler->ORDER_, child->prev, child, true, node->numChild() == 2);
                }
                node->consolidateChild();
            }
            child = child->next;
        }

        // If current node is filled up, request further update on parent layer
        if (node->numKeys() >= scheduler->ORDER_ || !isHalfFull(node, scheduler->ORDER_)) {
            scheduler->internal_request_queue.push(
                Request{TreeOp::UPDATE, std::nullopt, -1, node->parent}
            );
        }
    }

    static FreeNode<T>* lockFreeFindLeafNode(FreeNode<T>* node, T key) {
        while (!node->isLeaf) {
            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            node = node->children[index];
        }
        return node;
    }   

    /**
     * Execute on leaf node - get element directly
     * */    
    static std::optional<T> getFromLeaf(FreeNode<T> *node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            return *it;
        }
        return std::nullopt;
    }

    /**
     * Execute on leaf node - insert element directly
     * */
    static void insertKeyToLeaf(FreeNode<T> *node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

    /**
     * Execute on leaf node - remove element directly
     * */
    static bool removeFromLeaf(FreeNode<T> *node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            return true;
        }
        return false;
    }

    /**
     * @return true if the (right node if borrowFromLeft) | (left node if !borrowFromLeft) get
     * enough key to be at least half-full and do not need further modification.
     */
    static bool tryBorrow(int order, FreeNode<T> *left, FreeNode<T> *right, bool borrowFromLeft) {
        DBG_ASSERT(left->isLeaf == right->isLeaf);

        FreeNode<T>* parent = right->parent;
        size_t index = left->childIndex;

        if (borrowFromLeft) {
            /**
             * Case 1. Borrow from left node
             * Since execute in batch, may borrow multiple keys in one operation. Use
             * while loop
             *      moreHalfFull(left, order)     Left have excessive key
             *      !isHalfFull(right, order)     Right need borrow key
             */
            while (moreHalfFull(left, order) && !isHalfFull(right, order)) {
                T keyParentMove = parent->keys[index],
                  keySiblingMove = left->keys.back();

                parent->keys[index] = keySiblingMove;
                if (!right->isLeaf) {
                    /**
                     * Case 1a. Borrow from left where both are internal nodes
                     * 
                     * NOTE: If is an internal node, we want to preserve the ordering relationship
                     * between keys and subtrees. Therefore the parent key is used.
                     * */
                    right->keys.insert(right->keys.begin(), keyParentMove);
                    left->keys.pop_back();

                    right->children.push_front(left->children.back());
                    left->children.pop_back();
                } else {
                    /**
                     * Case 1b. Borrow from left where both are leaves
                     * 
                     * NOTE: If is a leaf node, we want to preserve all keys in the leaf, so we will
                     * put sibling key into the node instead of the parent key.
                    */
                    right->keys.insert(right->keys.begin(), keySiblingMove);
                    left->keys.pop_back();
                }
                left->consolidateChild();
                right->consolidateChild();
            }
        } else {
            /**
             * Case 3. Left borrow from right.
             * Since execute in batch, may borrow multiple keys in one operation. Use
             * while loop
             *      !isHalfFull(left, order)       Left need borrow key
             *      moreHalfFull(right, order)     Right have excessive key
             */
            while (moreHalfFull(right, order) && !isHalfFull(left, order)) {
                T keyParentMove = parent->keys[index],
                  keySiblingMove = right->keys.front();

                right->keys.erase(right->keys.begin());
                if (left->isLeaf) {
                    /**
                     * Case 3b. Borrow from left where both are leaves
                     * 
                     * NOTE: If is a leaf node, we want to preserve all keys in the leaf, so we will
                     * put sibling key into the node instead of the parent key.
                     * */
                    left->keys.push_back(keySiblingMove);
                    parent->keys[index] = right->keys.front();
                } else {
                    /**
                     * Case 3a. Borrow from left where both are internal nodes
                     * 
                     * NOTE: If is an internal node, we want to preserve the ordering relationship
                     * between keys and subtrees. Therefore the parent key is used.
                     * */
                    left->keys.push_back(keyParentMove);
                    parent->keys[index] = keySiblingMove;
                    
                    left->children.push_back(right->children.front());
                    right->children.pop_front();
                }
                left->consolidateChild();
                right->consolidateChild();
            }
        }
        return isHalfFull(borrowFromLeft ? right : left, order);
    }

    /**
     * Just merge.
     */
    static void merge(Scheduler *scheduler, int order, FreeNode<T> *left, FreeNode<T> *right, bool leftMergeToRight, bool needLock) {
        FreeNode<T> *parent = left->parent;
        size_t index = left->childIndex;

        if (leftMergeToRight) {
            if (!left->isLeaf) {
                /**
                 * Case 3.a Merge with right where both nodes are internal nodes
                 * */
                right->keys.insert(right->keys.begin(), parent->keys[index]);
                right->children.insert(
                    right->children.begin(), left->children.begin(), left->children.end()
                );
            } else {
                /** Case 3.b Merge with right where both are leaves, nothing to do here. */
            }

            right->keys.insert(right->keys.begin(), left->keys.begin(), left->keys.end());
            left->keys.clear();
            right->consolidateChild();

            /** Fix linked list */
            right->prev = left->prev;
            /**
             * TODO: This behavior below is potentially race-condition causing. Since when the B+ tree
             * only have 2 child, merging one will inevitably cause the thread to access another subtree
             * (which might be owned by some other threads on the same level)
             * 
             * TODO: We can resolve this problem by introducing a "releaseQueue" in scheduler and let background
             * thread do the release job when free.
             */
            if (left->prev != nullptr) left->prev->next = right;
            
            parent->children.erase(parent->children.begin() + left->childIndex);

            scheduler->internal_release_queue.push(left);
            // delete left;
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
            left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
            right->keys.clear();
            left->consolidateChild();


            /** Fix linked list */
            left->next = right->next;
            if (right->next != nullptr) right->next->prev = left;

            parent->children.erase(parent->children.begin() + right->childIndex);  // todo

            /**
             * Since removing this might cause racing condition, we will handle all of them to the background
             * thread where they can be destructed in a thread-safe mannor.
             */
            scheduler->internal_release_queue.push(right);
            // delete right;
        }
        parent->keys.erase(parent->keys.begin() + index);
    }

    // internal_execute call bigSplitInternalToLeft
    static void bigSplitToLeft(int order, FreeNode<T> *child, bool needLock) {
        DBG_ASSERT (child->numKeys() >= order);

        FreeNode<T> *new_node = new FreeNode<T>(child->isLeaf),
                   *parent = child->parent;
        
        new_node->parent = parent;

        size_t numToSplitLeft;
        if ((child->numKeys() - order) >= ((order - 1) / 2)) { 
            // Recursive case: splitted new left node gets exactly (order - 1) keys
            numToSplitLeft = order-1;
        } else {
            // Base Case: splitted new left node gets exactly (order / 2) keys
            numToSplitLeft = ((order - 1) / 2);
        }

        size_t index = child->childIndex;
        if (child->isLeaf) {
            new_node->keys.insert(new_node->keys.begin(), child->keys.begin(), child->keys.begin() + numToSplitLeft);
            child->keys.erase(child->keys.begin(), child->keys.begin() + numToSplitLeft);
            parent->keys.insert(parent->keys.begin() + index, child->keys.front());
        } else {
            new_node->keys.insert(new_node->keys.begin(), child->keys.begin(), child->keys.begin() + numToSplitLeft);
            child->keys.erase(child->keys.begin(), child->keys.begin() + numToSplitLeft);
            parent->keys.insert(parent->keys.begin() + index, child->keys.front());
            child->keys.erase(child->keys.begin());

            new_node->children.insert(new_node->children.begin(), child->children.begin(), child->children.begin() + numToSplitLeft + 1);
            child->children.erase(child->children.begin(), child->children.begin() + numToSplitLeft + 1);
        }

        parent->children.insert(parent->children.begin() + index, new_node);

        new_node->consolidateChild();
        child->consolidateChild();
        parent->consolidateChild();

        /** Fix linked list */
        new_node->next = child;
        new_node->prev = child->prev;
        child->prev    = new_node;
        if (new_node->prev != nullptr) new_node->prev->next = new_node;

        DBG_ASSERT(isHalfFull(new_node, order));
        DBG_ASSERT(isHalfFull(child, order));
    }

    // internal_execute call bigSplitInternalToRight
    static void bigSplitToRight(int order, FreeNode<T> *child, bool needLock) {
        DBG_ASSERT (child->numKeys() >= order);

        FreeNode<T> *new_node = new FreeNode<T>(child->isLeaf),
                   *parent = child->parent;
        

        new_node->parent = parent;

        size_t numToSplitRight;
        if ((child->numKeys() - order) >= ((order-1) / 2)) { 
            // Recursive case: splitted new right node gets exactly (order - 1) keys
            numToSplitRight = order-1;
        } else {
            // Base Case: splitted new right node gets exactly (order / 2) keys
            numToSplitRight = ((order-1) / 2);
        }

        size_t index = child->childIndex;
        if (child->isLeaf) {
            new_node->keys.insert(new_node->keys.begin(), child->keys.end() - numToSplitRight, child->keys.end());
            child->keys.erase(child->keys.end() - numToSplitRight, child->keys.end());
            parent->keys.insert(parent->keys.begin() + index, new_node->keys.front());
        } else {
            new_node->keys.insert(new_node->keys.begin(), child->keys.end() - numToSplitRight, child->keys.end());
            child->keys.erase(child->keys.end() - numToSplitRight, child->keys.end());
            parent->keys.insert(parent->keys.begin() + index, child->keys.back());
            child->keys.pop_back();

            new_node->children.insert(new_node->children.begin(), child->children.end() - numToSplitRight-1, child->children.end());
            child->children.erase(child->children.end() - numToSplitRight-1, child->children.end());

            new_node->consolidateChild();
            child->consolidateChild();   
        }

        parent->children.insert(parent->children.begin() + index + 1, new_node);
        parent->consolidateChild();

        /** Fix linked list */
        new_node->prev = child;
        new_node->next = child->next;
        child->next = new_node;
        if (new_node->next != nullptr)  new_node->next->prev = new_node;

        DBG_ASSERT(isHalfFull(new_node, order));
        DBG_ASSERT(isHalfFull(child, order));
    }
};
}
