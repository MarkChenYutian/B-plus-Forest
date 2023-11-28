#include <algorithm>
#include <iostream>
#include <cassert>
#include <optional>
#include "tree.h"

namespace Tree {
    template<typename T>
    FineLockBPlusTree<T>::FineLockBPlusTree(int order): rootPtr(nullptr), ORDER_(order), size_(0) {}

    template <typename T>
    int FineLockBPlusTree<T>::size() {
        return size_;
    }

    template <typename T>
    FineLockBPlusTree<T>::~FineLockBPlusTree() {
        if (rootPtr != nullptr) rootPtr->releaseAll();
    }

    template <typename T>
    void FineLockBPlusTree<T>::insert(T key) {
        size_ ++;
        LockNode<T> *node;
        rootLock.lock();
        std::deque<std::shared_mutex*> dq = {&rootLock};
        if (rootPtr == nullptr) {  // tree is empty before
            rootPtr = new Tree::LockNode<T>(true);
            rootPtr->keys.push_back(key);
            while (!dq.empty()) {
                dq.front()->unlock();
                dq.pop_front();
            }
            return;
        }
        node = findLeafNodeInsert(rootPtr, key, dq);

        insertKey(node, key);
        if (node->numKeys() >= ORDER_) splitNode(node, key);
        
        while (!dq.empty()) {
            dq.front()->unlock();
            dq.pop_front();
            // TODO: pop_back()
        }
    }

    template <typename T>
    LockNode<T>* FineLockBPlusTree<T>::findLeafNodeInsert(LockNode<T>* node, T key, std::deque<std::shared_mutex*> &dq) {
        node->latch.lock();
        dq.push_back(&node->latch);
        
        while (!node->isLeaf) {
            /** TODO: Is this value < ORDER_ or <= ORDER_ */
            if (node->numKeys() + 1 < ORDER_) {
                // release stack
                // while (dq.size() > 1) {
                //     dq.front()->unlock();
                //     dq.pop_front();
                // }
                // assert(dq[0] == node->latch);
            }

            size_t index = node->getGtKeyIdx(key);
            LockNode<T> *child = node->children[index];
            
            child->latch.lock();
            dq.push_back(&child->latch);
            node = child;
        }
        return node;
    }

    template <typename T>
    LockNode<T>* FineLockBPlusTree<T>::findLeafNodeDelete(LockNode<T>* node, T key, std::deque<std::shared_mutex*> &dq) {
        node->latch.lock();
        dq.push_back(&node->latch);
        
        while (!node->isLeaf) {
            /**
             * The node is "safe" (will not trigger merge / borrow in this layer / parents)
             * So we can release all write locks for parents.
             */
            /** TODO: is this (ORDER_ / 2) or (ORDER_ / 2 + 1) ? */
            if (node->numKeys() > (ORDER_ / 2)) {
                // release stack
                // while (dq.size() > 1) {
                //     dq.front()->unlock();
                //     dq.pop_front();
                // }
                // assert(dq[0] == node->latch);
            }

            size_t index = node->getGtKeyIdx(key);
            LockNode<T> *child = node->children[index];

            child->latch.lock();
            dq.push_back(&child->latch);
            node = child;
        }
        return node;
    }

    /**
     * NOTE: retrieve the leaf node for a given key
     * Return the locknode pointer **with** shared mutex lock obtained (and not released)
     */
    template <typename T>
    LockNode<T>* FineLockBPlusTree<T>::findLeafNodeRead(LockNode<T>* node, T key) {
        node->latch.lock_shared();

        while (!node->isLeaf) {
            size_t index = node->getGtKeyIdx(key);
            LockNode<T> *child = node->children[index];

            child->latch.lock_shared();
            node->latch.unlock_shared();
            
            node = child;
        }
        return node;
    }
    
    template <typename T>
    void FineLockBPlusTree<T>::insertKey(LockNode<T>* node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

    template <typename T>
    void FineLockBPlusTree<T>::splitNode(LockNode<T>* node, T key) {
        LockNode<T> *new_node = new LockNode<T>(node->isLeaf);
        auto middle   = node->numKeys() / 2;
        auto mid_key  = node->keys[middle];

        auto node_key_middle   = node->keys.begin() + middle;
        auto node_key_end      = node->keys.end();

        auto node_child_middle = node->children.begin() + middle;
        auto node_child_end    = node->children.end();

        if (node->isLeaf) {
            /**
             * Case 1: Leaf node split - trivial
             * After splitting, the original "node" becomes [node, new_node]
             **/
            new_node->keys.insert(new_node->keys.begin(), node_key_middle, node_key_end);
            node->keys.erase(node_key_middle, node_key_end);
        } else { 
            /**
             * Case 2: Internal node split, need to rebuild children index 
             * So that children know where it is in parent node
             **/
            new_node->keys.insert(new_node->keys.begin(), node_key_middle+1, node_key_end);
            node->keys.erase(node_key_middle, node_key_end);

            new_node->children.insert(new_node->children.begin(), node_child_middle+1, node_child_end);
            node->children.erase(node_child_middle+1, node_child_end);

            new_node->consolidateChild();
            node->consolidateChild();
        }

        /**
         * After splitting the node directly, we need to register the new_node into
         * the B+ tree structure.
         */
        if (!node->parent) {
            /**
             * Case 1: The root is splitted, so we need to create a new root node
             * above both of them.
             */
            // std::unique_lock lock(rootLock);

            auto new_root = new LockNode<T>(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            
            node->next   = new_node;
            node->prev   = nullptr;
            new_node->prev   = node;
            new_node->next   = nullptr;

            new_root->consolidateChild();
            
            rootPtr = new_root;
            insertKey(new_root, mid_key);
            
        } else {
            /**
             * Case 2: The internal node (or leaf node) is splitted, now we want to 
             * register new_node into some parent node and maybe recursively split the 
             * parent if needed.
             */
            LockNode<T> *parent = node->parent;
            size_t index = node->childIndex;
            parent->keys.insert(parent->keys.begin() + index, mid_key); 
            parent->children.insert(parent->children.begin() + index + 1, new_node);

            /**
             * Since we inserted children in the middle of parent node, we have to rebuild the 
             * childIndex for all children of parent using consolidateChild() method.
             */
            parent->consolidateChild();
            
            /**
             * Rebuild linked list in internal node level
             */
            new_node->next = node->next;
            new_node->prev = node;
            node->next = new_node;
            if (new_node->next != nullptr) new_node->next->prev = new_node;

            /**
             * If the parent is too full, split the parent node recursively.
             */
            if (parent->numKeys() >= ORDER_) splitNode(parent, key);
        }
    }

    template <typename T>
    std::optional<T> FineLockBPlusTree<T>::get(T key) {
        LockNode<T> *node;
        {
            std::shared_lock lock(rootLock);
            if (!rootPtr) return std::nullopt;
            node = findLeafNodeRead(rootPtr, key);
        }
        
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int index = std::distance(node->keys.begin(), it);

        if (index < node->numKeys() && node->keys[index] == key) {
            node->latch.unlock_shared();
            return key; // Key found in this node
        }

        node->latch.unlock_shared();
        return std::nullopt; // Key not found
    }

    template <typename T>
    bool FineLockBPlusTree<T>::isHalfFull(LockNode<T>* node) {
        return node->numKeys() >= (ORDER_ / 2);
    }

    template <typename T>
    bool FineLockBPlusTree<T>::moreHalfFull(LockNode<T>* node) {
        return node->numKeys() > (ORDER_ / 2);
    }

    template <typename T>
    bool FineLockBPlusTree<T>::remove(T key) {
        /** TODO: Do we need any locking here? */
        rootLock.lock();
        std::deque<std::shared_mutex*> dq = {&rootLock};
        LockNode<T>* node;

        if (rootPtr == nullptr) {
            while (!dq.empty()) {
                dq.front()->unlock();
                dq.pop_front();
            }
            return false;
        }
        node = findLeafNodeDelete(rootPtr, key, dq);        

        if (!removeFromLeaf(node, key)) {
            while (!dq.empty()) {
                dq.front()->unlock();
                dq.pop_front();
            }
            return false;
        }

        size_ --;
        /** Case 1: Removing the last element of tree
         *  the tree will be empty and rootPtr replaced by nullptr 
         * */
        if (node == rootPtr && node->numKeys() == 0) {
            rootPtr = nullptr;
            while (!dq.empty()) {
                    dq.front()->unlock();
                    dq.pop_front();
            }
            return true;
        }

        /** 
         * Case 2a: If the node is less than half full (< Order / 2), 
         * borrow (rebalance) the tree 
         * */
        if (!isHalfFull(node)) {
            removeBorrow(node);
        }
        
        while (!dq.empty()) {
            dq.front()->unlock();
            dq.pop_front();
        }
        return true;
    }

    template <typename T>
    void FineLockBPlusTree<T>::removeBorrow(LockNode<T> *node) {
        // Edge case: root has no sibling node to borrow with
        if (node->parent == nullptr && node == rootPtr) {
            if (node->numKeys() == 0) {
                rootPtr = node->children[0];
                rootPtr->parent = nullptr;
                /** TODO: This is a leak, but adding back will unlock a free'd mutex */
                // delete node;
            }
            return;
        };
        

        /**
         * NOTE: For the remaining cases, we know that node cannot be the root pointer,
         * hence node must have a valid parent pointer.
         * 
         * For the simplicity and fine grained locking, we always operate the sibling node
         * with same direct parent as current node.
         */
        LockNode<T> *leftNode = node->prev
                ,  *rightNode = node->next;
        
        if (leftNode != nullptr && leftNode->parent == node->parent) {
            /**
             * If left node exists and have same parent as current node, we 
             * 1. try to borrow from left node (node -> prev)
             * 2. If 1) failed, try to merge with left node (node -> prev)
             */
            if (moreHalfFull(leftNode)) {
                if (!node->isLeaf) {
                    /**
                     * Case 1a. Borrow from left, where both are internal nodes
                     */
                    size_t index = leftNode->childIndex;

                    T keyParentMove = node->parent->keys[index],
                      keySiblingMove = leftNode->keys.back();
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.push_front(keyParentMove);
                    leftNode->keys.pop_back();

                    node->children.push_front(leftNode->children.back());
                    leftNode->children.pop_back();
                    node->consolidateChild();
                } else {
                    /**
                     * Case 1b. Borrow from left, where both are leaves
                     */
                    T keyBorrow = node->prev->keys.back();
                    node->keys.push_front(keyBorrow);
                    node->prev->keys.pop_back();
                    node->parent->rebuild();
                }
                
            } else {
                /**
                 * Case 2. Have to merge with left node
                 */
                removeMerge(node);
            }
        } else {
            /**
             * If right node exists and have same parent as current node, we 
             * 1. try to borrow from right node (node -> next)
             * 2. If 1) failed, try to merge with right node (node -> next)
             */
            if (moreHalfFull(rightNode)) {
                if (!node->isLeaf) {
                    /**
                     * Case 3a. Borrow from right, where both are internal nodes
                     */
                    size_t index = node->childIndex;
                    
                    T keyParentMove  = node->parent->keys[index],
                      keySiblingMove = rightNode->keys[0];
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.push_back(keyParentMove);
                    rightNode->keys.pop_front();

                    node->children.push_back(rightNode->children[0]);
                    rightNode->children.pop_front();

                    // Since we have changed the children's index for both node and right node
                    // We need to update the childIndex for both (unlike leftNode case)
                    node->consolidateChild();
                    rightNode->consolidateChild();
                } else { 
                    /**
                     * Case 3b. Borrow from right, where both are leaves
                     */
                    T keyBorrow = *(node->next->keys.begin());
                    node->keys.push_back(keyBorrow);
                    node->next->keys.pop_front();
                    node->parent->rebuild();
                }
            } else {
                /**
                 * Case 4. Merge with right
                 */
                removeMerge(node);
            }

        }
    }

    template <typename T>
    void FineLockBPlusTree<T>::removeMerge(LockNode<T>* node) {
        /**
         * NOTE: No need to handle root here since we always first try to borrow
         * then perform merging. (Root cannot borrow, so this removeMerge will
         * not be called)
         *
         * NOTE: When merging, always merge with a sibling that shares same direct
         * parent with node.
         * 
         * This is guarenteed to exist since
         *  1. Every parent have at least 2 children
         *  2. One of the sibling (left / right) must be of same parent by (1)
         */
        if (node->prev != nullptr && (node->prev->parent == node->parent)) {
            if (!node->isLeaf) {
                /**
                 * Case 1a. Merge with left where both are internal nodes
                 * 
                 * First, we want to find the key in parent that is larger then node->prev
                 * (the key in between of node -> prev and node)
                 * */
                size_t index = node->prev->childIndex;
                node->prev->keys.push_back(node->parent->keys[index]);
                node->parent->keys.erase(node->parent->keys.begin() + index);

                node->prev->children.insert(
                    node->prev->children.end(), node->children.begin(), node->children.end()
                );
                node->prev->keys.insert(node->prev->keys.end(), node->keys.begin(), node->keys.end());
                node->keys.clear();

                // Set parent pointer and rebuild childIndex
                node->prev->consolidateChild();
            } else {
                /**
                 * Case 1b. Merge with left node where both are leaves
                 */
                size_t index = node->prev->childIndex;
                node->parent->keys.erase(node->parent->keys.begin() + index);

                node->prev->keys.insert(node->prev->keys.end(), node->keys.begin(), node->keys.end());
                node->keys.clear();
            }

            LockNode<T> *parent = node->parent;
            parent->rebuild();
            
            /**
             * NOTE: If after merging, the parent is less than half full, to rebalance the B+ tree
             * we will need to borrow for the parent node.
             */
            if (!isHalfFull(parent)) removeBorrow(parent);
            
        } else {
            if (!node->isLeaf) { 
                /**
                 * Case 3.a Merge with right where both nodes are internal nodes
                 * */
                size_t index = node->childIndex;

                /**
                 * Then, we assign the keys to merging node
                 * */
                node->keys.push_back(node->parent->keys[index]);
                node->parent->keys.erase(node->parent->keys.begin() + index);

                node->children.insert(node->children.end(), node->next->children.begin(), node->next->children.end());
                node->keys.insert(node->keys.end(), node->next->keys.begin(), node->next->keys.end());
                node->next->keys.clear(); 

                // Reassign parents & reconstruct childIdx
                node->consolidateChild();
            } else {
                size_t index = node->childIndex;

                /**
                 * Then, we assign the keys to left child node (node)
                 * */
                node->parent->keys.erase(node->parent->keys.begin() + index);
                node->keys.insert(node->keys.end(), node->next->keys.begin(), node->next->keys.end());
                node->next->keys.clear();
            }
            node->parent->rebuild();

            /**
             * If parent node is less than half-full due to the key borrowing
             * during merge, need to rebalance the parent node.
             */
            if (!isHalfFull(node->parent)) removeBorrow(node->parent);
        }
    }
    
    template <typename T>
    bool FineLockBPlusTree<T>::removeFromLeaf(LockNode<T>* node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            return true;
        }
        return false;
    }

    template <typename T>
    bool FineLockBPlusTree<T>::debug_checkIsValid(bool verbose) {
        {
            std::shared_lock lock(rootLock);
            if (rootPtr == nullptr) return (size_ == 0);
        }

        std::shared_lock lock2(rootLock);
        std::unique_lock lock1(rootPtr->latch);

        // checking parent child pointers
        bool isValidParentPtr = rootPtr->debug_checkParentPointers();
        if (!isValidParentPtr) return false;

        // checking ordering
        bool isValidOrdering = rootPtr->debug_checkOrdering(std::nullopt, std::nullopt);
        if (!isValidOrdering)  return false;

        // checking number of key/children
        bool isValidChildCnt = rootPtr->debug_checkChildCnt(ORDER_);
        if (!isValidChildCnt) return false;

        Tree::LockNode<T>* src = rootPtr;
        do {
            if (src->numChild() == 0) break;
            src = src->children[0];
            LockNode<T> *ckptr = src;

            // Check the leaf nodes linked list
            while (ckptr->next != nullptr) {
                if (ckptr->next->prev != ckptr) {
                    std::cerr << "Corrupted linked list!\nI will try to print the tree to help debugging:" << std::endl;
                    std::cout << "\033[1;31m FAILED";
                    this->print();
                    std::cout << "\033[0m";
                    return false;
                }

                if (ckptr->next->keys[0] < ckptr->keys.back()) {
                    std::cerr << "Leaves not well-ordered!\nI will try to print the tree to help debugging:" << std::endl;
                    std::cout << "\033[1;31m FAILED";
                    this->print();
                    std::cout << "\033[0m";
                    return false;
                }

                ckptr = ckptr->next;
            }
        } while (!src->isLeaf);

        int cnt_leaf_key = 0;
        for (;src != nullptr; src = src->next) {
            cnt_leaf_key += src->numKeys();
        }
        if (size_ != cnt_leaf_key) {
            std::cout << "FAIL: expect size " << size_ << " actual leaf cnt " << cnt_leaf_key << std::endl;
            return false;
        }

        if (verbose)
            std::cout << "\033[1;32mPASS! tree is valid" << " \033[0m" << std::endl;
        return true;
    }

    template <typename T>
    void FineLockBPlusTree<T>::print() {
        
        std::cout << "[Sequential B+ Tree]" << std::endl;
        if (!rootPtr) {
            std::cout << "(Empty)" << std::endl;
            return;
        }
        LockNode<T>* src = rootPtr;
        int level_cnt = 0;
        do {
            LockNode<T>* ptr = src;
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

    template <typename T>
    std::vector<T> FineLockBPlusTree<T>::toVec() {
        LockNode<T> *ptr = rootPtr;
        std::vector<T> vec;
        if (ptr == nullptr) return vec;
        
        for (; !ptr->isLeaf; ptr = ptr->children[0]){}
        while (ptr != nullptr) {
            for (T &key : ptr->keys) vec.push_back(key);
            ptr = ptr->next;
        }
        return vec;
    }
};

