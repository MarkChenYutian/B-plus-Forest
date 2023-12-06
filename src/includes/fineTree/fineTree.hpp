#pragma once
#include <algorithm>
#include <iostream>
#include <cassert>
#include <optional>
#include "tree.h"
#include "fineTree/fineNode.hpp"

namespace Tree {
    template<typename T>
    FineLockBPlusTree<T>::FineLockBPlusTree(int order): ORDER_(order), size_(0), rootPtr(FineNode<T>(true, true)) {}

    template <typename T>
    FineNode<T> *FineLockBPlusTree<T>::getRoot() {
        return &rootPtr;
    }

    template <typename T>
    int FineLockBPlusTree<T>::size() {
        return size_;
    }

    template <typename T>
    FineLockBPlusTree<T>::~FineLockBPlusTree() {
        if (rootPtr.numChild() != 0) rootPtr.children[0]->releaseAll();
    }

    template <typename T>
    void FineLockBPlusTree<T>::insert(T key) {
        size_ ++;

        LockDeque<T> dq = LockDeque<T>(false);
        FineNode<T> *node = findLeafNodeInsert(&rootPtr, key, dq);
        DBG_ASSERT(dq.isLocked(node));

        if (node == &rootPtr) {
            FineNode<T> *root = new FineNode<T>(true);
            insertKey(root, key);

            rootPtr.children.push_back(root);
            rootPtr.isLeaf = false;
            rootPtr.consolidateChild();
        } else {
            insertKey(node, key);

            if (node->numKeys() >= ORDER_) {
                DBG_ASSERT(dq.isLocked(node->parent));
                splitNode(node, key);
            }
        }

        dq.releaseAll();
    }

    template <typename T>
    FineNode<T>* FineLockBPlusTree<T>::findLeafNodeRead(FineNode<T>* node, T key, LockDeque<T> &dq) {
        DBG_ASSERT(node == &rootPtr);
        dq.retrieveLock(node);

        while (!node->isLeaf) {
            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            FineNode<T> *child = node->children[index];

            dq.retrieveLock(child);
            dq.releasePrev();

            node = child;
        }
        return node;
    }

    template <typename T>
    FineNode<T>* FineLockBPlusTree<T>::findLeafNodeInsert(FineNode<T>* node, T key, LockDeque<T> &dq) {
        DBG_ASSERT(node == &rootPtr);
        dq.retrieveLock(node);

        while (!node->isLeaf) {
            if (node->numKeys() + 1 < ORDER_) {
                dq.releasePrev();
            }

            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            FineNode<T> *child = node->children[index];
            dq.retrieveLock(child);
            
            node = child;
        }
        
        return node;
    }

    template <typename T>
    FineNode<T>* FineLockBPlusTree<T>::findLeafNodeDelete(FineNode<T>* node, T key, LockDeque<T> &dq) {
        DBG_ASSERT(node == &rootPtr);
        dq.retrieveLock(node);

        while (!node->isLeaf) {
            if (moreHalfFull(node)) {
                dq.releasePrev();
            }
            /** getGTKeyIdx will have index = 0 if node is dummy node */
            size_t index = node->getGtKeyIdx(key);
            FineNode<T> *child = node->children[index];
            dq.retrieveLock(child);
            node = child;
        }
        return node;
    }
    
    template <typename T>
    void FineLockBPlusTree<T>::insertKey(FineNode<T>* node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

    template <typename T>
    void FineLockBPlusTree<T>::splitNode(FineNode<T>* node, T key) {
        DBG_ASSERT(node != &rootPtr);
        FineNode<T> *new_node = new FineNode<T>(node->isLeaf);
        auto middle   = node->numKeys() / 2;
        auto mid_key  = node->keys[middle];

        auto node_key_begin    = node->keys.begin();
        auto node_key_middle   = node->keys.begin() + middle;
        auto node_key_end      = node->keys.end();

        auto node_child_begin  = node->children.begin();
        auto node_child_middle = node->children.begin() + middle;
        auto node_child_end    = node->children.end();

        /**
         * NOTE: For the fine-grained lock implementation, we only want to 
         * modify the nodes within same subtree as current node when splitting
         * to prevent over-lock a data structure / have data race when accessing
         * across subtrees.
         * 
         * If current node is the right-most node of its parent, we convert 
         *      (, A, node) -> (, A, new_node, node)
         * Otherwise, we convert
         *      (node, A, ) -> (node, new_node, A, ) // newNodeOnRight
         * 
         * In this way, we will not need to touch the linked list pointers in different
         * subtree.
         * 
         * If newNodeOnRight -> (node, new_node)
         */
        bool newNodeOnRight = (node->parent == &rootPtr) || (node->childIndex != node->parent->numChild() - 1);

        if (node->isLeaf) {
            /**
             * Case 1: Leaf node split - trivial
             * After splitting, the original "node" becomes [node, new_node]
             **/
            if (newNodeOnRight) {
                new_node->keys.insert(new_node->keys.begin(), node_key_middle, node_key_end);
                node->keys.erase(node_key_middle, node_key_end);
            } else {
                new_node->keys.insert(new_node->keys.begin(), node_key_begin, node_key_middle);
                node->keys.erase(node_key_begin, node_key_middle);
            }
        } else { 
            /**
             * Case 2: Internal node split, need to rebuild children index 
             * So that children know where it is in parent node
             **/
            if (newNodeOnRight) {
                new_node->keys.insert(new_node->keys.begin(), node_key_middle+1, node_key_end);
                node->keys.erase(node_key_middle, node_key_end);

                new_node->children.insert(new_node->children.begin(), node_child_middle+1, node_child_end);
                node->children.erase(node_child_middle+1, node_child_end);
            } else {
                new_node->keys.insert(new_node->keys.begin(), node_key_begin, node_key_middle);
                node->keys.erase(node_key_begin, node_key_middle+1);

                new_node->children.insert(new_node->children.begin(), node_child_begin, node_child_middle+1);
                node->children.erase(node_child_begin, node_child_middle+1);
            }

            new_node->consolidateChild();
            node->consolidateChild();
        }

        /**
         * After splitting the node directly, we need to register the new_node into
         * the B+ tree structure.
         */
        if (node->parent == &rootPtr) {
            /**
             * Case 1: The root is splitted, so we need to create a new root node
             * above both of them.
             * 
             * Current node is the root.
             * 
             * This case must have newNodeOnRight = true since original root is both
             * the left-most and right-most child.
             */
            assert (newNodeOnRight);
            FineNode<T> *new_root = new FineNode<T>(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            
            node->next   = new_node;
            node->prev   = nullptr;
            new_node->prev   = node;
            new_node->next   = nullptr;

            new_root->consolidateChild();
            
            /** Update the dummy node */
            new_root->parent = &rootPtr;
            new_root->childIndex = 0;
            rootPtr.children[0] = new_root;
            insertKey(new_root, mid_key);
        } else {
            /**
             * Case 2: The internal node (or leaf node) is splitted, now we want to 
             * register new_node into some parent node and maybe recursively split the 
             * parent if needed.
             */
            FineNode<T> *parent = node->parent;
            size_t index = node->childIndex;
                        
            if (newNodeOnRight) {
                parent->keys.insert(parent->keys.begin()+index, mid_key);
                parent->children.insert(parent->children.begin() + index + 1, new_node);
            } else {
                parent->keys.insert(parent->keys.begin()+index, mid_key);
                parent->children.insert(parent->children.begin() + index, new_node);
            }
                        

            /**
             * Since we inserted children in the middle of parent node, we have to rebuild the 
             * childIndex for all children of parent using consolidateChild() method.
             */
            parent->consolidateChild();
            
            /**
             * Rebuild linked list in internal node level
             */
            DBG_ASSERT(new_node->parent == node->parent);
            if (newNodeOnRight) {
                new_node->next = node->next;
                new_node->prev = node;
                node->next = new_node;
                
                /** NOTE: We want to ensure this for the correctness of fine-grain lock  */
                DBG_ASSERT(new_node->next != nullptr);
                DBG_ASSERT(new_node->parent == new_node->next->parent);
                new_node->next->prev = new_node;
            } else {
                new_node->next = node;
                new_node->prev = node->prev;
                node->prev = new_node;

                /** NOTE: We want to ensure this for the correctness of fine-grain lock  */
                DBG_ASSERT(new_node->prev != nullptr);
                DBG_ASSERT(new_node->prev->parent = new_node->parent);
                new_node->prev->next = new_node;
            }
            

            /**
             * If the parent is too full, split the parent node recursively.
             */
            if (parent->numKeys() >= ORDER_) splitNode(parent, key);
        }
    }

    template <typename T>
    std::optional<T> FineLockBPlusTree<T>::get(T key) {
        LockDeque<T> dq = LockDeque<T>(true);
        FineNode<T> *node = findLeafNodeRead(&rootPtr, key, dq);

        DBG_ASSERT(dq.isLocked(node));

        if (node == &rootPtr) {
            dq.releaseAll();
            return std::nullopt;
        }

        DBG_ASSERT(node != &rootPtr);
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int index = std::distance(node->keys.begin(), it);

        if (index < node->numKeys() && node->keys[index] == key) {
            dq.releaseAll();
            return key; // Key found in this node
        }

        dq.releaseAll();
        return std::nullopt; // Key not found
    }

    template <typename T>
    bool FineLockBPlusTree<T>::isHalfFull(FineNode<T>* node) {
        return node->numKeys() >= ((ORDER_-1) / 2);
    }

    template <typename T>
    bool FineLockBPlusTree<T>::moreHalfFull(FineNode<T>* node) {
        return node->numKeys() > ((ORDER_-1) / 2);
    }

    template <typename T>
    bool FineLockBPlusTree<T>::remove(T key) {
        LockDeque<T> dq = LockDeque<T>(false);
        FineNode<T>* node = findLeafNodeDelete(&rootPtr, key, dq);
        DBG_ASSERT(dq.isLocked(node));
        /**
         * NOTE: If the tree is empty, then node must be rootPtr
         * and since rootPtr have no key, removeFromLeaf(rootPtr, key)
         * must return false.
         */
        if (!removeFromLeaf(node, key)) {
            dq.releaseAll();
            return false;
        }
        
        DBG_ASSERT(node != &rootPtr);
        size_ --;
        /** 
         * Case 1: Removing the last element of tree
         * the tree will be empty and rootPtr replaced by nullptr 
         * */
        if (node->parent == &rootPtr && node->numKeys() == 0) {
            DBG_ASSERT(dq.isLocked(&rootPtr));

            rootPtr.children.clear();
            rootPtr.isLeaf = true;

            
            dq.popAndDelete(node);
            // delete node; // TODO: check if correct

            dq.releaseAll();
            return true;
        }
        
        /** 
         * Case 2: If the node is less than half full, 
         * borrow (rebalance) the tree 
         * */
        if (!isHalfFull(node)) removeBorrow(node, dq);

        dq.releaseAll();
        return true;
    }

    template <typename T>
    void FineLockBPlusTree<T>::removeBorrow(FineNode<T> *node, LockDeque<T> &dq) {
        // Edge case: root has no sibling node to borrow with
        if (node->parent == &rootPtr) {
            if (node->numKeys() == 0) {
                DBG_ASSERT(dq.isLocked(&rootPtr));
                
                rootPtr.children[0] = node->children[0];
                rootPtr.consolidateChild();

                /** TODO: ????? */
                dq.popAndDelete(node);
                // delete node;
            }
            return;
        };

        /**
         * NOTE: For the remaining cases, we know that node cannot be the root,
         * hence node must have a valid parent pointer.
         * 
         * For the simplicity and fine grained locking, we always operate the sibling node
         * with same direct parent as current node.
         */
        
        if (node->childIndex > 0) {
            /**
             * Left node exists and have same parent as current node, we 
             * 1. try to borrow from left node (node -> prev)
             * 2. If 1) failed, try to merge with left node (node -> prev)
             */
            FineNode<T> *leftNode = node->prev;
            DBG_ASSERT(leftNode->parent == node->parent);
            if (moreHalfFull(leftNode)) {
                size_t index = leftNode->childIndex;
                if (!node->isLeaf) {
                    /**
                     * Case 1a. Borrow from left, where both are internal nodes
                     */

                    T keyParentMove = node->parent->keys[index],
                      keySiblingMove = leftNode->keys.back();
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.insert(node->keys.begin(), keyParentMove);
                    leftNode->keys.pop_back();

                    node->children.push_front(leftNode->children.back());
                    leftNode->children.pop_back();
                    node->consolidateChild();
                } else {
                    /**
                     * Case 1b. Borrow from left, where both are leaves
                     */
                    T keySiblingMove = leftNode->keys.back();

                    node->parent->keys[index] = keySiblingMove;
                    node->keys.insert(node->keys.begin(), keySiblingMove);
                    leftNode->keys.pop_back();
                }
                
            } else {
                /**
                 * Case 2. Have to merge with left node
                 */
                removeMerge(node, dq);
            }
        } else {
            DBG_ASSERT(node->childIndex + 1 < node->parent->numChild());
            /**
             * If right node exists and have same parent as current node, we 
             * 1. try to borrow from right node (node -> next)
             * 2. If 1) failed, try to merge with right node (node -> next)
             */
            FineNode<T> *rightNode = node->next;
            DBG_ASSERT(rightNode->parent == node->parent);

            if (moreHalfFull(rightNode)) {
                size_t index = node->childIndex;
                if (!node->isLeaf) {
                    /**
                     * Case 3a. Borrow from right, where both are internal nodes
                     */
                    
                    T keyParentMove  = node->parent->keys[index],
                      keySiblingMove = rightNode->keys[0];
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.push_back(keyParentMove);
                    rightNode->keys.erase(rightNode->keys.begin());

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
                    T keySiblingMove = node->next->keys[0];

                    node->keys.push_back(keySiblingMove);
                    node->next->keys.erase(node->next->keys.begin());
                    node->parent->keys[index] = node->next->keys[0];
                }
            } else {
                /**
                 * Case 4. Merge with right
                 */
                removeMerge(node, dq);
            }

        }
    }

    template <typename T>
    void FineLockBPlusTree<T>::removeMerge(FineNode<T>* node, LockDeque<T> &dq) {
        bool leftMergeToRight;
        FineNode<T> *leftNode, *rightNode, *parent;

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

        if (node->parent->numChild() == 2) {
            // node->parent->parent locked
            if (node->parent->childIndex == 0) { 
                leftMergeToRight = false;
                // parent is the leftmost of its parent, 
                if (node->childIndex == 0) {
                    // node is the leftmost of its parent
                    leftNode = node;
                    rightNode = node->next;
                } else {
                    // node is not the leftmost of its parent
                    leftNode = node->prev;
                    rightNode = node;
                }
            } else {
                // parent is NOT the leftmost of its parent (could be the rightmost) 
                leftMergeToRight = true;
                if (node->childIndex == 0) {
                    leftNode = node;
                    rightNode = node->next;
                } else {
                    leftNode = node->prev;
                    rightNode = node;
                }
            }
        } else {
            DBG_ASSERT(node->parent->numChild() >= 3);
            if (node->childIndex == 0) {
                leftNode = node;
                rightNode = node->next;
                leftMergeToRight = false;
            } else {
                leftNode = node->prev;
                rightNode = node;
                leftMergeToRight = true;
            }    
        }
        assert (leftNode->parent == rightNode->parent);
        parent = leftNode->parent;

        if (leftMergeToRight) {
            size_t index = leftNode->childIndex;
                
            if (!leftNode->isLeaf) {
                /**
                 * Case 3.a Merge with right where both nodes are internal nodes
                 * */
                rightNode->keys.insert(rightNode->keys.begin(), parent->keys[index]);
                rightNode->children.insert(
                    rightNode->children.begin(), leftNode->children.begin(), leftNode->children.end()
                );
            } else {
                /**
                 * Case 3.b Merge with right where both are leaves, nothing to do here.
                 */
            }

            parent->keys.erase(parent->keys.begin() + index);
            parent->children.erase(parent->children.begin() + leftNode->childIndex);

            rightNode->keys.insert(rightNode->keys.begin(), leftNode->keys.begin(), leftNode->keys.end());
            leftNode->keys.clear();
            rightNode->consolidateChild();

            /** Fix linked list */
            rightNode->prev = leftNode->prev;
            if (leftNode->prev != nullptr) leftNode->prev->next = rightNode;

            // delete leftNode;
            dq.popAndDelete(leftNode);
        } else { 
            // Right merge to Left
            size_t index = leftNode->childIndex;
            if (!rightNode->isLeaf) { // internal node
                /**
                 * Case 1a. Merge with left where both are internal nodes
                 * 
                 * First, we want to find the key in parent that is larger then node->prev
                 * (the key in between of node -> prev and node)
                 * */
                leftNode->keys.push_back(parent->keys[index]);
                leftNode->children.insert(
                    leftNode->children.end(), rightNode->children.begin(), rightNode->children.end()
                );
            } else { // leaf node
                /** Case 1b. if are leaves, don't need to do operations above */
            }
            parent->keys.erase(parent->keys.begin() + index);
            parent->children.erase(parent->children.begin() + rightNode->childIndex);

            leftNode->keys.insert(leftNode->keys.end(), rightNode->keys.begin(), rightNode->keys.end());
            rightNode->keys.clear();
            leftNode->consolidateChild();

            /** Fix linked list */
            leftNode->next = rightNode->next;
            if (rightNode->next != nullptr) rightNode->next->prev = leftNode;

            // delete rightNode;
            dq.popAndDelete(rightNode);
        }
        parent->consolidateChild();


        /**
         * NOTE: If after merging, the parent is less than half full, to rebalance the B+ tree
         * we will need to borrow for the parent node.
         */
        if (!isHalfFull(parent)) removeBorrow(parent, dq);
    }
    
    template <typename T>
    bool FineLockBPlusTree<T>::removeFromLeaf(FineNode<T>* node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            return true;
        }
        return false;
    }

    template <typename T>
    bool FineLockBPlusTree<T>::debug_checkIsValid(bool verbose) {
        if (!rootPtr.isDummy) return false;
        if (rootPtr.numChild() == 0) return size_ == 0;
        if (rootPtr.numChild() > 1) return false;

        // checking parent child pointers
        DBG_ASSERT(rootPtr.children[0] != nullptr);
        bool isValidParentPtr = rootPtr.children[0]->debug_checkParentPointers();
        if (!isValidParentPtr) return false;

        // checking ordering
        bool isValidOrdering = rootPtr.children[0]->debug_checkOrdering(std::nullopt, std::nullopt);
        if (!isValidOrdering)  return false;

        // checking number of key/children
        bool isValidChildCnt = rootPtr.children[0]->debug_checkChildCnt(ORDER_);
        if (!isValidChildCnt) return false;

        Tree::FineNode<T>* src = rootPtr.children[0];
        do {
            if (src->numChild() == 0) break;
            src = src->children[0];
            FineNode<T> *ckptr = src;

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
        if (rootPtr.numChild() == 0) {
            std::cout << "(Empty)" << std::endl;
            return;
        }
        FineNode<T>* src = &rootPtr;
        int level_cnt = 0;
        do {
            FineNode<T>* ptr = src;
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
        FineNode<T> *ptr = &rootPtr;
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

