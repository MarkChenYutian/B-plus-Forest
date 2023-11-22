#include <algorithm>
#include <iostream>
#include <cassert>
#include <optional>
#include "tree.h"

namespace Tree {
    template<typename T>
    SeqBPlusTree<T>::SeqBPlusTree(int order): rootPtr(nullptr), ORDER_(order) {}

    template <typename T>
    SeqNode<T> *SeqBPlusTree<T>::getRoot() {
        return rootPtr;
    }

    template <typename T>
    SeqBPlusTree<T>::~SeqBPlusTree() {
        if (rootPtr != nullptr) rootPtr->releaseAll();
    }

    template <typename T>
    void SeqBPlusTree<T>::insert(T key) {
        if (rootPtr == nullptr) { // tree is empty before
            rootPtr = new Tree::SeqNode<T>(true);
            rootPtr->keys.push_back(key);
        } else {
            auto node = findLeafNode(rootPtr, key);
            insertKey(node, key);
            if (node->keys.size() >= ORDER_) splitNode(node, key);
        }
    }

    template <typename T>
    SeqNode<T>* SeqBPlusTree<T>::findLeafNode(SeqNode<T>* node, T key) {
        while (!node->isLeaf) {
            size_t index = node->getGtKeyIdx(key);
            node = node->children[index];
        }
        return node;
    }
    
    template <typename T>
    void SeqBPlusTree<T>::insertKey(SeqNode<T>* node, T key) {
        size_t index = node->getGtKeyIdx(key);
        node->keys.insert(node->keys.begin() + index, key);
    }

    template <typename T>
    void SeqBPlusTree<T>::splitNode(SeqNode<T>* node, T key) {
        SeqNode<T> *new_node = new SeqNode<T>(node->isLeaf);
        auto middle   = node->keys.size() / 2;
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
            auto new_root = new SeqNode<T>(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            
            node->parent = new_root;
            node->next   = new_node;
            node->prev   = nullptr;
            node->childIndex = 0;
            
            new_node->parent = new_root;
            new_node->prev   = node;
            new_node->next   = nullptr;
            new_node->childIndex = 1;
            
            rootPtr = new_root;
            insertKey(new_root, mid_key);
        } else {
            /**
             * Case 2: The internal node is splitted, now we want to register
             * new_node into some parent node and maybe recursively split the parent
             * if needed.
             */
            SeqNode<T> *parent = node->parent;
            size_t mid_key_idx = parent->getGeKeyIdx(key);
            parent->keys.insert(parent->keys.begin() + mid_key_idx, mid_key); 

            auto key_it = std::lower_bound(parent->keys.begin(), parent->keys.end(), new_node->keys[0]);
            int index = std::distance(parent->keys.begin(), key_it);
            parent->children.insert(parent->children.begin() + index + 1, new_node);
            /**
             * Since we inserted children in the middle of parent node, we have to rebuild the 
             * childIndex for all children of parent using consolidateChild() method.
             */
            parent->consolidateChild();
            
            /**
             * Rebuild linked list in internal node level
             */
            new_node->prev = parent->children[index];
            parent->children[index]->next = new_node;

            if (index + 2 >= parent->children.size()){
                new_node->next = nullptr;
            } else {
                new_node->next = parent->children[index + 2];
                parent->children[index + 2]->prev = new_node;
            }

            /**
             * If the parent is too full, split the parent node recursively.
             */
            if (parent->keys.size() >= ORDER_) splitNode(parent, key);
        }
    }

    template <typename T>
    std::optional<T> SeqBPlusTree<T>::get(T key) {
        if (!rootPtr) {
            return std::nullopt;
        }
        auto node = findLeafNode(rootPtr, key);
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int index = std::distance(node->keys.begin(), it);

        if (index < node->keys.size() && node->keys[index] == key) {
            return key; // Key found in this node
        } 
        return std::nullopt; // Key not found
    }

    template <typename T>
    bool SeqBPlusTree<T>::isHalfFull(SeqNode<T>* node) {
        return node->keys.size() >= (ORDER_ / 2);
    }

    template <typename T>
    bool SeqBPlusTree<T>::moreHalfFull(SeqNode<T>* node) {
        return node->keys.size() > (ORDER_ / 2);
    }

    template <typename T>
    bool SeqBPlusTree<T>::remove(T key) {
        if (!rootPtr) {return false;}
        SeqNode<T>* node = findLeafNode(rootPtr, key);

        if (!removeFromLeaf(node, key)) return false;
        
        /** Case 1: Removing the last element of tree
         *  the tree will be empty and rootPtr replaced by nullptr 
         * */
        if (node == rootPtr && node->keys.size() == 0) {
            rootPtr = nullptr;
            return true;
        }
        
        /** 
         * Case 2a: If the node is less than half full, 
         * borrow (rebalance) the tree 
         * */
        if (!isHalfFull(node)) removeBorrow(node);
        
        /** 
         * Case 2b: If the node is balanced (> half), rebuild key
         * FIXME: (is this necessary?) 
         * Rebuild parent key in the tree
         * */
        else if (node->parent != nullptr) node->parent->rebuild(); ////// meaning?
        return true;
    }

    template <typename T>
    void SeqBPlusTree<T>::removeBorrow(SeqNode<T> *node) {
        // Edge case: root has no sibling node to borrow with
        if (node->parent == nullptr && node == rootPtr) {
            if (node->keys.size() == 0) {
                assert (node->children.size() == 1);
                rootPtr = node->children[0];
                delete node;
            }
            return;
        };
        assert(node->parent != nullptr); // root does not have sibling

        if (node->prev != nullptr && node->prev->parent == node->parent) {
            if (moreHalfFull(node->prev)) {
                // borrow from prev
                if (!node->isLeaf) {
                    // printf("internal node borrow from left\n");
                    int index = 0;
                    while (index < node->parent->keys.size() && node->parent->keys[index] <= node->prev->keys.back()) index ++;

                    assert(index < node->parent->keys.size() && node->parent->keys[index] > node->prev->keys.back());
                    assert(index < node->parent->keys.size() && node->parent->keys[index] <= node->keys.front());

                    T keyParentMove = node->parent->keys[index],
                      keySiblingMove = node->prev->keys.back();
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.push_front(keyParentMove);
                    node->prev->keys.pop_back();

                    node->children.push_front(node->prev->children.back());
                    node->prev->children.pop_back();
                    node->children[0]->parent = node;
                } else {
                    // printf("leaf node borrow from left\n");
                    T keyBorrow = node->prev->keys.back();
                    node->keys.push_front(keyBorrow);
                    node->prev->keys.pop_back();
                    node->parent->rebuild();
                }
                
            } else {
                // merge with left
                removeMerge(node);
            }
        } else {
            assert(node->next != nullptr && node->next->parent == node->parent);
            if (moreHalfFull(node->next)) {
                // borrow from right
                if (!node->isLeaf) { // internal node
                    // printf("internal node borrow from right\n");
                    int index = 0;
                    while (index < node->parent->keys.size() && node->parent->keys[index] <= node->keys.back()) index ++;
                    assert(index < node->parent->keys.size() && node->parent->keys[index] > node->keys.back());
                    assert(index < node->parent->keys.size() && node->parent->keys[index] <= node->next->keys.front());
                    
                    T keyParentMove = node->parent->keys[index],
                      keySiblingMove = node->next->keys[0];
                    
                    node->parent->keys[index] = keySiblingMove;
                    node->keys.push_back(keyParentMove);
                    node->next->keys.pop_front();

                    node->children.push_back(node->next->children[0]);
                    node->next->children.pop_front();
                    node->children.back()->parent = node;
                } else { // leaf node
                    // printf("leaf node borrow from right\n");
                    T keyBorrow = *(node->next->keys.begin());
                    node->keys.push_back(keyBorrow);
                    node->next->keys.pop_front();
                    node->parent->rebuild();
                    // updateKeyToLCA(node, node->next, false);
                }
            } else {
                removeMerge(node);
            }

        }
    }

    template <typename T>
    void SeqBPlusTree<T>::removeMerge(SeqNode<T>* node) {
        /**
         * NOTE: No need to handle root here since we always first try to borrow
         * then perform merging. (Roott cannot borrow, so this removeMerge will
         * not be called)
         */
        /**
         * NOTE: When merging, always merge with a sibling that shares same direct
         * parent with node.
         * 
         * This is guarenteed to exist since
         *  1. Every parent have at least 2 children
         *  2. One of the sibling (left / right) must be of same parent by (1)
         */
        if (node->prev != nullptr && (node->prev->parent == node->parent)) {             
            // merge with left
            // printf("merge with left\n");
            assert(node->prev->keys.size() > 0);
            assert(node->prev->keys.size() + node->keys.size() < ORDER_);

            if (!node->isLeaf) { // if it is internal node, also needs to merge children
                // printf("merge with left -- internal node\n");
                /**
                 * First, we want to find the key in parent that is larger then node->prev
                 * (the key in between of node -> prev and node)
                 * */ 
                int index = 0;
                while (index < node->parent->keys.size() && node->parent->keys[index] <= node->prev->keys.back()) index ++;
                
                assert(index < node->parent->keys.size() && node->parent->keys[index] > node->prev->keys.back());
                assert(index < node->parent->keys.size() && node->parent->keys[index] <= node->keys.front());

                node->prev->keys.push_back(node->parent->keys[index]);
                node->parent->keys.erase(node->parent->keys.begin() + index);
                // Reassign parents
                for (auto child : node->children) child->parent = node->prev;
                node->prev->children.insert(
                    node->prev->children.end(), node->children.begin(), node->children.end()
                );
            }


            node->prev->keys.insert(node->prev->keys.end(), node->keys.begin(), node->keys.end());
            node->keys.clear();
            auto parent = node->parent;
            parent->rebuild();
            
            // TODO: Check children after merge
            // assert(node->children.size() == node->keys.size() + 1 || (node->isLeaf && node->children.size() == 0));
            if (!isHalfFull(parent)) {
                // printf("calling remove borrow from remove merge with left\n");
                removeBorrow(parent);
            }
            
        } else { // merge with right
            // printf("merge with right\n");
            assert(node->next != nullptr); // Impossible!
            assert(node->next->keys.size() > 0);
            assert(node->keys.size() + node->next->keys.size() < ORDER_);
            
            if (!node->isLeaf) { 
                // if it is internal node, also needs to merge children
                // printf("merge with right -- internal node\n");
                /**
                 * First, we want to find the key in parent that is larger then node
                 * (the key in between of node and node -> next)
                 * */ 
                int index = 0;
                while (index < node->parent->keys.size() && node->parent->keys[index] <= node->keys.back()) index ++;

                assert(index < node->parent->keys.size() && node->parent->keys[index] > node->keys.back());
                assert(index < node->parent->keys.size() && node->parent->keys[index] <= node->next->keys.front());

                /**
                 * Then, we assign the keys to 
                 * */
                node->keys.push_back(node->parent->keys[index]);
                node->parent->keys.erase(node->parent->keys.begin() + index);
                // Reassign parents
                for (auto child : node->next->children) child->parent = node;
                node->children.insert(node->children.end(), node->next->children.begin(), node->next->children.end());
            }

            node->keys.insert(node->keys.end(), node->next->keys.begin(), node->next->keys.end());
            node->next->keys.clear();            
            node->parent->rebuild();

            // Check children after merge
            assert(node->children.size() == node->keys.size() + 1 || (node->isLeaf && node->children.size() == 0));
            if (!isHalfFull(node->parent)) removeBorrow(node->parent);
        }
    }
    
    template <typename T>
    bool SeqBPlusTree<T>::removeFromLeaf(SeqNode<T>* node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        // node->printKeys();
        // std::cout << "Erasing key at " << std::distance(node->keys.begin(), it) << std::endl;
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            return true;
        }
        // node->printKeys();
        return false;
    }

    template <typename T>
    void SeqBPlusTree<T>::debug_assertIsValid(bool verbose) {
        // Move to left-most leaf node
        Tree::SeqNode<T>* src = rootPtr;
        // checking parent child pointers
        src->debug_checkParentPointers();
        // checking ordering
        src->debug_checkOrdering(std::nullopt, std::nullopt);
        // checking number of key/children
        src->debug_checkChildCnt(ORDER_);

        do {
            src = src->children[0];
            auto ckptr = src;
            // Check the leaf nodes linked list
            while (ckptr->next != nullptr) {
                if (ckptr->next->prev != ckptr) {
                    std::cerr << "Corrupted linked list!\nI will try to print the tree to help debugging:" << std::endl;
                    std::cout << "\033[1;31m FAILED";
                    this->print();
                    std::cout << "\033[0m";
                    assert(false);
                }
                if (ckptr->next->keys[0] < ckptr->keys.back()) {
                    std::cerr << "Leaves not well-ordered!\nI will try to print the tree to help debugging:" << std::endl;
                    std::cout << "\033[1;31m FAILED";
                    this->print();
                    std::cout << "\033[0m";
                    assert(false);
                }
                ckptr = ckptr->next;
            }
        } while (!src->isLeaf);

        if (verbose)
            std::cout << "\033[1;32mPASS! tree is valid" << " \033[0m" << std::endl;
    }

    template <typename T>
    void SeqBPlusTree<T>::print() {
        
        std::cout << "[Sequential B+ Tree]" << std::endl;
        if (!rootPtr) {
            std::cout << "(Empty)" << std::endl;
            return;
        }
        SeqNode<T>* src = rootPtr;
        int level_cnt = 0;
        do {
            SeqNode<T>* ptr = src;
            std::cout << level_cnt << "\t| ";
            while (ptr != nullptr) {
                ptr->printKeys();
                std::cout << "<->";
                ptr = ptr->next;
            }
            level_cnt ++;
            std::cout << std::endl;
            if (src->children.size() == 0) break;
            src = src->children[0];
        } while (true);
        
        std::cout << std::endl;
    }
};

