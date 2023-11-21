#include <algorithm>
#include <iostream>
#include <vector>
#include <cassert>
#include <optional>
#include "tree.h"

namespace Tree {
    template<typename T>
    BPlusTree<T>::BPlusTree(int order): rootPtr(nullptr), ORDER_(order) {}

    template <typename T>
    void recursive_clean(Node<T>* ptr) {
        if (ptr->isLeaf) {
            delete ptr;
            return;
        }
        for (auto child : ptr->children) recursive_clean(child);
        delete ptr;
    }

    template <typename T>
    Node<T> *BPlusTree<T>::getRoot() {return rootPtr;}

    template <typename T>
    int BPlusTree<T>::getOrder() {return ORDER_;}

    template <typename T>
    BPlusTree<T>::~BPlusTree() {
        if (rootPtr == nullptr) return;
        recursive_clean(rootPtr);
    }

    template <typename T>
    void BPlusTree<T>::insert(T key) {
        if (rootPtr == nullptr) { // tree is empty before
            rootPtr = new Tree::Node<T>(true);
            rootPtr->keys.push_back(key);
        } else {
            auto node = findLeafNode(rootPtr, key);
            auto prev = node->prev;
            auto next = node->next;
            insertNonFull(node, key);
            if (node->keys.size() >= ORDER_) splitNode(node, key);
        }
    }

    template <typename T>
    Node<T>* BPlusTree<T>::findLeafNode(Node<T>* node, T key) {
        while (!node->isLeaf) {
            /**
             * std::upper_bound returns an iterator pointing to the first 
             * element in a sorted range that is greater than a specified value.
             */
            auto it = std::upper_bound(node->keys.begin(), node->keys.end(), key);
            int index = std::distance(node->keys.begin(), it);
            node = node->children[index];
        }
        return node;
    }
    
    template <typename T>
    void BPlusTree<T>::insertNonFull(Node<T>* node, T key) {
        /**
         * std::lower_bound returns an iterator pointing to the first element in a sorted 
         * range that is not less than a specified value.
         */
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        node->keys.insert(it, key);
    }

    template <typename T>
    void BPlusTree<T>::printBPlusTree() {
        if (!rootPtr) return;
        std::cout << "------Printing Tree------\n";
        std::cout << "B+ Tree:" << std::endl;
        printNode(rootPtr);
        std::cout << std::endl;
    }

    template <typename T>
    void printNode(Node<T>* node) {
        std::cout << "{";
        for (auto key : node->keys) std::cout << key << ", ";
        std::cout << " children: [";
        for (auto child : node->children) printNode(child);
        std::cout << "]";
        std::cout << "}";
    }

    template <typename T>
    void BPlusTree<T>::splitNode(Node<T>* node, T key) {
        auto new_node = new Node<T>(node->isLeaf);
        auto middle   = node->keys.size() / 2;
        auto mid_key  = node->keys[middle];

        auto node_key_middle = node->keys.begin() + middle;
        auto node_key_end    = node->keys.end();

        auto node_child_middle = node->children.begin() + middle;
        auto node_child_end  = node->children.end();

        if (node->isLeaf) {
            new_node->keys.insert(new_node->keys.begin(), node_key_middle, node_key_end);
            // new_node: right half
            node->keys.erase(node_key_middle, node_key_end);
            // node: left half
        } else { // internal node
            new_node->keys.insert(new_node->keys.begin(), node_key_middle+1, node_key_end);
            node->keys.erase(node_key_middle, node_key_end);
            new_node->children.insert(new_node->children.begin(), node_child_middle+1, node_child_end);
            node->children.erase(node_child_middle+1, node_child_end);

            for (auto child : new_node->children) child->parent = new_node;
        }
        
        if (!node->parent) { // current node is root, create a new root
            auto new_root = new Node<T>(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            
            node->parent = new_root;
            node->next = new_node;
            
            new_node->parent = new_root;
            new_node->prev = node;
            
            rootPtr = new_root;
            insertNonFull(new_root, mid_key);
        } else { // non-root: insert the middle key to the parent node
            auto parent = node->parent;
            auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
            parent->keys.insert(it, mid_key); 
            auto key_it = std::lower_bound(parent->keys.begin(), parent->keys.end(), new_node->keys[0]);
            int index = std::distance(parent->keys.begin(), key_it);
            parent->children.insert(parent->children.begin() + index + 1, new_node);
            new_node->parent = parent;
            
            new_node->prev = parent->children[index];
            if (index + 2 >= parent->children.size() || parent->children[index + 2] == nullptr)
                new_node->next = nullptr;
            else new_node->next = parent->children[index + 2];

            if (parent->children[index] != nullptr) parent->children[index]->next = new_node;
            if (index + 2 < parent->children.size() && parent->children[index + 2] != nullptr) parent->children[index + 2]->prev = new_node;
            // Check if the parent needs to be split

            if (parent->keys.size() >= ORDER_) splitNode(parent, key);
        }
    }

    template <typename T>
    std::optional<T> BPlusTree<T>::get(T key) {
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
    bool BPlusTree<T>::isHalfFull(Node<T>* node) {
        return node->keys.size() >= (ORDER_ / 2);
    }

    template <typename T>
    bool BPlusTree<T>::moreHalfFull(Node<T>* node) {
        return node->keys.size() > (ORDER_ / 2);
    }

    template <typename T>
    bool BPlusTree<T>::remove(T key) {
        if (!rootPtr) {return false;}
        Node<T>* node = findLeafNode(rootPtr, key);
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
    void BPlusTree<T>::removeBorrow(Node<T> *node) {
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
                    printf("internal node borrow from left\n");
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
                    printf("leaf node borrow from left\n");
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
                    printf("internal node borrow from right\n");
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
                    printf("leaf node borrow from right\n");
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
    void BPlusTree<T>::removeMerge(Node<T>* node) {
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
            printf("merge with left\n");
            assert(node->prev->keys.size() > 0);
            assert(node->prev->keys.size() + node->keys.size() < ORDER_);

            if (!node->isLeaf) { // if it is internal node, also needs to merge children
                printf("merge with left -- internal node\n");
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
                printf("calling remove borrow from remove merge with left\n");
                removeBorrow(parent);
            }
            
        } else { // merge with right
            printf("merge with right\n");
            assert(node->next != nullptr); // Impossible!
            assert(node->next->keys.size() > 0);
            assert(node->keys.size() + node->next->keys.size() < ORDER_);
            
            if (!node->isLeaf) { 
                // if it is internal node, also needs to merge children
                printf("merge with right -- internal node\n");
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
    bool BPlusTree<T>::removeFromLeaf(Node<T>* node, T key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
            return true;
        }
        return false;
    }
    };

template class Tree::BPlusTree<int>;
