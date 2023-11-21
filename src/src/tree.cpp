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
    void BPlusTree<T>::remove(T key) {
        if (!rootPtr) {return;}
        Node<T>* node = findLeafNode(rootPtr, key);
        if (!removeFromLeaf(node, key)) return;
        
        if (node == rootPtr && node->keys.size() == 0) {
            // ------TODO: add comment
            rootPtr = nullptr;
            return;
        }

        if (!isHalfFull(node)) removeBorrow(node);
        else if (node->parent != nullptr) node->parent->rebuild();
    }

    template <typename T>
    void BPlusTree<T>::removeBorrow(Node<T> *node) {
        // check if neighbors > half full => borrow 
        // if fails, need to merge
        if (node->prev != nullptr && moreHalfFull(node->prev)) { 
            // brow from prev (Left -> right)
            T keyBorrow = node->prev->keys.back();
            node->keys.push_front(keyBorrow);
            node->prev->keys.pop_back();
            updateKeyToLCA(node->prev, node, true);
            // TODO: borrow children?
        } else if (node->next != nullptr && moreHalfFull(node->next)) {
            // brow from next (Right -> left)
            T keyBorrow = *(node->next->keys.begin());
            node->keys.push_back(keyBorrow);
            node->next->keys.pop_front();
            updateKeyToLCA(node, node->next, false);
            // TODO: borrow children?
        } else {
            removeMerge(node);
        }
    }

    template <typename T>
    void BPlusTree<T>::removeMerge(Node<T>* node) {
        if (node->prev != nullptr) { // merge with left
            assert(node->prev->keys.size() > 0);
            T lkey = node->prev->keys.back();
            node->prev->keys.insert(node->prev->keys.end(), node->keys.begin(), node->keys.end());
            
            node->keys.clear();

            node->parent->rebuild();
            // if (!node->isLeaf) {
                // assert(false);
                // node->prev->children.insert(node->prev->children.end(), node->children.begin(), node->children.end()); ///????
                // TODO: if it is internal node, also needs to merge children
            // }

            // TODO: parent need borrow? call removeBorrow()
        } else { // merge with right
            assert(node->next != nullptr); // Impossible!

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
