#include <algorithm>
#include <iostream>
#include <vector>
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
    BPlusTree<T>::~BPlusTree() {
        if (rootPtr == nullptr) return;
        recursive_clean(rootPtr);
    }

    template <typename T>
    void BPlusTree<T>::insert(T key) {
        if (rootPtr == nullptr) {
            rootPtr = new Tree::Node<T>(true);
            rootPtr->keys.push_back(key);
        } else {
            auto node = findLeafNode(rootPtr, key);
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
        // std::cout << "Root: ";
        // for (int key : rootPtr->keys) std::cout << key << " ";
        // std::cout << std::endl;
        // std::cout << "Children: ";
        // for (Node<T>* child : rootPtr->children) {
        //     for (int key : child->keys) std::cout << key << " ";
        //     std::cout << "(";
        //     for ()
        //     std::cout << ")";
        //     std::cout << "| ";
        // }
        // std::cout << std::endl;
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
        printf("\n-----splitNode begin-----\n");
        auto new_node = new Node<T>(node->isLeaf);
        int middle    = node->keys.size() / 2;
        auto mid_key  = node->keys[middle];

        auto node_key_middle = node->keys.begin() + middle;
        auto node_key_end    = node->keys.end();

        auto node_child_middle = node->children.begin() + middle;
        auto node_child_end  = node->children.end();

        if (node->isLeaf) {
            new_node->keys.insert(new_node->keys.begin(), node_key_middle, node_key_end);
            node->keys.erase(node_key_middle, node_key_end);
            // root
            // non-root
        } else { // TODO root can also be internal node
            new_node->keys.insert(new_node->keys.begin(), node_key_middle+1, node_key_end);
            node->keys.erase(node_key_middle, node_key_end);
            
            printf("internal node!!!!!\n");
            new_node->children.insert(new_node->children.begin(), node_child_middle+1, node_child_end);
            node->children.erase(node_child_middle+1, node_child_end);

            for (auto child : new_node->children) child->parent = new_node;

            std::cout << "print node\n";
            printNode(node);
            std::cout << "\nprint new node\n";
            printNode(new_node);
            std::cout << "\n";
        }

        // Move half of the keys to the new node (right node)
        

        // If it's an internal node, move half of the children as well
        
        // Insert the middle key to the parent
        if (!node->parent) {
            printf("insert the middle key to the parent -- current node is root\n");
            // If the current node is the root, create a new root
            auto new_root = new Node<T>(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            node->parent = new_root;
            new_node->parent = new_root;
            rootPtr = new_root;
            printf("new_root must be empty now\n");
            insertNonFull(new_root, mid_key);
        } else {
            printf("insert the middle key to the parent -- current node is leaf\n");
            // Otherwise, insert the middle key to the parent node
            auto parent = node->parent;
            auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
            parent->keys.insert(it, mid_key); // parent insert not key but mid_key



            auto key_it = std::lower_bound(parent->keys.begin(), parent->keys.end(), new_node->keys[0]);
            int index = std::distance(parent->keys.begin(), key_it);
            parent->children.insert(parent->children.begin() + index + 1, new_node);
            new_node->parent = parent;

            // Check if the parent needs to be split
            if (parent->keys.size() >= ORDER_) {
                this->printBPlusTree();
                splitNode(parent, key);
            }
        }
    }
    // template <typename T>
    // void ~BPlusTree<T>() {
        
    // }

    // template <typename T>
    // class BPlusTree {
    // public:

        /*
        void remove(int key) {
            if (!rootPtr) {
                return;
            }
            Node* node = findLeafNode(rootPtr, key);
            removeFromLeaf(node, key);
            if (node->keys.size() < ORDER_ / 2) {
                // 
            }
        }
        std::optional<int> get(int key) {
            if (!rootPtr) {
                return std::nullopt;
            }
            Node* node = findLeafNode(rootPtr, key);
            return getFromLeafNode(node, key);
        }
        
        */

    // private:
        /*
        // Private helper functions
        
        
        
        void removeFromLeaf(Node* node, int key) {
            auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
            if (it != node->keys.end() && *it == key) {
                node->keys.erase(it);
            }
        }
        void removeFromNonLeaf(Node* node, int key) { //////////////////
            // Find the child which contains the key
            auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
            int index = std::distance(node->keys.begin(), it);

            // Recursive call to remove the key from the child
            Node* child = node->children[index];
            if (child->keys.size() >= ORDER_ / 2) { // remove from at least hall-full node
                remove(key); // TODO: this is wrong, shouldn't call remove
            } else {
                // TODO: Implement merge or redistribution with siblings

            }

        }
        std::optional<int> getFromLeafNode(Node* node, int key) {
            // Find the child which contains the key
            auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
            int index = std::distance(node->keys.begin(), it);

            if (index < node->keys.size() && node->keys[index] == key) {
                return key; // Key found in this node
            } 
            return std::nullopt; // Key not found
        }
        */
    };

template class Tree::BPlusTree<int>;
