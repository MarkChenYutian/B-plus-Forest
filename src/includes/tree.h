#ifndef TREE_H
#define TREE_H

#include <iostream>
#include <algorithm>
#include <vector>
#include <memory>

namespace Tree {
    template <typename T>
    struct Node {
        bool isLeaf;
        Node<T>* parent;
        std::vector<T> keys;
        std::vector<Node<T>*> children;
        Node(bool leaf) : isLeaf(leaf), parent(nullptr) {}
    };


    template<typename T>
    class BPlusTree {
        private:
            Node<T>* rootPtr;
            int ORDER_;

        public:
            BPlusTree(int order);
            ~BPlusTree();

            // Public API
            void insert(T key);
            void remove(T key);
            int  get(T key);
            void printBPlusTree();

        private:
            // Private helper functions
            Node<T>* findLeafNode(Node<T>* node, T key);
            void splitNode(Node<T>* node, T key);
            void insertNonFull(Node<T>* node, T key);
            void removeFromLeaf(Node<T>* node, T key);
            void removeFromNonLeaf(Node<T>* node, T key);
            int getFromLeafNode(Node<T>* node, T key, bool &isValid);
    };
}

#endif
