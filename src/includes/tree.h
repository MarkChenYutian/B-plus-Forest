#ifndef TREE_H
#define TREE_H

#include <iostream>
#include <algorithm>
#include <deque>
#include <memory>

namespace Tree {
    template <typename T>
    struct Node {
        bool isLeaf;
        Node<T>* parent;
        std::deque<T> keys;
        std::deque<Node<T>*> children;
        Node<T>* next;
        Node<T>* prev;
        Node(bool leaf) : isLeaf(leaf), parent(nullptr), next(nullptr), prev(nullptr) {};
        void rebuild();
        void debug_checkParentPointers();
        void debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        void debug_checkChildCnt(int ordering);
        void printKeys();
    };


    template<typename T>
    class SeqBPlusTree {
        private:
            Node<T>* rootPtr;
            int ORDER_;

        public:
            SeqBPlusTree(int order = 3);
            ~SeqBPlusTree();

            // Getter
            Node<T>* getRoot();
            void debug_assertIsValid();

            // Public Tree API
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);

        private:
            // Private helper functions
            Node<T>* findLeafNode(Node<T>* node, T key);
            void splitNode(Node<T>* node, T key);
            void insertNonFull(Node<T>* node, T key);
            bool removeFromLeaf(Node<T>* node, T key);

            bool isHalfFull(Node<T>* node);
            bool moreHalfFull(Node<T>* node);

            void removeBorrow(Node<T>* node);
            void removeMerge(Node<T>* node);
    };
}

#endif
