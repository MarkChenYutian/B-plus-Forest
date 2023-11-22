#ifndef TREE_H
#define TREE_H

#include <iostream>
#include <algorithm>
#include <deque>
#include <memory>

namespace Tree {
    template <typename T>
    /**
     * NOTE: A tree node for sequential version of B+ tree
     */
    struct SeqNode {
        bool isLeaf;                       // Check if node is leaf node
        int childIndex;                    // Which child am I in parent? (-1 if no parent)
        std::deque<T> keys;                // Keys
        std::deque<SeqNode<T>*> children;  // Children
        SeqNode<T>* parent;                // Pointer to parent node
        SeqNode<T>* next;                  // Pointer to left sibling
        SeqNode<T>* prev;                  // Pointer to right sibling

        SeqNode(bool leaf) : isLeaf(leaf), parent(nullptr), next(nullptr), prev(nullptr) {};
        /**
         * Regenerate the node's keys based on current child.
         * NOTE: SIDE_EFFECT - will delete empty children automatically!
         */
        void rebuild();
        void printKeys();
        void releaseAll();
        void consolidateChild();
        void debug_checkParentPointers();
        void debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        void debug_checkChildCnt(int ordering);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        /**
         * Return the index of first key that is greater than or equal to "key".
         * 
         * NOTE:
         * If the key is larger than all keys in node, will return an
         * **out-of-bound** index!
         */
        inline size_t getGeKeyIdx(T key) {
            size_t index = 0;
            while (index < numKeys() && keys[index] < key) index ++;
            return index;
        }
        inline size_t getGtKeyIdx(T key) {
            size_t index = 0;
            while (index < numKeys() && keys[index] <= key) index ++;
            return index;
        }
    };


    template<typename T>
    class SeqBPlusTree {
        private:
            SeqNode<T>* rootPtr;
            int ORDER_;

        public:
            SeqBPlusTree(int order = 3);
            ~SeqBPlusTree();

            // Getter
            SeqNode<T>* getRoot();
            void debug_assertIsValid(bool verbose);
            int  debug_numChild();

            // Public Tree API
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);

        private:
            // Private helper functions
            SeqNode<T>* findLeafNode(SeqNode<T>* node, T key);
            void splitNode(SeqNode<T>* node, T key);
            void insertKey(SeqNode<T>* node, T key);
            bool removeFromLeaf(SeqNode<T>* node, T key);

            bool isHalfFull(SeqNode<T>* node);
            bool moreHalfFull(SeqNode<T>* node);

            void removeBorrow(SeqNode<T>* node);
            void removeMerge(SeqNode<T>* node);
    };
}

#endif
