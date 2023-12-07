#pragma once

#include <iostream>
#include <algorithm>
#include <deque>
#include <mutex>
#include <atomic>
#include <shared_mutex>
#include <memory>
#include <optional>
#include <cassert>

#include "utility/SIMDOptimizer.h"


#ifdef DEBUG
std::mutex print_mutex;
#define DBG_PRINT(arg) \
    do { \
        std::lock_guard<std::mutex> lock(print_mutex); \
        arg; \
    } while (0);

#define DBG_ASSERT(arg) assert(arg);

#else

#define DBG_PRINT(arg) {}
#define DBG_ASSERT(arg) {}

#endif



namespace Tree {
    template <typename T>
    class ITree {
        public:
        virtual bool debug_checkIsValid(bool verbose) = 0;
        virtual int  size() = 0;
            
        virtual void insert(T key) = 0;
        virtual bool remove(T key) = 0;
        virtual void print() = 0;
        virtual std::optional<T> get(T key) = 0;
        virtual std::vector<T> toVec() = 0;
    };

    template <typename T>
    /**
     * NOTE: A tree node for sequential version of B+ tree
     */
    struct SeqNode {
        bool isLeaf;                       // Check if node is leaf node
        bool isDummy;                      // Check if node is dummy node
        int  childIndex;                   // Which child am I in parent? (-1 if no parent)
        std::vector<T> keys;               // Keys
        std::deque<SeqNode<T>*> children;  // Children
        SeqNode<T>* parent;                // Pointer to parent node
        SeqNode<T>* next;                  // Pointer to left sibling
        SeqNode<T>* prev;                  // Pointer to right sibling

        explicit SeqNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr), childIndex(-1) {};
        void printKeys();
        void releaseAll();
        void consolidateChild();
        bool debug_checkParentPointers();
        bool debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        bool debug_checkChildCnt(int ordering, bool allowEmpty=false);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        /**
         * Return the index of first key that is greater than or equal to "key".
         * 
         * NOTE:
         * If the key is larger than all keys in node, will return an
         * **out-of-bound** index!
         */
        inline size_t getGtKeyIdx(T key) {
            size_t index = 0;
            while (index < numKeys() && keys[index] <= key) index ++;
            return index;
        }
    };

    /**
     * NOTE: A tree node for sequential version of B+ tree
     */
    template <typename T>
    struct FreeNode {
        bool isLeaf;                       // Check if node is leaf node
        int  childIndex;                   // Which child am I in parent? (-1 if no parent)
        std::vector<T> keys;               // Keys
        std::deque<FreeNode<T>*> children; // Children
        FreeNode<T>* parent;               // Pointer to parent node
        FreeNode<T>* next;                 // Pointer to left sibling
        FreeNode<T>* prev;                 // Pointer to right sibling

        explicit FreeNode(bool leaf) : isLeaf(leaf), parent(nullptr), next(nullptr), prev(nullptr), childIndex(-1) {};
        void printKeys();
        void releaseAll();
        void consolidateChild();
        bool debug_checkParentPointers();
        bool debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        bool debug_checkChildCnt(int ordering, bool allowEmpty=false);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        /**
         * Return the index of first key that is greater than or equal to "key".
         * 
         * NOTE:
         * If the key is larger than all keys in node, will return an
         * **out-of-bound** index!
         */
        inline size_t getGtKeyIdx(T key) {
            return SIMDOptimizer<T>::getGtKeyIdxSpecialized(keys, key);
        }
    };

    /**
     * NOTE: A tree node for finegrained locked version of B+ tree
     */
    template <typename T>
    struct FineNode {
        std::shared_mutex latch;

        bool isLeaf;                        // Check if node is leaf node
        bool isDummy;                       // Check if node is dummy node
        int childIndex;                     // Which child am I in parent? (-1 if no parent)
        std::vector<T> keys;                // Keys
        std::deque<FineNode<T>*> children;  // Children
        FineNode<T>* parent;                // Pointer to parent node
        FineNode<T>* next;                  // Pointer to left sibling
        FineNode<T>* prev;                  // Pointer to right sibling

        explicit FineNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr), childIndex(-1) {};

        void printKeys();
        void releaseAll();
        void consolidateChild();
        bool debug_checkParentPointers();
        bool debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        bool debug_checkChildCnt(int ordering);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        inline size_t getGtKeyIdx(T key) {
            return SIMDOptimizer<T>::getGtKeyIdxSpecialized(keys, key);
        }
    };

    template<typename T>
    class SeqBPlusTree : public ITree<T> {
        private:
            SeqNode<T> rootPtr;
            int ORDER_;
            int size_;

        public:
            explicit SeqBPlusTree(int order = 3);
            ~SeqBPlusTree();

            // Getter
            SeqNode<T>* getRoot();
            bool debug_checkIsValid(bool verbose);
            int  size();

            // Public Tree API
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();

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

    template<typename T>
    class CoarseLockBPlusTree : public ITree<T> {
        private:
            std::mutex lock;
            SeqBPlusTree<T> tree;
        public:
            explicit CoarseLockBPlusTree(int order = 3);
            ~CoarseLockBPlusTree();
            bool debug_checkIsValid(bool verbose);
            int  size();
            
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
    };

    /*
     * To maximize the performance of fine grain lock algorithm, we use a fixed size array to store
     * lock pointers. 32 seems to be a very descent number here since it is hard to have tree with (order)^32 elements
     * where order >= 3.
     */
    constexpr size_t LockQueueMaxSize = 32;
    template <typename T>
    struct LockManager {
        bool isShared;
        FineNode<T> *nodes[LockQueueMaxSize];
        size_t start = 0, end = 0;

        explicit LockManager(bool isShared = false): isShared(isShared){}
        void retrieveLock(FineNode<T> *ptr);
        bool isLocked(FineNode<T> *ptr);
        void releaseAll();
        void releasePrev();
        void popAndDelete(FineNode<T> *ptr);
    };

    template<typename T>
    class FineLockBPlusTree : public ITree<T> {
        private:
            FineNode<T> rootPtr;
            int ORDER_;
            std::atomic<int> size_ = 0;
        
        public:
            FineLockBPlusTree(int order = 3);
            ~FineLockBPlusTree();
            bool debug_checkIsValid(bool verbose);
            int  size();
            
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
            FineNode<T> *getRoot();
        
        private:
            // Private helper functions
            FineNode<T>* findLeafNodeInsert(FineNode<T>* node, T key, LockManager<T> &dq);
            FineNode<T>* findLeafNodeDelete(FineNode<T>* node, T key, LockManager<T> &dq);
            FineNode<T>* findLeafNodeRead(FineNode<T>* node, T key, LockManager<T> &dq);

            void splitNode(FineNode<T>* node, T key);
            void insertKey(FineNode<T>* node, T key);
            bool removeFromLeaf(FineNode<T>* node, T key);

            bool isHalfFull(FineNode<T>* node);
            bool moreHalfFull(FineNode<T>* node);

            void removeBorrow(FineNode<T>* node, LockManager<T> &dq);
            void removeMerge(FineNode<T>* node, LockManager<T> &dq);
    };
};
