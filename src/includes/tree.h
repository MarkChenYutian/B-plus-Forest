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
#include <emmintrin.h>

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

constexpr size_t simdWidth = sizeof(__m128i) / sizeof(int);

namespace Tree {


    template <typename T>
    class ITree {
        public:
            bool debug_checkIsValid(bool verbose);
            int  size();
            
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
    };

    template <typename T>
    struct EfficientKeyFinder {
        static inline size_t getGtKeyIdxSpecialized(const std::vector<T> &keys, T key) {
            size_t index = 0, numKey = keys.size();
            while (index < numKey && keys[index] <= key) index ++;
            return index;
        }
    };

    template <>
    struct EfficientKeyFinder<int> {
        static inline size_t getGtKeyIdxSpecialized(const std::vector<int> &keys, int key) {
            size_t index = 0;
            size_t numKeys = keys.size();
            __m128i keyVector = _mm_set1_epi32(key);

            while (index + simdWidth < numKeys) {
                __m128i dataVector = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&keys[index]));
                __m128i cmpResult = _mm_cmpgt_epi32(dataVector, keyVector);
                int mask = _mm_movemask_epi8(cmpResult);
                if (mask != 0) {
                    // Find the exact element using scalar comparison
                    for (size_t i = 0; i < simdWidth; ++i) {
                        if (keys[index + i] > key) {
                            return index + i;
                        }
                    }
                }
                index += simdWidth;
            }

            // Handle any remaining elements
            while (index < numKeys && keys[index] <= key) index++;
            return index;
        }
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
            return EfficientKeyFinder<T>::getGtKeyIdxSpecialized(keys, key);
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
            return EfficientKeyFinder<T>::getGtKeyIdxSpecialized(keys, key);
        }
    };

    /**
     * NOTE: A data structure used to keep track of the locks retrieved by
     * a single thread.
     * **Only used as a private variable within each thread, never share to others!**
     */
    constexpr size_t LockQueueMaxSize = 20;

    template <typename T>
    struct LockDeque {
        bool isShared;
        FineNode<T> *nodes[LockQueueMaxSize];
        size_t start = 0, end = 0;

        LockDeque(bool isShared = false): isShared(isShared){}
        void retrieveLock(FineNode<T> *ptr) {
            if (isShared) ptr->latch.lock_shared();
            else ptr->latch.lock();
            nodes[end] = ptr;
            end ++;
        }
        bool isLocked(FineNode<T> *ptr) {
            for (size_t idx = start; idx < end; idx ++) {
                if (nodes[idx] == ptr) return true;
            };
            return false;
        }
        void releaseAll() {
            while (start != end) {
                if (nodes[start] != nullptr) {
                    if (isShared) nodes[start]->latch.unlock_shared();
                    else nodes[start]->latch.unlock();
                }
                start ++;
            }
        }
        void releasePrev() {
            while ((end - start) > 1) {
                if (nodes[start] != nullptr) {
                    if (isShared) nodes[start]->latch.unlock_shared();
                    else nodes[start]->latch.unlock();
                }
                start++;
            }
        }
        void popAndDelete(FineNode<T> *ptr) {
            DBG_ASSERT(!isShared);
            DBG_ASSERT(isLocked(ptr->parent));
            for (size_t idx = start; idx < end; idx ++) {
                if (nodes[idx] == ptr) {
                    nodes[idx] = nullptr;
                    break;
                }
            }
            delete ptr;
        }
    };

    template<typename T>
    class SeqBPlusTree : public ITree<T> {
        private:
            SeqNode<T> rootPtr;
            int ORDER_;
            int size_;

        public:
            SeqBPlusTree(int order = 3);
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
            CoarseLockBPlusTree(int order = 3);
            ~CoarseLockBPlusTree();
            bool debug_checkIsValid(bool verbose);
            int  size();
            
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
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
            FineNode<T>* findLeafNodeInsert(FineNode<T>* node, T key, LockDeque<T> &dq);
            FineNode<T>* findLeafNodeDelete(FineNode<T>* node, T key, LockDeque<T> &dq);
            FineNode<T>* findLeafNodeRead(FineNode<T>* node, T key, LockDeque<T> &dq);

            void splitNode(FineNode<T>* node, T key);
            void insertKey(FineNode<T>* node, T key);
            bool removeFromLeaf(FineNode<T>* node, T key);

            bool isHalfFull(FineNode<T>* node);
            bool moreHalfFull(FineNode<T>* node);

            void removeBorrow(FineNode<T>* node, LockDeque<T> &dq);
            void removeMerge(FineNode<T>* node, LockDeque<T> &dq);
    };
};
