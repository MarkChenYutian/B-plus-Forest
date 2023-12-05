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
#include "mpi.h"

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
            bool debug_checkIsValid(bool verbose);
            int  size();
            
            void insert(T key);
            bool remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
    };

    template <typename T>
    /**
     * NOTE: A tree node for sequential version of B+ tree
     */
    struct SeqNode {
        bool isLeaf;                       // Check if node is leaf node
        bool isDummy;                      // Check if node is dummy node
        int  childIndex;                   // Which child am I in parent? (-1 if no parent)
        std::deque<T> keys;                // Keys
        std::deque<SeqNode<T>*> children;  // Children
        SeqNode<T>* parent;                // Pointer to parent node
        SeqNode<T>* next;                  // Pointer to left sibling
        SeqNode<T>* prev;                  // Pointer to right sibling

        SeqNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr), childIndex(-1) {};
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

    template <typename T>
    /**
     * NOTE: A tree node for sequential version of B+ tree
     */
    struct FreeNode {
        bool isLeaf;                       // Check if node is leaf node
        bool isDummy;                      // Check if node is dummy node
        int  childIndex;                   // Which child am I in parent? (-1 if no parent)
        std::deque<T> keys;                // Keys
        std::deque<FreeNode<T>*> children; // Children
        FreeNode<T>* parent;               // Pointer to parent node
        FreeNode<T>* next;                 // Pointer to left sibling
        FreeNode<T>* prev;                 // Pointer to right sibling
        std::atomic<int> occupy_flag;      // An atomic CAS flag for modifying next,prev pointers.

        FreeNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr), childIndex(-1), occupy_flag(0) {};
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
     * NOTE: A tree node for finegrained locked version of B+ tree
     */
    template <typename T>
    struct FineNode {
        std::shared_mutex latch;

        bool isLeaf;                        // Check if node is leaf node
        bool isDummy;                       // Check if node is dummy node
        int childIndex;                     // Which child am I in parent? (-1 if no parent)
        std::deque<T> keys;                 // Keys
        std::deque<FineNode<T>*> children;  // Children
        FineNode<T>* parent;                // Pointer to parent node
        FineNode<T>* next;                  // Pointer to left sibling
        FineNode<T>* prev;                  // Pointer to right sibling

        FineNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr) {};

        void printKeys();
        void releaseAll();
        void consolidateChild();
        bool debug_checkParentPointers();
        bool debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        bool debug_checkChildCnt(int ordering);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        inline size_t getGtKeyIdx(T key) {
            size_t index = 0;
            while (index < numKeys() && keys[index] <= key) index ++;
            return index;
        }
    };

    /**
     * NOTE: A tree node used by the distributed version of B+ tree
    */
    template <typename T>
    struct DistriNode {
        std::shared_mutex latch;

        bool isLeaf;                        // Check if node is leaf node
        bool isDummy;                       // Check if node is dummy node
        int childIndex;                     // Which child am I in parent? (-1 if no parent)
        std::deque<T> keys;                 // Keys
        std::deque<DistriNode<T>*> children;  // Children
        DistriNode<T>* parent;                // Pointer to parent node
        DistriNode<T>* next;                  // Pointer to left sibling
        DistriNode<T>* prev;                  // Pointer to right sibling

        DistriNode(bool leaf, bool dummy=false) : isLeaf(leaf), isDummy(dummy), parent(nullptr), next(nullptr), prev(nullptr) {};
        
        void printKeys();
        void releaseAll();
        void consolidateChild();
        bool debug_checkParentPointers();
        bool debug_checkOrdering(std::optional<T> lower, std::optional<T> upper);
        bool debug_checkChildCnt(int ordering);

        inline size_t numKeys()  {return keys.size();}
        inline size_t numChild() {return children.size();}
        inline size_t getGtKeyIdx(T key) {
            size_t index = 0;
            while (index < numKeys() && keys[index] <= key) index ++;
            return index;
        }
    };

    /**
     * NOTE: A datastructure used to keep track of the locks retrieved by
     * a single thread.
     * **Only used as a private variable within each thread, never share to others!**
     */
    template <typename T>
    struct LockDeque {
        bool isShared;
        std::deque<FineNode<T>*> nodes;
        
        LockDeque(bool isShared = false): isShared(isShared){}
        void retrieveLock(FineNode<T> *ptr) {
            if (isShared) ptr->latch.lock_shared();
            else ptr->latch.lock();
            nodes.push_back(ptr);
        }
        bool isLocked(FineNode<T> *ptr) {
            for (const auto node : nodes) {
                if (node == ptr) return true;
            }
            return false;
        }
        void releaseAllWriteLocks() {
            assert (!isShared);
            while (!nodes.empty()) {
                nodes.front()->latch.unlock();
                nodes.pop_front();
            }
        }
        void releasePrevWriteLocks() {
            assert (!isShared);
            while (nodes.size() > 1) {
                nodes.front()->latch.unlock();
                nodes.pop_front();
            }
        }
        void releaseAllReadLocks() {
            assert (isShared);
            while (!nodes.empty()) {
                nodes.front()->latch.unlock_shared();
                nodes.pop_front();
            }
        }
        void releasePrevReadLocks() {
            assert (isShared);
            while (nodes.size() > 1) {
                nodes.front()->latch.unlock_shared();
                nodes.pop_front();
            }
        }
        void popAndDelete(FineNode<T> *ptr) {
            DBG_ASSERT(!isShared);
            DBG_ASSERT(isLocked(ptr->parent));
            for (size_t idx = 0; idx < nodes.size(); idx ++) {
                if (nodes[idx] == ptr) {
                    nodes.erase(nodes.begin() + idx);
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

    template<typename T>
    class DistriBPlusTree {
        private:
            int ORDER_;
            int RANK_;
            int NUM_PROC_;
            MPI_Comm WORLD_;
            FineLockBPlusTree<T> internalTree;
        
        public:
            DistriBPlusTree(int order, MPI_Comm world);
            ~DistriBPlusTree();

            bool debug_checkIsValid(bool verbose);
            int size();

            void insert(T key);
            void remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
        
        private:
            /**
             * GET, REMOVE - literally get and remove request
             * STOP - Tell other distributed trees "I'm going to terminate"
             * */
            enum RequestType {GET, REMOVE, STOP, ACK};
            struct Tree_Request {
                int src_rank;
                RequestType op;
                T key;
            };

            struct Tree_Result {
                std::optional<T> result;
            };

            struct Background_Args {
                FineLockBPlusTree<T> *internalTree;
                MPI_Comm world;
                std::atomic<bool> *is_terminate;
                MPI_Datatype TREE_REQUEST;
                MPI_Datatype TREE_RESULT;
                int numProc;
                int rank;
            };

            MPI_Datatype TREE_REQUEST;
            MPI_Datatype TREE_RESULT;
            pthread_t bg_thread;
            std::atomic<bool> is_terminate;
        
        private:
            static void *MPI_background(void *args);
    };
};
