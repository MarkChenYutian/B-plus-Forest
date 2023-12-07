#include "tree.h"

namespace Tree {
    template <typename T>
    void LockManager<T>::retrieveLock(FineNode<T> *ptr) {
        if (isShared) ptr->latch.lock_shared();
        else ptr->latch.lock();
        nodes[end] = ptr;
        end ++;
    }

    template <typename T>
    bool LockManager<T>::isLocked(FineNode<T> *ptr) {
        for (size_t idx = start; idx < end; idx ++) {
            if (nodes[idx] == ptr) return true;
        };
        return false;
    }

    template <typename T>
    void LockManager<T>::releaseAll() {
        while (start != end) {
            if (nodes[start] != nullptr) {
                if (isShared) nodes[start]->latch.unlock_shared();
                else nodes[start]->latch.unlock();
            }
            start ++;
        }
    }

    template <typename T>
    void LockManager<T>::releasePrev() {
        while ((end - start) > 1) {
            if (nodes[start] != nullptr) {
                if (isShared) nodes[start]->latch.unlock_shared();
                else nodes[start]->latch.unlock();
            }
            start++;
        }
    }

    template <typename T>
    void LockManager<T>::popAndDelete(FineNode<T> *ptr) {
        for (size_t idx = start; idx < end; idx ++) {
            if (nodes[idx] == ptr) {
                nodes[idx] = nullptr;
                delete ptr;
                return;
            }
        }
    }
};
