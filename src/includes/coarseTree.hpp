#include <optional>
#include "tree.h"

namespace Tree {
    template <typename T>
    CoarseLockBPlusTree<T>::CoarseLockBPlusTree(int order) {
        tree = SeqBPlusTree<T>(order);
    }

    template <typename T>
    CoarseLockBPlusTree<T>::~CoarseLockBPlusTree() {}

    template <typename T>
    std::optional<T> CoarseLockBPlusTree<T>::get(T key) {
        std::lock_guard<std::mutex> guard(lock);
        return tree.get(key);
    }

    template <typename T>
    void CoarseLockBPlusTree<T>::insert(T key) {
        std::lock_guard<std::mutex> guard(lock);
        tree.insert(key);
    }

    template <typename T>
    bool CoarseLockBPlusTree<T>::remove(T key) {
        std::lock_guard<std::mutex> guard(lock);
        return tree.remove(key);
    }

    template <typename T>
    void CoarseLockBPlusTree<T>::print() {
        std::lock_guard<std::mutex> guard(lock);
        tree.print();
    }

    template <typename T>
    int CoarseLockBPlusTree<T>::size() {
        return size;
    }

    template <typename T>
    bool CoarseLockBPlusTree<T>::debug_checkIsValid(bool verbose) {
        std::lock_guard<std::mutex> guard(lock);
        return tree.debug_checkIsValid(verbose);
    }

    template <typename T>
    std::vector<T> CoarseLockBPlusTree<T>::toVec() {
        std::lock_guard<std::mutex> guard(lock);
        return tree.toVec();
    }
}
