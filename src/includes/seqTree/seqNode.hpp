#pragma once
#include <cassert>
#include <optional>
#include "../tree.h"

namespace Tree {
    template <typename T>
    void SeqNode<T>::releaseAll() {
        if (!isLeaf) {
            for (auto child : children) child->releaseAll();
        }
        delete this;
    }

    template <typename T>
    bool SeqNode<T>::debug_checkParentPointers() {
        for (Tree::SeqNode<T>* child : children) {
            if (child->parent != this) 
                return false;
            if (!(child->isLeaf || child->debug_checkParentPointers())) 
                return false;
        }

        return this->parent->children[childIndex] == this;
    }
    
    template <typename T>
    void SeqNode<T>::printKeys() {
        std::cout << "[";
        std::cout << childIndex << "|";
        for (int i = 0; i < numKeys(); i ++) {
            std::cout << keys[i];
            if (i != numKeys() - 1) std::cout << ",";
        }
        std::cout << "]";
    }

    template <typename T>
    bool SeqNode<T>::debug_checkOrdering(std::optional<T> lower, std::optional<T> upper) {
        for (const auto key : this->keys) {
            if (lower.has_value() && key < lower.value()) {                
                std::cout << "\033[1;31m FAILED lower has value:" << lower.value() << " ";
                this->printKeys();
                std::cout << "\033[0m" << std::endl;
                return false;
            }
            if (upper.has_value() && key >= upper.value()) {
                std::cout << "\033[1;31m FAILED upper has value:" << upper.value() << " ";
                this->printKeys();
                std::cout << "\033[0m" << std::endl;
                return false;
            }
        }
        // if parent->prev == nullptr lower = null
        // else lower = parent->prev->last key
        if (!this->isLeaf) {
            for (int i = 0; i < numChild(); i ++) {\
                if (i == 0) {
                    if (!this->children[i]->debug_checkOrdering(lower, this->keys[0])) {
                        std::cout << "\033[1;31m FAILED i == 0";
                        this->printKeys();
                        std::cout << "\033[0m" << std::endl;
                        return false;
                    }
                } else if (i == numChild() - 1) {
                    if (!this->children[i]->debug_checkOrdering(this->keys.back(), upper)) {
                        std::cout << "\033[1;31m FAILED i == numChild() - 1";
                        this->printKeys();
                        std::cout << "\033[0m" << std::endl;
                        return false;
                    }
                } else {
                    if (!this->children[i]->debug_checkOrdering(this->keys[i - 1], this->keys[i])) {
                        std::cout << "\033[1;31m FAILED else";
                        this->printKeys();
                        std::cout << "\033[0m" << std::endl;
                        return false;
                    }
                }
            }
        }
        return true;
    }
    
    template <typename T>
    bool SeqNode<T>::debug_checkChildCnt(int order, bool allowEmpty) {
        if (isLeaf && !allowEmpty) {
            return numChild() == 0 && numKeys() >= ((order-1)/2);
        } else if (isLeaf) {
            return numChild() == 0 && (numKeys() == 0 || numKeys() >= ((order-1)/2));
        }

        if (numKeys() <= 0) return false;
        if (numKeys() >= order) return false;
        if (numChild() != numKeys() + 1) return false;
        for (auto child : this->children) {
            bool childIsValid = child->debug_checkChildCnt(order, allowEmpty);
            if (!childIsValid) return false;
        }
        return true;
    }

    template <typename T>
    void SeqNode<T>::consolidateChild() {
        for (size_t id = 0; id < numChild(); id ++) {
            children[id]->parent = this;
            children[id]->childIndex = id;
        }
    }
}
