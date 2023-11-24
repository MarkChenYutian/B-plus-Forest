#include <cassert>
#include <optional>
#include "tree.h"

namespace Tree {
    template <typename T>
    void SeqNode<T>::releaseAll() {
        if (!isLeaf) {
            for (auto child : children) child->releaseAll();
        }
        delete this;
    }

    template <typename T>
    T getMin(SeqNode<T>* node) {
        while (!node->isLeaf) node = node->children[0];
        return node->keys[0];
    }

    template <typename T>
    void SeqNode<T>::rebuild() {
        size_t key_len_before = keys.size();
        auto old_children = children;
        children = std::deque<SeqNode<T>*>();
        

        auto left_most_prev = old_children[0] -> prev;
        auto right_most_next = old_children.back() -> next;

        for (SeqNode<T>* child : old_children) {
            if (child->keys.size() == 0) delete child;
            else children.push_back(child);
        }        


        for (int i = 0; i < children.size(); i ++) {
            if (i == 0) {
                children[i] -> prev = left_most_prev;
                if (left_most_prev != nullptr) left_most_prev -> next = children[i];
            }
            else children[i] -> prev = children[i - 1];

            if (i == children.size() - 1) {
                children[i] -> next = right_most_next;
                if (right_most_next != nullptr) right_most_next -> prev = children[i];
            }
            else children[i] -> next = children[i + 1];
        }

        keys.clear();
        for (int i = 1; i < children.size(); i++) {
            keys.push_back(getMin(children[i]));
        }

        consolidateChild();
    }

    template <typename T>
    bool SeqNode<T>::debug_checkParentPointers() {
        for (Tree::SeqNode<T>* child : children) {
            if (child->parent != this) 
                return false;
            if (!(child->isLeaf || child->debug_checkParentPointers())) 
                return false;
        }

        return  !(this->parent != nullptr && this->parent->children[childIndex] != this);
    }
    
    template <typename T>
    void SeqNode<T>::printKeys() {
        std::cout << "[";
        for (int i = 0; i < keys.size(); i ++) {
            std::cout << keys[i];
            if (i != keys.size() - 1) std::cout << ",";
        }
        std::cout << "]";
    }

    template <typename T>
    bool SeqNode<T>::debug_checkOrdering(std::optional<T> lower, std::optional<T> upper) {
        for (const auto key : this->keys) {
            if (lower.has_value() && key < lower.value()) {                
                return false;
            }
            if (upper.has_value() && key >= upper.value()) {
                return false;
            }
        }
        // if parent->prev == nullptr lower = null
        // else lower = parent->prev->last key
        if (!this->isLeaf) {
            for (int i = 0; i < this->children.size(); i ++) {\
                if (i == 0) {
                    if (!this->children[i]->debug_checkOrdering(lower, this->keys[0])) 
                        return false;
                } else if (i == this->children.size() - 1) {
                    if (!this->children[i]->debug_checkOrdering(this->keys.back(), upper))
                        return false;
                } else {
                    if (!this->children[i]->debug_checkOrdering(this->keys[i - 1], this->keys[i]))
                        return false;
                }
            }
        }
        return true;
    }
    
    template <typename T>
    bool SeqNode<T>::debug_checkChildCnt(int ordering) {
        if (this->isLeaf) {
            return this->children.size() == 0;
        }
        if (this->keys.size() <= 0) return false;
        if (this->keys.size() >= ordering) return false;
        if (this->children.size() != this->keys.size() + 1) return false;
        for (auto child : this->children) {
            bool childIsValid = child->debug_checkChildCnt(ordering);
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
