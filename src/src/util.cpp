#include <cassert>
#include <optional>
#include "tree.h"

namespace Tree {
    template <typename T>
    void Node<T>::rebuild() {
        size_t key_len_before = keys.size();
        auto old_children = children;
        children = std::deque<Node<T>*>();

        auto left_most_prev = old_children[0] -> prev;
        auto right_most_next = old_children.back() -> next;

        for (Node<T>* child : old_children) {
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
            keys.push_back(children[i]->keys[0]);
        }
        // assert(key_len_before == keys.size());
    }

    template <typename T>
    void Node<T>::debug_checkParentPointers() {
        for (Tree::Node<T>* child : children) {
            assert(child->parent == this);
            if (!child->isLeaf) child->debug_checkParentPointers();
        }
    }

    template <typename T>
    void BPlusTree<T>::updateKeyToLCA(Node<T>* left, Node<T>* right, bool isLeftToRight) {
        T lkey = left->keys.back(), rkey = right->keys.front();
        while (left->parent != right->parent) {
            left = left->parent;
            right = right->parent;
        }
        Node<T>* lca = left->parent;
        int index = 0;

        if (isLeftToRight) {
            while (index < lca->keys.size() && lca->keys[index] <= rkey) {index ++;}
            lca->keys[index] = rkey;
        } else {
            while (index < lca->keys.size() && lca->keys[index] < lkey) {index ++;}
            lca->keys[index] = rkey;
        }
    }

    template <typename T>
    void Node<T>::printKeys() {
        std::cout << "[";
        for (const auto key : this->keys) std::cout << key << ", ";
        std::cout << "]";
    }

    template <typename T>
    void Node<T>::debug_checkOrdering(std::optional<T> lower, std::optional<T> upper) {
        for (const auto key : this->keys) {
            if (lower.has_value()) assert(key >= lower.value());
            if (upper.has_value()) assert(key < upper.value());
        }
        if (!this->isLeaf) {
            for (int i = 0; i < this->children.size(); i ++) {\
                if (i == 0) {
                    this->children[0]->debug_checkOrdering(lower, this->keys[0]);
                } else if (i == this->children.size() - 1) {
                    this->children.back()->debug_checkOrdering(this->keys.back(), upper);
                } else {
                    this->children[i]->debug_checkOrdering(this->keys[i - 1], this->keys[i]);
                }
            }
        }
    }
    
    template <typename T>
    void Node<T>::debug_checkChildCnt(int ordering) {
        if (this->isLeaf) {
            assert(this->children.size() == 0);
            return;
        }
        assert (this->keys.size() > 0);
        assert (this->keys.size() < ordering);
        assert (this->children.size() == this->keys.size() + 1);
        for (auto child : this->children) {
            child->debug_checkChildCnt(ordering);
        }
    }
}

template class Tree::Node<int>;
template class Tree::BPlusTree<int>;
