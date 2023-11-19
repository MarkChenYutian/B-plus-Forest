#include <iostream>
#include <vector>

class BPlusTree {
private:
    struct Node {
        bool isLeaf;
        std::vector<int> keys; // keys size is always <= ORDER
        std::vector<Node*> children;
        Node* parent;

        Node(bool leaf = false) : isLeaf(leaf), parent(nullptr) {}
    };

    Node* rootptr;
    int ORDER_;

public:
    BPlusTree(int ORDER = 3) : rootptr(nullptr), ORDER_(ORDER) {}

    // Public API
    void insert(int key) {
        if (!rootptr) {
            rootptr = new Node(true);
            rootptr->keys.push_back(key);
        } else {
            Node* node = findLeafNode(rootptr, key);
            insertNonFull(node, key);
            if (node->keys.size() >= ORDER_) { // TODO: >= ORDER_ or > ORDER_?
                splitNode(node, key);
            }
        }
    }
    void remove(int key) {
        if (!rootptr) {
            return;
        }
        Node* node = findLeafNode(rootptr, key);
        removeFromLeaf(node, key);
        if (node->keys.size() < ORDER_ / 2) {
            // 
        }
    }
    std::optional<int> get(int key) {
        if (!rootptr) {
            return std::nullopt;
        }
        Node* node = findLeafNode(rootptr, key);
        return getFromLeafNode(node, key);
    }
    void printBPlusTree() {
        if (!rootptr) {
            return;
        }
        std::cout << "B+ Tree:" << std::endl;
        std::cout << "Root: ";
        for (int key : rootptr->keys) {
            std::cout << key << " ";
        }
        std::cout << std::endl;
        std::cout << "Children: ";
        for (Node* child : rootptr->children) {
            for (int key : child->keys) {
                std::cout << key << " ";
            }
            std::cout << "| ";
        }
        std::cout << std::endl;
    }

private:
    // Private helper functions
    Node* findLeafNode(Node* node, int key) {
        while (!node->isLeaf) {
            auto it = std::upper_bound(node->keys.begin(), node->keys.end(), key);
            // std::upper_bound returns an iterator pointing to the first element in a sorted range that is greater than a specified value.
            int index = std::distance(node->keys.begin(), it);
            node = node->children[index];
        }
        return node;
    }
    void splitNode(Node* node, int key) {
        Node* new_node = new Node(node->isLeaf);
        int middle = node->keys.size() / 2;

        // Move half of the keys to the new node
        new_node->keys.insert(new_node->keys.begin(), node->keys.begin() + middle, node->keys.end());
        node->keys.erase(node->keys.begin() + middle, node->keys.end());

        // If it's an internal node, move half of the children as well
        if (!node->isLeaf) {
            new_node->children.insert(new_node->children.begin(), node->children.begin() + middle, node->children.end());
            node->children.erase(node->children.begin() + middle, node->children.end());
            for (Node* child : new_node->children) {
                child->parent = new_node;
            }
        }

        // Insert the middle key to the parent
        if (!node->parent) {
            // If the current node is the root, create a new root
            Node* new_root = new Node(false);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            node->parent = new_root;
            new_node->parent = new_root;
            rootptr = new_root;
            insertNonFull(new_root, key);
        } else {
            // Otherwise, insert the middle key to the parent node
            Node* parent = node->parent;
            auto it = std::lower_bound(parent->keys.begin(), parent->keys.end(), key);
            parent->keys.insert(it, key);
            it = std::lower_bound(parent->keys.begin(), parent->keys.end(), node->keys[0]);
            int index = std::distance(parent->keys.begin(), it);
            parent->children.insert(parent->children.begin() + index + 1, new_node);
            new_node->parent = parent;

            // Check if the parent needs to be split
            if (parent->keys.size() >= ORDER_) {
                splitNode(parent, key);
            }
        }
    }
    void insertNonFull(Node* node, int key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        // std::lower_bound returns an iterator pointing to the first element in a sorted range that is not less than a specified value.
        node->keys.insert(it, key);
    }
    void removeFromLeaf(Node* node, int key) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        if (it != node->keys.end() && *it == key) {
            node->keys.erase(it);
        }
    }
    void removeFromNonLeaf(Node* node, int key) { //////////////////
        // Find the child which contains the key
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int index = std::distance(node->keys.begin(), it);

        // Recursive call to remove the key from the child
        Node* child = node->children[index];
        if (child->keys.size() >= ORDER_ / 2) { // remove from at least hall-full node
            remove(key); // TODO: this is wrong, shouldn't call remove
        } else {
            // TODO: Implement merge or redistribution with siblings

        }

    }
    std::optional<int> getFromLeafNode(Node* node, int key) {
        // Find the child which contains the key
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int index = std::distance(node->keys.begin(), it);

        if (index < node->keys.size() && node->keys[index] == key) {
            return key; // Key found in this node
        } 
        return std::nullopt; // Key not found
    }
};


int f() {return 0;}
