#ifndef TREE_H
#define TREE_H

#include <iostream>
#include <vector>
#include <optional>

namespace Tree {
    template<typename T>
class BPlusTree {
    private:
        struct Node {
            bool isLeaf;
            std::shared_ptr<Node> parent;
            std::vector<int> keys;
            union body
            {
                // Having children if not leaf node
                std::vector<std::shared_ptr<Node> > children;    
                // Having value if is leaf node
                std::vector<T*> value;          
            };
            Node(bool leaf) : isLeaf(leaf), parent(nullptr) {}
        };

        std::shared_ptr<Node> rootPtr;
        int ORDER_;

    public:
        BPlusTree(int order=3) : rootPtr(nullptr), ORDER_(order) {}

        // Public API
        void insert(int key);
        void remove(int key);
        int get(int key);
        void printBPlusTree();

    private:
        // Private helper functions
        std::shared_ptr<Node> findLeafNode(Node* node, int key);
        void splitNode(std::shared_ptr<Node> node, int key);
        void insertNonFull(std::shared_ptr<Node> node, int key);
        void removeFromLeaf(std::shared_ptr<Node> node, int key);
        void removeFromNonLeaf(std::shared_ptr<Node> node, int key);
        int getFromLeafNode(std::shared_ptr<Node> node, int key, bool &isValid);
    };
}



#endif
