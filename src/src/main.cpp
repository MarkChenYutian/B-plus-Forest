#include <iostream>
#include "tree.h"

#define TESTCASE (std::cout << "\n\nTESTCASE " << __LINE__ << ":\n");

int main() {
    std::cout << "Hello, World!" << std::endl;

    {TESTCASE
        auto tree = Tree::BPlusTree<int>(2);
        tree.insert(10);
        tree.insert(15);
        tree.printBPlusTree();
    }

    {TESTCASE
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.printBPlusTree();
        tree.insert(20);
        tree.printBPlusTree();
        tree.insert(12);
        tree.printBPlusTree();
        tree.insert(17);
        tree.printBPlusTree();
        tree.insert(18);
        tree.printBPlusTree();
        tree.insert(16);
        tree.printBPlusTree();
        tree.insert(14);
        tree.printBPlusTree();
    }
    
    return 0;
}
