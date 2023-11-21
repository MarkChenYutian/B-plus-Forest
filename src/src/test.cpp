#include <stdio.h>
#include <iostream>
#include <cassert>
#include <optional>
#include "tree.h" 

#define TESTCASE(name) (std::cout << "\n\n\033[1;35mUnit Test '" << name << "' @ line " << __LINE__ << "\n\033[0m");



template <typename T>
bool isValidBtree(Tree::BPlusTree<T>* tree) {
    // Move to left-most leaf node
    Tree::Node<T>* src = tree->getRoot();
    // checking parent child pointers
    src->checkParentChildPointers();
    std::cout << "Check B+ tree level " << -1 << " ";
    std::cout << "[";
        for (const auto key : src->keys) {
            std::cout << key << ", ";
        }
    std::cout << "] <-> \n";
    int lvl = 0;
    do {
        src = src->children[0];

        auto ckptr = src;
        std::cout << "Check B+ tree level " << lvl << " ";
        std::cout << "[";
            for (const auto key : ckptr->keys) {
                std::cout << key << ", ";
            }
        std::cout << "] <-> ";

        // Check the leaf nodes linked list
        while (ckptr->next != nullptr) {
            
            if (ckptr->next->prev != ckptr) {
                std::cerr << "Corrupted linked list!" << std::endl;
                tree->printBPlusTree();
                return false;
            }
            if (ckptr->next->keys[0] < ckptr->keys.back()) {
                std::cerr << "Leaves not well-ordered!" << std::endl;
                tree->printBPlusTree();
                return false;
            }
            ckptr = ckptr->next;
            std::cout << "[";
            for (const auto key : ckptr->keys) {
                std::cout << key << ", ";
            }
            std::cout << "] <-> ";
        }
        std::cout << std::endl;
        lvl ++;
    } while (!src->isLeaf);

    return true;
}



template <typename T>
void assertHasValue(Tree::BPlusTree<T>* tree, T key) {
    assert(tree->get(key).has_value() && (*tree->get(key) == key));
}

template <typename T>
void assertNoValue(Tree::BPlusTree<T>* tree, T key) {
    assert(!tree->get(key).has_value());
}

int main() {
    std::cout << ">>>>> Unit Test <<<<<" << std::endl;

    {TESTCASE("insert")
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        isValidBtree(&tree);
        tree.insert(18);
        isValidBtree(&tree); ////
        tree.insert(16);
        tree.insert(14);
        assert (isValidBtree(&tree));
        assertHasValue(&tree, 17);
        assertNoValue(&tree, 21);
    }

    {TESTCASE("Simple Remove")
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);

        tree.remove(20);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 20);

        tree.remove(16);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 16);

        tree.remove(14);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove, borrow right to left")
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);

        tree.remove(14);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 14);

        tree.remove(12);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 12);
    }

    {TESTCASE("Remove, borrow left to right")
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        assert (isValidBtree(&tree));
        tree.remove(17);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 17);

    }

    {TESTCASE("Remove, merge")
        auto tree = Tree::BPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);

        tree.remove(16);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 16);

        tree.remove(12);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 12);

        tree.remove(14);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 14);
    }

    // {TESTCASE
    //     auto tree = Tree::BPlusTree<int>(4);
    //     tree.insert(15);
    //     tree.insert(10);
    //     tree.insert(20);
    //     tree.insert(12);
    //     tree.insert(17);
    //     tree.insert(18);
    //     tree.insert(16);
    //     tree.insert(14);
    //     tree.insert(13);
    //     assert (isValidBtree(&tree));

    //     tree.remove(13);
    //     assert (isValidBtree(&tree));
    //     assertNoValue(&tree, 13);
    // }
    
    return 0;
}
