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
    src->debug_checkParentPointers();
    // checking ordering
    src->debug_checkOrdering(std::nullopt, std::nullopt);
    // checking number of key/children
    src->debug_checkChildCnt(tree->getOrder());

    std::cout << "B+ Tree (Root)" << "\t";
    src->printKeys();
    std::cout << " <-> \n";
    int lvl = 0;
    do {
        src = src->children[0];

        auto ckptr = src;
        std::cout << "B+ Tree Lv " << lvl << " \t";
        ckptr->printKeys();
        std::cout << " <-> ";

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
            ckptr->printKeys();
            std::cout << " <-> ";
        }
        std::cout << std::endl;
        lvl ++;
    } while (!src->isLeaf);

    std::cout << "\033[1;32mPASS! tree is valid" << " \033[0m" << std::endl;
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
        isValidBtree(&tree);
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
        assert (isValidBtree(&tree));

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

    {TESTCASE("Remove, left/right both same parent with node, but left not moreHalfFull, choose to merge with left")
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

        tree.remove(14);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 14);

        tree.remove(12);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 12);
    }

    {TESTCASE("Remove, only right is same parent with node, right moreHalfFull, borrow from right")
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
    
    {TESTCASE("Remove, both left/right same parent with node, both not moreHalfFull, choose to merge with left")
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

        assert(tree.remove(16));
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 16);

        tree.remove(12);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 12);

        assert(!tree.remove(12));
        
        assert(tree.remove(14));
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove(order=4), both left/right same parent with node, but left not moreHalfFull, choose to merge with left")
        auto tree = Tree::BPlusTree<int>(4);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.insert(13);
        assert (isValidBtree(&tree));

        tree.remove(13);
        assert (isValidBtree(&tree));
        assertNoValue(&tree, 13);
    }

    {TESTCASE("Remove, order=5 merge right + merge internal right")
        auto tree = Tree::BPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        assert (isValidBtree(&tree));

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        assert (isValidBtree(&tree));

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        assert (isValidBtree(&tree));

        tree.remove(17);
        assert (isValidBtree(&tree));

        tree.remove(11);
        assert (isValidBtree(&tree)); 
        // B+ Tree (Root)          [20, 30, 40, 45, ] <->
        // B+ Tree Lv 0    [10, 12, 15, ] <-> [20, 22, 25, 27, ] <-> [30, 32, 35, 37, ] <-> [40, 42, ] <-> [45, 47, 50, ] <->
    }

    {TESTCASE("Remove, order=5 merge right + borrow internal right")
        auto tree = Tree::BPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        assert (isValidBtree(&tree));

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        assert (isValidBtree(&tree));

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        tree.insert(31);
        assert (isValidBtree(&tree));

        tree.remove(17);
        assert (isValidBtree(&tree));

        tree.remove(11);
        assert (isValidBtree(&tree)); 
    }


    {TESTCASE("Remove, order=5 merge right + merge internal left")
        auto tree = Tree::BPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        assert (isValidBtree(&tree));

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        assert (isValidBtree(&tree));

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        assert (isValidBtree(&tree));

        tree.remove(30);
        assert (isValidBtree(&tree));

        tree.remove(22);
        assert (isValidBtree(&tree));
    }


    {TESTCASE("Remove, order=5 INTENDED merge right + borrow internal left")
        auto tree = Tree::BPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        assert (isValidBtree(&tree));

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        assert (isValidBtree(&tree));

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(16);
        tree.insert(14);
        tree.insert(13);
        assert (isValidBtree(&tree));

        tree.remove(30);
        assert (isValidBtree(&tree));

        tree.remove(22);
        assert (isValidBtree(&tree));
    }
    /*
    {TESTCASE("Remove, order=5 merge")
        auto tree = Tree::BPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        assert (isValidBtree(&tree));

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        assert (isValidBtree(&tree));

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        assert (isValidBtree(&tree));

        tree.remove(27);
        assert (isValidBtree(&tree));

        // tree.remove(25);
        // assert (isValidBtree(&tree)); 
    }*/
    return 0;
}
