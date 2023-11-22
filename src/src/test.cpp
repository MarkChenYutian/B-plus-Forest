#include <stdio.h>
#include <iostream>
#include <cassert>
#include <optional>
#include "tree.hpp" 
#include "node.hpp"

#define TESTCASE(name) (std::cout << "\n\n\033[1;35mUnit Test '" << name << "' @ line " << __LINE__ << "\n\033[0m");

template <typename T>
void assertHasValue(Tree::SeqBPlusTree<T>* tree, T key) {
    assert(tree->get(key).has_value() && (*tree->get(key) == key));
}

template <typename T>
void assertNoValue(Tree::SeqBPlusTree<T>* tree, T key) {
    assert(!tree->get(key).has_value());
}

int main() {
    std::cout << ">>>>> Unit Test <<<<<" << std::endl;

    {TESTCASE("Insert sanity test")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.debug_assertIsValid(true);
        tree.insert(18);
        tree.debug_assertIsValid(true);
        tree.insert(16);
        tree.insert(14);
        tree.debug_assertIsValid(true);
        assertHasValue(&tree, 17);
        assertNoValue(&tree, 21);
    }

    {TESTCASE("Simple Remove")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_assertIsValid(true);

        tree.remove(20);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 20);

        tree.remove(16);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 16);

        tree.remove(14);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove, left/right both same parent with node, but left not moreHalfFull, choose to merge with left")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_assertIsValid(true);

        tree.remove(14);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 14);

        tree.remove(12);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 12);
    }

    {TESTCASE("Remove, only right is same parent with node, right moreHalfFull, borrow from right")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_assertIsValid(true);
        tree.remove(17);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 17);

    }
    
    {TESTCASE("Remove, both left/right same parent with node, both not moreHalfFull, choose to merge with left")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_assertIsValid(true);

        assert(tree.remove(16));
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 16);

        tree.remove(12);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 12);

        assert(!tree.remove(12));
        
        assert(tree.remove(14));
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove(order=4), both left/right same parent with node, but left not moreHalfFull, choose to merge with left")
        auto tree = Tree::SeqBPlusTree<int>(4);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.insert(13);
        tree.debug_assertIsValid(true);

        tree.remove(13);
        tree.debug_assertIsValid(true);
        assertNoValue(&tree, 13);
    }

    {TESTCASE("Remove, order=5 merge right + merge internal right")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_assertIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_assertIsValid(true);

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        tree.debug_assertIsValid(true);

        tree.remove(17);
        tree.debug_assertIsValid(true);

        tree.remove(11);
        tree.debug_assertIsValid(true); 
        // B+ Tree (Root)          [20, 30, 40, 45, ] <->
        // B+ Tree Lv 0    [10, 12, 15, ] <-> [20, 22, 25, 27, ] <-> [30, 32, 35, 37, ] <-> [40, 42, ] <-> [45, 47, 50, ] <->
    }

    {TESTCASE("Remove, order=5 merge right + borrow internal right")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_assertIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_assertIsValid(true);

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
        tree.debug_assertIsValid(true);

        tree.remove(17);
        tree.debug_assertIsValid(true);

        tree.remove(11);
        tree.debug_assertIsValid(true); 
    }


    {TESTCASE("Remove, order=5 merge right + merge internal left")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_assertIsValid(true);

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_assertIsValid(true);

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.debug_assertIsValid(true);

        tree.remove(30);
        tree.debug_assertIsValid(true);

        tree.remove(22);
        tree.debug_assertIsValid(true);
    }


    {TESTCASE("Remove, order=5 INTENDED merge right + borrow internal left")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_assertIsValid(true);

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_assertIsValid(true);

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(16);
        tree.insert(14);
        tree.insert(13);
        tree.debug_assertIsValid(true);

        tree.remove(30);
        tree.debug_assertIsValid(true);

        tree.remove(22);
        tree.debug_assertIsValid(true);
    }
    
    {TESTCASE("Remove, order=5 merge")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_assertIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_assertIsValid(true);

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        tree.debug_assertIsValid(true);

        tree.remove(27);
        tree.debug_assertIsValid(true);

        tree.remove(25);
        tree.debug_assertIsValid(true); 
    }
    return 0;
}
