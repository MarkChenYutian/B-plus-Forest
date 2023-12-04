#include <stdio.h>
#include <iostream>
#include <cassert>
#include <optional>
#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"

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
        assertNoValue(&tree, 0);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        assertHasValue(&tree, 20);
        assertHasValue(&tree, 15);
        assertHasValue(&tree, 10);
        tree.insert(12);
        tree.insert(17);
        tree.debug_checkIsValid(true);
        tree.insert(18);
        assertHasValue(&tree, 18);
        assertNoValue(&tree, 16);
        tree.debug_checkIsValid(true);
        tree.insert(16);
        tree.insert(14);
        tree.debug_checkIsValid(true);
        assertHasValue(&tree, 17);
        assertNoValue(&tree, 21);
    }
    
    {TESTCASE("Delete all element leak test")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.remove(15);
        tree.remove(10);
        tree.remove(20);
        tree.debug_checkIsValid(true);
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
        tree.debug_checkIsValid(true);

        tree.remove(20);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 20);

        tree.remove(16);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 16);

        tree.remove(14);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_checkIsValid(true);

        tree.remove(14);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 14);

        tree.remove(12);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 12);
    }

    {TESTCASE("Remove")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_checkIsValid(true);
        tree.remove(17);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 17);

    }
    
    {TESTCASE("Remove")
        auto tree = Tree::SeqBPlusTree<int>(3);
        tree.insert(15);
        tree.insert(10);
        tree.insert(20);
        tree.insert(12);
        tree.insert(17);
        tree.insert(18);
        tree.insert(16);
        tree.insert(14);
        tree.debug_checkIsValid(true);

        assert(tree.remove(16));
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 16);

        tree.remove(12);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 12);

        assert(!tree.remove(12));
        
        assert(tree.remove(14));
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 14);
    }

    {TESTCASE("Remove(order=4)")
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
        tree.debug_checkIsValid(true);

        tree.remove(13);
        tree.debug_checkIsValid(true);
        assertNoValue(&tree, 13);
    }

    {TESTCASE("Remove")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_checkIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_checkIsValid(true);

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        tree.debug_checkIsValid(true);

        tree.remove(17);
        tree.debug_checkIsValid(true);

        tree.remove(11);
        tree.debug_checkIsValid(true); 
    }

    {TESTCASE("Remove, order=5")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_checkIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_checkIsValid(true);

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
        tree.debug_checkIsValid(true);

        tree.remove(17);
        tree.debug_checkIsValid(true);

        tree.remove(11);
        tree.debug_checkIsValid(true); 
    }


    {TESTCASE("Remove, order=5")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_checkIsValid(true);

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_checkIsValid(true);

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.debug_checkIsValid(true);

        tree.remove(30);
        tree.debug_checkIsValid(true);

        tree.remove(22);
        tree.debug_checkIsValid(true);
    }


    {TESTCASE("Remove, order=5")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_checkIsValid(true);

        tree.insert(5);
        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_checkIsValid(true);

        tree.insert(2);
        tree.insert(7);
        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(16);
        tree.insert(14);
        tree.insert(13);
        tree.debug_checkIsValid(true);

        tree.remove(30);
        tree.debug_checkIsValid(true);

        tree.remove(22);
        tree.debug_checkIsValid(true);
    }
    
    {TESTCASE("Remove, order=5")
        auto tree = Tree::SeqBPlusTree<int>(5);
        tree.insert(10);
        tree.insert(20);
        tree.insert(30);
        tree.insert(40);
        tree.insert(50);
        tree.debug_checkIsValid(true);

        tree.insert(15);
        tree.insert(25);
        tree.insert(35);
        tree.insert(45);
        tree.debug_checkIsValid(true);

        tree.insert(12);
        tree.insert(17);
        tree.insert(22);
        tree.insert(27);
        tree.insert(32);
        tree.insert(37);
        tree.insert(42);
        tree.insert(47);
        tree.insert(11);
        tree.debug_checkIsValid(true);

        tree.remove(27);
        tree.debug_checkIsValid(true);

        tree.remove(25);
        tree.debug_checkIsValid(true); 
    }
    return 0;
}
