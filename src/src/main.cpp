#include <iostream>
#include "node.hpp"
#include "freetree.hpp"
#include "seqTree.hpp"
#include "engine.hpp"

#define TESTCASE(name) (std::cout << "\n\n\033[1;35mUnit Test '" << name << "' @ line " << __LINE__ << "\n\033[0m");

int main() {    
    // {TESTCASE("INSERT sanity test")
    //     Tree::FreeBPlusTree<int> tree(3, 4);
    //     tree.insert(15);
    //     tree.insert(10);
    //     tree.insert(20);
    //     tree.insert(12);
    //     tree.insert(17);
    //     tree.insert(18);
    //     tree.insert(16);
    //     tree.insert(14);
    // }

    {TESTCASE("Big INSERT & GET")
        // std::vector<std::string> Cases = {"../test/IAndG_0.case", "../test/small_0.case"};
        std::vector<std::string> Cases = {"../test/small_0.case"};
        auto runner = Engine::lockfreeCheckEngine<Tree::FreeBPlusTree<int>, int>(Cases, 4, 1);
        runner.Run();
    }

    // {TESTCASE("Simple Remove")
    //     auto tree = Tree::FreeBPlusTree<int>(3, 1);
    //     tree.insert(15);
    //     tree.insert(10);
    //     tree.insert(20);
    //     tree.insert(12);

    //     tree.insert(17);
    //     tree.insert(18);
    //     tree.insert(16);
    //     tree.insert(14);

    //     tree.remove(20);
    //     tree.remove(16);
    //     tree.remove(14);
    //     tree.remove(10);
        
    //     tree.remove(12);
    //     tree.remove(15);
    //     tree.remove(17);
    //     tree.remove(18);

    //     tree.insert(17);
    //     tree.insert(10);
    //     tree.insert(20);
    //     tree.insert(12);
    // }

    // {TESTCASE("Complex Remove")
    //     auto tree = Tree::FreeBPlusTree<int>(3, 1);
    //     tree.insert(15);
    //     tree.insert(10);
    //     tree.insert(20);
    //     tree.insert(12);
    //     tree.insert(17);
    //     tree.insert(18);
    //     tree.insert(16);
    //     tree.insert(14);
    //     tree.insert(11);

    //     tree.insert(13);
    //     tree.remove(15);
    //     tree.remove(10);
    //     tree.remove(20);
    //     tree.remove(12);
    //     tree.remove(17);
    //     tree.remove(18);
    //     tree.remove(16);
    //     tree.remove(14);

    //     tree.remove(11);
    //     tree.remove(13);
    //     tree.get(11);

    // }
    

    return 0;
}
