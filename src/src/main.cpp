#include <iostream>
#include "node.hpp"
#include "freetree.hpp"
#include "seqTree.hpp"
#include "engine.hpp"

#define TESTCASE(name) (std::cout << "\n\n\033[1;35mUnit Test '" << name << "' @ line " << __LINE__ << "\n\033[0m");

int main() {    
    // {TESTCASE("Insert sanity test")
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
    std::vector<std::string> Cases = {"../test/IAndG_0.case"};
    auto runner = Engine::lockfreeCheckEngine<Tree::FreeBPlusTree<int>, int>(Cases, 4, 2);
    runner.Run();

    return 0;
}
