#include <vector>
#include "engine.hpp"
#include "seqTree.hpp"
#include "coarseTree.hpp"
#include "fineTree.hpp"
#include "lockNode.hpp"

int main() {
    std::vector<std::string> Cases = {};
    for (int i = 0; i < 10; i ++) {
        std::string s = "../test/small_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
    for (int i = 0; i < 10; i ++) {
        std::string s = "../test/large_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
    // auto runner = Engine::correctnessEngine<Tree::FineLockBPlusTree<int>, int>(Cases, 11, 1);
    auto runner = Engine::threadingEngine<Tree::FineLockBPlusTree<int>, int>(Cases, 11, 8);
    runner.Run();
    return 0;
}
