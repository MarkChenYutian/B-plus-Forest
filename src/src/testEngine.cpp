#include <vector>
#include "engine.hpp"
#include "seqTree.hpp"
#include "coarseTree.hpp"


int main() {
    std::vector<std::string> Cases;
    for (int i = 0; i < 10; i ++) {
        std::string s = "../test/small_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
    for (int i = 0; i < 10; i ++) {
        std::string s = "../test/large_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    // auto runner = Engine::correctnessEngine<Tree::SeqBPlusTree<int>, int>(Cases, 11, 4);
    auto runner = Engine::threadingEngine<Tree::CoarseLockBPlusTree<int>, int>(Cases, 11, 4);
    runner.Run();
    return 0;
}
