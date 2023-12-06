#include <vector>
#include "engine.hpp"
#include "coarseTree/coarseTree.hpp"

int main() {

    std::vector<std::string> Cases = {};
    for (int i = 0; i < 10; i ++) {
        std::string s = "../src/test/small_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
    for (int i = 0; i < 10; i ++) {
        std::string s = "../src/test/large_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    {
        Engine::EngineConfig cfg {5, 2, Cases};
        auto runner = Engine::ThreadEngine<Tree::CoarseLockBPlusTree>(cfg);
        runner.Run();
    }


    return 0;
}
