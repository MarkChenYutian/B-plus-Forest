#include <vector>
#include "engine.hpp"
#include "freeTree/freeTree.hpp"

int main() {

    std::vector<std::string> Cases = {};
    for (int i = 0; i < 1; i ++) {
        std::string s = "../test/large_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
//    for (int i = 0; i < 10; i ++) {
//        std::string s = "../test/large_" + std::to_string(i) + ".case";
//        Cases.push_back(s);
//    }
//    for (int i = 0; i < 1; i ++) {
//        std::string s = "../test/mega_" + std::to_string(i) + ".case";
//        Cases.push_back(s);
//    }
    {
        Engine::EngineConfig cfg {7, 1, 1, Cases};
        auto runner = Engine::BenchmarkEngine<Tree::FreeBPlusTree>(cfg);
        runner.Run();
    }
    return 0;
}
