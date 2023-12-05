#include <vector>
#include "engine.hpp"
#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"

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
    for (int i = 0; i < 3; i ++) {
        std::string s = "../src/test/mega_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    {
        auto runner = Engine::seqEngine<Tree::SeqBPlusTree<int>, int>(Cases, 16, 1);
        runner.Run();
    }

    


    return 0;
}
