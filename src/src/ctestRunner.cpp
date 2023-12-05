#include "engine.hpp"

#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"
#include "coarseTree/coarseTree.hpp"
#include "fineTree/fineNode.hpp"
#include "fineTree/fineTree.hpp"
#include "freeTree/freeNode.hpp"
#include "freeTree/freeTree.hpp"

enum TreeType {Sequential, CoarseGrain, FineGrain, LockFree, Distributed};

struct EngineConfig {
    int order;
    int numThread;
    int numRun;
    EngineConfig(int order, int numThread, int numRun): order(order), numThread(numThread), numRun(numRun) {};
};

void MetaEngine(TreeType type, std::string const &name, std::vector<std::string> cases, EngineConfig const &cfg) {
    std::cout << "TESTCASE: " << name << std::endl;
    if (type == TreeType::Sequential) {
        auto runner = Engine::seqEngine<Tree::SeqBPlusTree<int>, int>(cases, cfg.order, cfg.numThread);
        runner.Run();
    } else if (type == TreeType::CoarseGrain) {
        auto runner = Engine::threadingEngine<Tree::CoarseLockBPlusTree<int>, int>(cases, cfg.order, cfg.numThread);
        runner.Run();
    } else if (type == TreeType::FineGrain) {
        auto runner = Engine::threadingEngine<Tree::FineLockBPlusTree<int>, int>(cases, cfg.order, cfg.numThread);
        runner.Run();
    } else if (type == TreeType::LockFree) {
        auto runner = Engine::lockfreeEngine<Tree::FreeBPlusTree<int>, int>(cases, cfg.order, cfg.numThread);
        runner.Run();
    } else {
        assert(false);
        // Does not support DistriBPlusTree yet!
        // auto runner = Engine::benchmarkEngine<Tree::DistriBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        // runner.Run();
    }
}

int main(int argc, char **argv) {
    int order, numThread;
    std::string caseName, treeType, baseDir;
    TreeType type;
    
    order = std::stoi(argv[1]);
    numThread = std::stoi(argv[2]);
    treeType = argv[3];
    baseDir = "../src/test/";
    caseName = argv[4];

    if (treeType == "Seq") type = TreeType::Sequential;
    else if (treeType == "Coarse") type = TreeType::CoarseGrain;
    else if (treeType == "Fine") type = TreeType::FineGrain;
    else if (treeType == "Free") type = TreeType::LockFree;
    else assert(false);

    std::vector<std::string> Cases = {baseDir + caseName};
    // for (int i = 0; i < 3; i ++) {
    //     std::string s = "../src/test/B_sparseGet_" + std::to_string(i) + ".case";
    //     Cases.push_back(s);
    // }
    // for (int i = 0; i < 3; i ++) {
    //     std::string s = "../src/test/B_denseMedian_" + std::to_string(i) + ".case";
    //     Cases.push_back(s);
    // }
    // for (int i = 0; i < 3; i ++) {
    //     std::string s = "../src/test/mega_" + std::to_string(i) + ".case";
    //     Cases.push_back(s);
    // }

    EngineConfig config = EngineConfig(order, numThread, 1);
    MetaEngine(type, "AutoTesting...", Cases, config);
    return 0;
}
