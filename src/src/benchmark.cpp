#include "benchmark.hpp"

#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"
#include "coarseTree/coarseTree.hpp"
#include "fineTree/fineNode.hpp"
#include "fineTree/fineTree.hpp"
#include "freeTree/freeNode.hpp"
#include "freeTree/freeTree.hpp"
#include "distriTree/distriTree.hpp"

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
        auto runner = Engine::benchmarkEngine<Tree::SeqBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        runner.Run();
    } else if (type == TreeType::CoarseGrain) {
        auto runner = Engine::benchmarkEngine<Tree::CoarseLockBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        runner.Run();
    } else if (type == TreeType::FineGrain) {
        auto runner = Engine::benchmarkEngine<Tree::FineLockBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        runner.Run();
    } else if (type == TreeType::LockFree) {
        auto runner = Engine::benchmarkEngine<Tree::FreeBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        runner.Run();
    } else {
        assert(false);
        // Does not support DistriBPlusTree yet!
        // auto runner = Engine::benchmarkEngine<Tree::DistriBPlusTree<int>, int>(cases, cfg.order, cfg.numThread, cfg.numRun);
        // runner.Run();
    }
}

int main() {
    std::vector<std::string> Cases = {};
    // for (int i = 0; i < 3; i ++) {
    //     std::string s = "../src/test/small_" + std::to_string(i) + ".case";
    //     Cases.push_back(s);
    // }
    for (int i = 0; i < 1; i ++) {
        std::string s = "../src/test/B_denseMedian_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }
    // for (int i = 0; i < 3; i ++) {
    //     std::string s = "../src/test/mega_" + std::to_string(i) + ".case";
    //     Cases.push_back(s);
    // }

    EngineConfig sequentialCfg = EngineConfig(7, 1, 1);
    EngineConfig parallelCfg2  = EngineConfig(7, 4, 3);
    EngineConfig parallelCfg4  = EngineConfig(7, 4, 3);

    // MetaEngine(TreeType::Sequential, "Baseline", Cases, sequentialCfg);
    // MetaEngine(TreeType::CoarseGrain, "CoarseGrain x2", Cases, parallelCfg2);
    // MetaEngine(TreeType::CoarseGrain, "CoarseGrain x4", Cases, parallelCfg4);
    // MetaEngine(TreeType::FineGrain  , "FineGrain x2", Cases, parallelCfg2);
    // MetaEngine(TreeType::FineGrain  , "FineGrain x4", Cases, parallelCfg4);
    MetaEngine(TreeType::LockFree   , "LockFree x1", Cases, sequentialCfg);
    
    return 0;
}
