#include "engine.hpp"

#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"
#include "coarseTree/coarseTree.hpp"
#include "fineTree/fineNode.hpp"
#include "fineTree/fineTree.hpp"
#include "freeTree/freeNode.hpp"
#include "freeTree/freeTree.hpp"

enum TreeType {Sequential, CoarseGrain, FineGrain, LockFree, Distributed};

void MetaEngine(TreeType type, std::string const &name, std::vector<std::string> cases, Engine::EngineConfig const &cfg) {
    std::cout << "TESTCASE: " << name << std::endl;
    if (type == TreeType::Sequential) {
        auto runner = Engine::BenchmarkEngine<Tree::SeqBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::CoarseGrain) {
        auto runner = Engine::BenchmarkEngine<Tree::CoarseLockBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::FineGrain) {
        auto runner = Engine::BenchmarkEngine<Tree::FineLockBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::LockFree) {
        auto runner = Engine::BenchmarkEngine<Tree::FreeBPlusTree>(cfg);
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
    for (int i = 0; i < 3; i ++) {
        std::string s = "../src/test/large_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    Engine::EngineConfig sequentialCfg {7, 1, Cases};
    Engine::EngineConfig parallelx2Cfg {7, 2, Cases};
    Engine::EngineConfig parallelx4Cfg {7, 4, Cases};

    MetaEngine(TreeType::Sequential, "Baseline", Cases, sequentialCfg);
    // MetaEngine(TreeType::CoarseGrain, "CoarseGrain x2", Cases, parallelx2Cfg);
    // MetaEngine(TreeType::CoarseGrain, "CoarseGrain x4", Cases, parallelx4Cfg);
    // MetaEngine(TreeType::FineGrain  , "FineGrain x2", Cases, parallelx2Cfg);
    // MetaEngine(TreeType::FineGrain  , "FineGrain x4", Cases, parallelx4Cfg);
    MetaEngine(TreeType::LockFree   , "LockFree x1", Cases, sequentialCfg);
    
    return 0;
}
