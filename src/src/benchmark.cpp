#include "engine.hpp"

#include "seqTree/seqNode.hpp"
#include "seqTree/seqTree.hpp"
#include "coarseTree/coarseTree.hpp"
#include "fineTree/fineNode.hpp"
#include "fineTree/fineTree.hpp"
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
        std::string s = "../test/B_megaGet_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    Engine::EngineConfig sequentialCfg {5, 1, 1, Cases, std::make_pair(100000, 900000)};
    Engine::EngineConfig parallelx2Cfg {5, 2, 1, Cases, std::make_pair(100000, 900000)};
    Engine::EngineConfig parallelx4Cfg {5, 4, 1, Cases, std::make_pair(100000, 900000)};
    Engine::EngineConfig parallelx8Cfg {5, 8, 1, Cases, std::make_pair(100000, 900000)};

    Engine::EngineConfig workerx2Cfg {5, 1, 2, Cases, std::make_pair(100000, 900000)};
    Engine::EngineConfig workerx4Cfg {5, 1, 4, Cases, std::make_pair(100000, 900000)};
    Engine::EngineConfig workerx8Cfg {5, 1, 8, Cases, std::make_pair(100000, 900000)};

     MetaEngine(TreeType::Sequential, "Baseline", Cases, sequentialCfg);
     MetaEngine(TreeType::CoarseGrain, "CoarseGrain x2", Cases, parallelx2Cfg);
     MetaEngine(TreeType::CoarseGrain, "CoarseGrain x4", Cases, parallelx4Cfg);

     MetaEngine(TreeType::FineGrain  , "FineGrain x1", Cases, sequentialCfg);
     MetaEngine(TreeType::FineGrain  , "FineGrain x2", Cases, parallelx2Cfg);
     MetaEngine(TreeType::FineGrain  , "FineGrain x4", Cases, parallelx4Cfg);
     MetaEngine(TreeType::FineGrain  , "FineGrain x8", Cases, parallelx8Cfg);

     MetaEngine(TreeType::LockFree   , "LockFree x1", Cases, sequentialCfg);
     MetaEngine(TreeType::LockFree   , "LockFree x2", Cases, workerx2Cfg);
     MetaEngine(TreeType::LockFree   , "LockFree x4", Cases, workerx4Cfg);
     MetaEngine(TreeType::LockFree   , "LockFree x8", Cases, workerx8Cfg);

    return 0;
}
