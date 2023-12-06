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
        auto runner = Engine::SeqEngine<Tree::SeqBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::CoarseGrain) {
        auto runner = Engine::ThreadEngine<Tree::CoarseLockBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::FineGrain) {
        auto runner = Engine::ThreadEngine<Tree::FineLockBPlusTree>(cfg);
        runner.Run();
    } else if (type == TreeType::LockFree) {
        auto runner = Engine::BenchmarkEngine<Tree::FreeBPlusTree>(cfg);
        runner.Run();
    } else {
        assert(false);
    }
}

int main(int argc, char **argv) {
    int order, numThread;
    std::string caseName, treeType, baseDir;
    TreeType type;
    
    order = std::stoi(argv[1]);
    numThread = std::stoi(argv[2]);
    treeType = argv[3];
    baseDir = "../test/";
    caseName = argv[4];

    if (treeType == "Seq") type = TreeType::Sequential;
    else if (treeType == "Coarse") type = TreeType::CoarseGrain;
    else if (treeType == "Fine") type = TreeType::FineGrain;
    else if (treeType == "Free") type = TreeType::LockFree;
    else assert(false);

    std::vector<std::string> Cases = {baseDir + caseName};
    Engine::EngineConfig config {order, numThread, Cases};
    MetaEngine(type, "", Cases, config);
    return 0;
}
