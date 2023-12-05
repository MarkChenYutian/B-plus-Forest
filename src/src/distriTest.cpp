#include <vector>
#include "engine.hpp"
#include "distriTree/distriTree.hpp"

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    /*
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
        // auto runner = Engine::distriEngine<Tree::DistriBPlusTree<int>, int>(Cases, 16, 8);
        // runner.Run();
    }
    */
    auto T = Tree::DistriBPlusTree<int>(5, MPI_COMM_WORLD);
    
    std::cout << getpid() << std::endl;
    return 0;
}
