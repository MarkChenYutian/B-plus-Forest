#include <vector>
#include "engine.hpp"
#include "distriTree/distriTree.hpp"

int main(int argc, char *argv[]) {
    std::vector<std::string> Cases = {};
    for (int i = 0; i < 3; i ++) {
        std::string s = "../test/B_megaGet_" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    int mpi_thread_support;
    int init_status = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_thread_support);
    assert(init_status == MPI_SUCCESS && mpi_thread_support >= MPI_THREAD_MULTIPLE);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    {
        Engine::EngineConfig sequentialCfg {5, 1, 1, Cases, std::make_pair(100000, 900000)};
        auto engine = Engine::DistributeEngine(sequentialCfg, MPI_COMM_WORLD);
        engine.Run();
    }

    MPI_Finalize();
    DBG_PRINT(std::cout << "EXIT" << std::endl;);
    return 0;
}
