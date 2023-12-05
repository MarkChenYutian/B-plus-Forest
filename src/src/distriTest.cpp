#include <vector>
#include "engine.hpp"
#include "distriTree/distriTree.hpp"

int main(int argc, char *argv[]) {
    // MPI_Init(&argc, &argv);
    int mpi_thread_support;
    int init_status = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &mpi_thread_support);
    assert(init_status == MPI_SUCCESS && mpi_thread_support >= MPI_THREAD_SERIALIZED);
    std::cout << "requested MPI level: " << MPI_THREAD_SERIALIZED << ", get MPI level: " << mpi_thread_support << std::endl;
    {
        auto T = Tree::DistriBPlusTree<int>(5, MPI_COMM_WORLD);
        // sleep(3);
    }
    MPI_Finalize();
    DBG_PRINT(std::cout << "EXIT" << std::endl;);
    return 0;
}
