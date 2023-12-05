#include <vector>
#include "engine.hpp"
#include "distriTree/distriTree.hpp"

int main(int argc, char *argv[]) {
    // MPI_Init(&argc, &argv);
    int mpi_thread_support;
    int init_status = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &mpi_thread_support);
    assert(init_status == MPI_SUCCESS && mpi_thread_support >= MPI_THREAD_SERIALIZED);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    {
        auto T = Tree::DistriBPlusTree<int>(5, MPI_COMM_WORLD);
        T.insert(rank);
        T.insert(rank+size);
        MPI_Barrier(MPI_COMM_WORLD);
        for (size_t i = 0; i < size; i++) {
            std::optional<int> val1 = T.get(i);
            std::optional<int> val2 = T.get(i+size);
            // if (!val.has_value()) {
            //     DBG_PRINT(std::cout << "Rank: " << rank << "  | missing " << i << std::endl;);
            // }
            assert(val1.has_value());
            assert(val2.has_value());
            // DBG_PRINT(std::cout << "Rank: " << rank << "  |  " << val.value() << std::endl;);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        T.remove(rank);
        MPI_Barrier(MPI_COMM_WORLD);
        for (size_t i = 0; i < size; i++) {
            std::optional<int> val = T.get(i+size);
            assert(val.has_value());
        }
        for (size_t i = 0; i < size; i++) {
            std::optional<int> val = T.get(i);
            assert(!val.has_value());
        }
    }
    MPI_Finalize();
    DBG_PRINT(std::cout << "EXIT" << std::endl;);
    return 0;
}
