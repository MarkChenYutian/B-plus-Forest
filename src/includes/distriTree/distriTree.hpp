#pragma once

#include <optional>
#include "mpi.h"
#include "tree.h"

namespace Tree {

template <typename T>
DistributeBPlusTree<T>::DistributeBPlusTree(int order, int rank): ORDER_(order)
{}

}
