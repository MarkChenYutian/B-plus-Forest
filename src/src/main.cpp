#include <iostream>
#include "node.hpp"
#include "freetree/freetree.hpp"
#include "seqTree.hpp"

int main() {
    Tree::SeqBPlusTree<int> T_ref(3);
    T_ref.insert(10);
    T_ref.insert(15);
    T_ref.insert(20);
    Tree::SeqNode<int> *rootPtr = T_ref.getRoot();

    Tree::FreeBPlusTree<int> T(3, 4, rootPtr);
    T.get(10);
    T.get(15);
    T.get(20);
    T.get(21);
    T.get(18);
    T.remove(18);
    T.remove(15);
    T.remove(20);
    return 0;
}
