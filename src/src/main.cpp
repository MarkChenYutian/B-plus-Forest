#include <iostream>
#include "node.hpp"
#include "freetree.hpp"
#include "seqTree.hpp"

int main() {
    Tree::SeqBPlusTree<int> T_ref(3);
    T_ref.insert(10);
    // T_ref.insert(15);
    // T_ref.insert(20);
    Tree::SeqNode<int> *rootPtr = T_ref.getRoot();
    // T_ref.getRoot()->children[0]->printKeys();

    Tree::FreeBPlusTree<int> T(3, 4);
    // T.get(10);
    // T.get(15);
    // T.get(20);
    // T.get(21);
    // T.get(18);
    T.insert(10);
    T.insert(15);
    // T.insert(20);
    // sleep(1);
    // DBG_PRINT(
    //     T_ref.print();
    // );
    return 0;
}