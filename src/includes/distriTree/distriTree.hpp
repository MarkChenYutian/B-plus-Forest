#pragma once

#include <optional>
#include <pthread.h>
#include <barrier>
#include "mpi.h"
#include "tree.h"
#include "fineTree/fineTree.hpp"
#include "fineTree/fineNode.hpp"

constexpr int RequestTAG  = 1;
constexpr int ResultTAG   = 2;
constexpr int AckTAG      = 3;

pthread_barrier_t barrier_mpi;

namespace Tree {

template <typename T>
DistriBPlusTree<T>::DistriBPlusTree(int order, MPI_Comm world): 
ORDER_(order), WORLD_(world), internalTree(FineLockBPlusTree<T>(order)) {
    pthread_barrier_init(&barrier_mpi, NULL, 2); // TODO
    MPI_Comm_rank(WORLD_, &RANK_);
    MPI_Comm_size(WORLD_, &NUM_PROC_);

    MPI_Type_contiguous(sizeof(Tree_Request), MPI_CHAR, &TREE_REQUEST);
    MPI_Type_contiguous(sizeof(Tree_Result), MPI_CHAR, &TREE_RESULT);

    MPI_Type_commit(&TREE_REQUEST);
    MPI_Type_commit(&TREE_RESULT);
    
    Background_Args bg_args;
    bg_args.internalTree = &internalTree;
    bg_args.world        = WORLD_;
    bg_args.TREE_REQUEST = TREE_REQUEST;
    bg_args.TREE_RESULT  = TREE_RESULT;
    bg_args.numProc      = NUM_PROC_;
    bg_args.rank         = RANK_;

    pthread_create(&bg_thread, NULL, MPI_background, &bg_args);
    DBG_PRINT(
        std::cout << "Distributed Tree RANK: " << RANK_ << std::endl;
    )
    
}

template <typename T>
void *DistriBPlusTree<T>::MPI_background(void *args) {
    DBG_PRINT(std::cout << "Background Thread Start" << std::endl;);
    
    Background_Args *bg_args = static_cast<Background_Args*>(args);

    int nProc = bg_args->numProc;
    int rank  = bg_args->rank;
    MPI_Comm world = bg_args->world;
    MPI_Datatype _TREE_REQUEST = bg_args->TREE_REQUEST;
    MPI_Datatype _TREE_RESULT  = bg_args->TREE_RESULT;
    FineLockBPlusTree<T>* internalTree = bg_args->internalTree;
    
    // Count number of process sending "STOP" Request. Terminate this if and only if 
    // all processes STOPs.
    int terminateCounter = 0;
    DBG_PRINT(std::cout << "Background Thread INIT done" << std::endl;);
    while (true) {
        if (terminateCounter == nProc) break;
        DBG_PRINT(std::cout << "hello\n";);
        Tree_Request request = {-1, RequestType::STOP, -1000};
        /*
        int src_rank;
                RequestType op;
                T key;
        */
        Tree_Result  result;
        MPI_Request  isend_handle;
        
        // Receive a request from others
        // MPI_Recv(&request, 1, _TREE_REQUEST, MPI_ANY_SOURCE, RequestTAG, world, MPI_STATUS_IGNORE);
        MPI_Status status;
        // MPI_Recv(&request, 1, _TREE_REQUEST, 0, RequestTAG, world, MPI_STATUS_IGNORE);
        MPI_Recv(&request, 1, _TREE_REQUEST, 0, RequestTAG, world, &status);
        DBG_PRINT(std::cout << "BG recv smth\n";);
        switch(request.op) {
            case RequestType::GET:
                assert(false);
                result.result = internalTree->get(request.key);
                MPI_Isend(&result, 1, _TREE_RESULT, request.src_rank, ResultTAG, world, &isend_handle);
                break;
            case RequestType::REMOVE:
                assert(false);
                internalTree->remove(request.key);
                break;
            case RequestType::STOP:
                DBG_PRINT(std::cout << "Receive STOP" << std::endl;);
                terminateCounter ++;
                /**
                 * After receiving a stop, acknowl
                 * */
                Tree_Request ack_request;
                ack_request.op = RequestType::ACK;
                ack_request.src_rank = rank;
                // MPI_Isend(&ack_request, 1, _TREE_REQUEST, request.src_rank, AckTAG, world, &isend_handle);
                MPI_Send(&ack_request, 1, _TREE_REQUEST, request.src_rank, AckTAG, world);
                DBG_PRINT(std::cout << "Send ACK" << std::endl;);
                break;
            default:
                assert(false);
        }
    }
    DBG_PRINT(std::cout << "bg_thread about to return\n";);
    return nullptr;
}

template <typename T>
DistriBPlusTree<T>::~DistriBPlusTree() {
    DBG_PRINT(std::cout << "in DESTRUCTOR rank: " << RANK_ << std::endl;);
    // Tell other MPI processes "I'm going to stop"
    Tree_Request stop_request;
    stop_request.op = RequestType::STOP;
    stop_request.src_rank = RANK_;

    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        DBG_PRINT(std::cout << "idx: " << idx << " | NUM_PROC: " << NUM_PROC_ << std::endl);
        MPI_Send(&stop_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_);
    }
    DBG_PRINT(std::cout << "outta 1st for loop, trying to recv ACK\n");
    // foreground should rec numProc ACK from others
    // for (size_t idx = 0; idx < NUM_PROC_; idx ++) {
    //     Tree_Request ack_request;
    //     MPI_Recv(&ack_request, 1, TREE_REQUEST, AckTAG, 0, WORLD_, MPI_STATUS_IGNORE);
    // }
    // MPI_Recv(&ack_request, 1, TREE_REQUEST, AckTAG, 0, WORLD_, MPI_STATUS_IGNORE);
    DBG_PRINT(std::cout << "outta 2nd loop, recv all ACK\n");
    /**
     * After getting all N-1 ACK from other processes, we can now tell bg to terminate
     * when appropriate.
     * */
    pthread_barrier_wait(&barrier_mpi);
    if (pthread_join(bg_thread, NULL) != 0) {
        std::cerr << "Error joining bg_thread at rank " << RANK_ << std::endl;
        exit(1);
    }
    pthread_barrier_destroy(&barrier_mpi);
    
}

/**
 * NOTE: We allow multiple keys in a single tree, so insert does not need
 * any cross-process communication. Just insert into the tree directly.
 * */
template <typename T>
void DistriBPlusTree<T>::insert(T key) {
    assert(false);
    internalTree.insert(key);
}

template <typename T>
void DistriBPlusTree<T>::remove(T key) {
    assert(false);
    // Send request to other process: Blocking!
    Tree_Request remove_request;
    remove_request.op = RequestType::REMOVE;
    remove_request.src_rank = RANK_;
    remove_request.key = key;
    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        if (idx == RANK_) continue;
        MPI_Isend(&remove_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_);
    }
    /** 
    * Remove does not get other process's response, because it trusts others to do their jobs correctly.
    * */
    internalTree.remove(key);
}

/**
 * 
 * */
template <typename T>
std::optional<T> DistriBPlusTree<T>::get(T key) {
    assert(false);
    // Send request to other process: Blocking!
    MPI_Request mpi_handle;
    Tree_Request get_request;
    get_request.op = RequestType::GET;
    get_request.src_rank = RANK_;
    get_request.key = key;
    
    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        if (idx == RANK_) continue;
        MPI_Isend(&get_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_, &mpi_handle);
    }

    std::optional<T> local_result = internalTree.get(key);

    // Recv response from other process
    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        if (idx == RANK_) continue;
        Tree_Result get_result;
        MPI_Recv(&get_result, 1, TREE_RESULT, idx, ResultTAG, WORLD_, MPI_STATUS_IGNORE);
        if (local_result.has_value()) continue;
        
        std::optional<T> other_result = get_result.result;
        if (other_result.has_value()) local_result = other_result;
    }

    return local_result;
}

}
