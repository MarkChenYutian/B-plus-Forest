#pragma once

#include <optional>
#include <pthread.h>
#include "mpi.h"
#include "tree.h"
#include "fineTree/fineTree.hpp"
#include "fineTree/fineNode.hpp"
#include "distriTree.h"

constexpr int RequestTAG  = 1;
constexpr int ResultTAG   = 2;
constexpr int AckTAG      = 3;


namespace Tree {

template <typename T>
DistriBPlusTree<T>::DistriBPlusTree(int order, MPI_Comm world): 
ORDER_(order), WORLD_(world), internalTree(FineLockBPlusTree<T>(order)) {
    MPI_Comm_rank(WORLD_, &RANK_);
    MPI_Comm_size(WORLD_, &NUM_PROC_);

    MPI_Type_contiguous(sizeof(Tree_Request), MPI_CHAR, &TREE_REQUEST);
    MPI_Type_contiguous(sizeof(Tree_Result), MPI_CHAR, &TREE_RESULT);

    MPI_Type_commit(&TREE_REQUEST);
    MPI_Type_commit(&TREE_RESULT);
    
    bg_args = new Background_Args();
    bg_args->internalTree = &internalTree;
    bg_args->world        = WORLD_;
    bg_args->TREE_REQUEST = TREE_REQUEST;
    bg_args->TREE_RESULT  = TREE_RESULT;
    bg_args->numProc      = NUM_PROC_;
    bg_args->rank         = RANK_;

    pthread_create(&bg_thread, NULL, MPI_background, bg_args);
}

template <typename T>
void *DistriBPlusTree<T>::MPI_background(void *args) {
    Background_Args *bg_args_recv = static_cast<Background_Args*>(args);

    int nProc = bg_args_recv->numProc;
    int rank  = bg_args_recv->rank;
    MPI_Comm world = bg_args_recv->world;
    MPI_Datatype _TREE_REQUEST = (bg_args_recv->TREE_REQUEST);
    MPI_Datatype _TREE_RESULT  = (bg_args_recv->TREE_RESULT);
    FineLockBPlusTree<T>* internalTree = bg_args_recv->internalTree;
    DBG_PRINT(std::cout << "BG Thread Start with rank: " << rank << std::endl;);
    
    // Count number of process sending "STOP" Request. Terminate this if and only if 
    // all processes STOPs.
    int terminateCounter = 0;
    MPI_Request  isend_handle;
    
    while (true) {
        if (terminateCounter == nProc) break;
        Tree_Request request;
        Tree_Result  result;

        // Receive a request from others
        MPI_Recv(&request, 1, _TREE_REQUEST, MPI_ANY_SOURCE, RequestTAG, world, MPI_STATUS_IGNORE);
        switch(request.op) {
            case RequestType::GET:
                result.result = internalTree->get(request.key);
                MPI_Isend(&result, 1, _TREE_RESULT, request.src_rank, ResultTAG, world, &isend_handle);
                break;
            case RequestType::REMOVE:
                internalTree->remove(request.key);
                break;
            case RequestType::STOP:
                terminateCounter ++;
                /**
                 * After receiving a stop, acknowledge the sender.
                 * */
                Tree_Request ack_request;
                ack_request.op = RequestType::ACK;
                ack_request.src_rank = rank;
                MPI_Isend(&ack_request, 1, _TREE_REQUEST, request.src_rank, AckTAG, world, &isend_handle);
                // MPI_Send(&ack_request, 1, _TREE_REQUEST, request.src_rank, AckTAG, world);
                break;
            default:
                assert(false);
        }
    }
    return nullptr;
}

template <typename T>
DistriBPlusTree<T>::~DistriBPlusTree() {
    // Tell other MPI processes "I'm going to stop"
    Tree_Request stop_request;
    DBG_PRINT(std::cout << "Subtree rank "<< RANK_ << " scheduled to stop." << std::endl;);
    stop_request.op = RequestType::STOP;
    stop_request.src_rank = RANK_;

    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        MPI_Isend(&stop_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_, &garbage_request);
    }
    // foreground should rec numProc ACK from others
    for (size_t idx = 0; idx < NUM_PROC_; idx ++) {
        Tree_Request ack_request;
        MPI_Recv(&ack_request, 1, TREE_REQUEST, idx, AckTAG, WORLD_, MPI_STATUS_IGNORE);
    }

    /**
     * After getting all N ACK from other processes, bg will terminate.
     * */
    if (pthread_join(bg_thread, NULL) != 0) {
        std::cerr << "Error joining bg_thread at rank " << RANK_ << std::endl;
        exit(1);
    }
    
    delete bg_args;
}

/**
 * NOTE: We allow multiple keys in a single tree, so insert does not need
 * any cross-process communication. Just insert into the tree directly.
 * */
template <typename T>
void DistriBPlusTree<T>::insert(T key) {
    internalTree.insert(key);
}

template <typename T>
void DistriBPlusTree<T>::remove(T key) {
    // Send request to other process: Blocking!
    Tree_Request remove_request;
    remove_request.op = RequestType::REMOVE;
    remove_request.src_rank = RANK_;
    remove_request.key = key;
    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        if (idx == RANK_) continue;
        MPI_Isend(&remove_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_, &garbage_request);
    }
    /** 
     * Remove does not get other process's response, because it trusts others to do their jobs correctly.
     * */
    internalTree.remove(key);
}

template <typename T>
std::optional<T> DistriBPlusTree<T>::get(T key) {
    // Send request to other process: Blocking!
    Tree_Request get_request;
    get_request.op = RequestType::GET;
    get_request.src_rank = RANK_;
    get_request.key = key;
    
    for (size_t idx = 0; idx < NUM_PROC_; idx++) {
        if (idx == RANK_) continue;
        MPI_Isend(&get_request, 1, TREE_REQUEST, idx, RequestTAG, WORLD_, &garbage_request);
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
