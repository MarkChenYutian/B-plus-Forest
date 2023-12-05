#pragma once
#include "tree.h"

namespace Tree {
    template<typename T>
    class DistriBPlusTree {
        private:
            int ORDER_;
            int RANK_;
            int NUM_PROC_;
            MPI_Comm WORLD_;
            FineLockBPlusTree<T> internalTree;
        
        public:
            DistriBPlusTree(int order, MPI_Comm world);
            ~DistriBPlusTree();

            bool debug_checkIsValid(bool verbose);
            int size();

            void insert(T key);
            void remove(T key);
            void print();
            std::optional<T> get(T key);
            std::vector<T> toVec();
        
        private:
            /**
             * GET, REMOVE - literally get and remove request
             * STOP - Tell other distributed trees "I'm going to terminate"
             * */
            enum RequestType {GET, REMOVE, STOP, ACK};
            struct Tree_Request {
                int src_rank;
                RequestType op;
                T key;
            };

            struct Tree_Result {
                std::optional<T> result;
            };

            struct Background_Args {
                int numProc;
                int rank;
                MPI_Comm world;
                MPI_Datatype TREE_REQUEST;
                MPI_Datatype TREE_RESULT;
                FineLockBPlusTree<T> *internalTree;
            };

            MPI_Datatype TREE_REQUEST;
            MPI_Datatype TREE_RESULT;
            MPI_Request garbage_request;
            Background_Args *bg_args;
            pthread_t bg_thread;
        
        private:
            static void *MPI_background(void *args);
    };
};
