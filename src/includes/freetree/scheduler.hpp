#pragma once
#include "tree.h"
#include "freeNode.hpp"
#include "freetree.hpp"
#include <barrier>
#include <boost/lockfree/queue.hpp>

constexpr int MAXWORKER          = 64;
constexpr int BATCHSIZE          = 64;
constexpr int TERMINATE_FLAG     = 0x40000000;
constexpr double COLLECT_TIMEOUT = 0.0001;
constexpr size_t QUEUE_SIZE = BATCHSIZE * 2;


// Helper Functions
enum PalmStage {
    COLLECT = 0,        // background thread
    SEARCH  = 1,        // worker threads
    DISTRIBUTE = 2,     // background thread
    EXEC_LEAF = 4,      // worker threads
    REDISTRIBUTE = 8,   // background thread
    EXEC_INTERNAL = 16, // worker threads
    EXEC_ROOT = 32      // background thread
};


namespace Tree {
    template <typename T>
    class Scheduler {
        // public fields
        public:
            int numWorker_;
            std::atomic<int> flag = 0;
            std::atomic<int> barrier_cnt = 0;
            std::atomic<bool> bg_move = true;
            std::atomic<bool> bg_notify_worker_terminate = false;

        // Helper structs
        public:
            struct WorkerArgs {
                Scheduler  *scheduler;
                FreeNode<T> *node;
                int threadID;
            };
            

            /**
             * NOTE: LeafOp defines the operations to be exeucted on the leaves
             * NOP    - no operation at all, used to pad the batch to uniform length
             * GET    - get something from the leaf node
             * INSERT - insertt something from the leaf node
             * DELETE - remove something from the leaf node
             * --------------use for internal nodes---------------
             * UPDATE - the internal node need to update (child may have splitted or merged)
             */
            enum TreeOp {NOP, GET, INSERT, DELETE, UPDATE};
            static std::string toString(TreeOp op) {
                switch (op) {
                case TreeOp::NOP: return "NOP";
                case TreeOp::DELETE: return "DELETE";
                case TreeOp::GET: return "GET";
                case TreeOp::INSERT: return "INSERT";
                case TreeOp::UPDATE: return "UPDATE";
                }
                DBG_ASSERT(false);
            }
            

            /**
             * NOTE: Request class contains the LeafOp and argument (of type T)
             */
            struct Request {
                TreeOp           op;
                std::optional<T> key;
                int              idx = -1;
                FreeNode<T>       *curr_node = nullptr;

                void print() {
                    if (key.has_value()) std::cout << toString(op) << ", " << key.value() << " at " << idx;
                    else std::cout << toString(op) << ", NONE at " << idx;
                }
            };
        
        private:
            FreeNode<T> *rootPtr;
            int ORDER_;
            pthread_t workers[MAXWORKER + 1];
            WorkerArgs workers_args[MAXWORKER + 1];
            
            /**
             * This queue handles the request from external client and will be collected into the curr_batch
             * periodically.
             * NOTE: this is only modified by background thread and client threads
             */
            boost::lockfree::queue<Request> request_queue;

            /**
             * This queue handles the request from internal worker threads and will be collected into the
             * curr_batch in the INTERNAL_UPDATE stage (stage 3)
             * NOTE: this is only modified by worker threads and never touched by client
             */
            boost::lockfree::queue<Request> internal_request_queue;

            /**
             * This queue handles the release requests from internal worker threads.
             * All pointers in this thread will be removed during the COLLECT phase to eliminate memory
             * leak.
             * 
             * Linked list will also be fixed in this phase to avoid racing condition.
             */
            boost::lockfree::queue<FreeNode<T>*> internal_release_queue;

            // This array stores the leaf nodes used by each request
            Request curr_batch[BATCHSIZE];
            // This array stores the worker-request assignment (distribution)
            std::vector<Request> request_assign[BATCHSIZE];

            // This barrier synchronize the worker and background thread
            pthread_barrier_t syncBarrier;
            
            struct PrivateWorker;
            struct PrivateBackground;

        public:
            /**
             * NOTE: 
             * Will spawn 1 background thread monitoring the Request queue
             *      spawn n worker threads executing the Request queue
             */
            Scheduler(int numWorker, FreeNode<T> *rootPtr, int order): 
                numWorker_(numWorker), rootPtr(rootPtr), ORDER_(order),
                request_queue(boost::lockfree::queue<Request>(QUEUE_SIZE)), 
                internal_request_queue(boost::lockfree::queue<Request>(BATCHSIZE)),
                internal_release_queue(boost::lockfree::queue<FreeNode<T>*>(BATCHSIZE * numWorker * 4))
            {                
                pthread_barrier_init(&syncBarrier, NULL, numWorker);

                assert (numWorker_ < MAXWORKER);
                setStage(flag, PalmStage::COLLECT);

                workers_args[numWorker_].scheduler = this;
                workers_args[numWorker_].threadID = numWorker_;
                workers_args[numWorker_].node = rootPtr;
                pthread_create(&workers[numWorker_], NULL, PrivateBackground::background_loop, &workers_args[numWorker_]);
                
                // launch threads
                for (size_t idx = 0; idx < numWorker_; idx ++) {
                    workers_args[idx].scheduler = this;
                    workers_args[idx].threadID  = idx;
                    workers_args[idx].node      = rootPtr;
                    pthread_create(&workers[idx], NULL, PrivateWorker::worker_loop, &workers_args[idx]);
                }
            };

            void waitToExit() {
                DBG_PRINT(std::cout << "Scheduler get Terminate signal, will exit after current batch" << std::endl);
                while (!request_queue.empty());
                PrivateBackground::release_nodes(this);
                
                setTerminate(flag, true);
                for (size_t i = 0; i < numWorker_ + 1; i ++) {
                    pthread_join(workers[i], NULL);
                }

                pthread_barrier_destroy(&syncBarrier);
            }

            /**
             * NOTE: Exposed synchronous submit_request API to client.
             * Will busy spin if the queue is full right now.
             */
            void submit_request(Request request) {
                /**
                 * LOCK FREE REQUEST_QUEUE
                 * 
                 * NOTE: the submid_request(...) API may be called by multiple threads
                 * in the client, we use the while loop below to ensure that no conflict
                 * write will occur on the request_queue. 
                 */
                while (!request_queue.bounded_push(request)) {};
            }

        private:
            static inline bool isTerminate(std::atomic<int> &flag) {
                return flag & TERMINATE_FLAG;
            }

            static inline PalmStage getStage(std::atomic<int> &flag) {
                return PalmStage(flag & (~TERMINATE_FLAG));
            }

            static inline void setTerminate(std::atomic<int> &flag, bool terminate) {
                if (terminate) flag |= TERMINATE_FLAG;
                else flag &= ~TERMINATE_FLAG;
            }

            static inline void setStage(std::atomic<int> &flag, PalmStage stage) {
                flag = (flag & (TERMINATE_FLAG)) | stage;
            }

        public:
            void debugPrint() {
                std::cout << "[Free B+ Tree]" << std::endl;
                if (rootPtr->numChild() == 0) {
                    std::cout << "(Empty)" << std::endl;
                    return;
                }
                FreeNode<T>* src = rootPtr;
                int level_cnt = 0;
                do {
                    FreeNode<T>* ptr = src;
                    std::cout << level_cnt << "\t| ";
                    while (ptr != nullptr) {
                        ptr->printKeys();
                        std::cout << "<->";
                        ptr = ptr->next;
                    }
                    level_cnt ++;
                    std::cout << std::endl;
                    if (src->numChild() == 0) break;
                    src = src->children[0];
                } while (true);
                
                std::cout << std::endl;
            }
    };
}
