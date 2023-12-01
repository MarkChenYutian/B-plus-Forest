#pragma once
#include "tree.h"
#include "freetree.hpp"
#include <boost/lockfree/queue.hpp>

constexpr int MAXWORKER          = 65;
constexpr int BATCHSIZE          = 3;
constexpr int TERMINATE_FLAG     = 0x40000000;
constexpr double COLLECT_TIMEOUT = 0.001;
constexpr size_t QUEUE_SIZE = (1 << 10);


// Helper Functions
enum PalmStage {
    COLLECT = 0,        // background thread
    SEARCH  = 1,        // worker threads
    DISTRIBUTE = 2,     // background thread
    EXEC_LEAF = 4,      // worker threads
    REDISTRIBUTE = 8,   // background thread

    CYCLE_END = 1024    // Cycle End
};


namespace Tree {
    template <typename T>
    class Scheduler {
        // public fields
        public:
            int numWorker_;
            std::atomic<int> flag = 0;
            std::atomic<int> barrier_cnt = 0;

        // Helper structs
        public:
            struct WorkerArgs {
                Scheduler  *scheduler;
                SeqNode<T> *node;
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
                assert(false);
            }
            

            /**
             * NOTE: Request class contains the LeafOp and argument (of type T)
             */
            struct Request {
                TreeOp           op;
                std::optional<T> key;
                int              idx = -1;
                SeqNode<T>       *curr_node = nullptr;

                void print() {std::cout << toString(op) << ", " << key << " at " << idx;}
            };
        
        private:
            SeqNode<T> *rootPtr;
            int ORDER_;
            pthread_t workers[MAXWORKER];
            WorkerArgs workers_args[MAXWORKER];
            
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

            // This array stores the leaf nodes used by each request
            Request curr_batch[BATCHSIZE];
            // This array stores the worker-request assignment (distribution)
            std::vector<Request> request_assign[BATCHSIZE];
            
            struct PrivateWorker;
            struct PrivateBackground;

        public:
            /**
             * NOTE: 
             * Will spawn 1 background thread monitoring the Request queue
             *      spawn n worker threads executing the Request queue
             */
            Scheduler(int numWorker, SeqNode<T> *rootPtr, int order): 
                numWorker_(numWorker), rootPtr(rootPtr), ORDER_(order),
                request_queue(boost::lockfree::queue<Request>(QUEUE_SIZE)), 
                internal_request_queue(boost::lockfree::queue<Request>(BATCHSIZE))
            {
                assert (numWorker_ + 1 < MAXWORKER);
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
                DBG_PRINT(std::cout << "Exit" << std::endl);
                while (!request_queue.empty());
                
                setTerminate(flag, true);
                for (size_t i = 0; i < numWorker_ + 1; i ++) {
                    pthread_join(workers[i], NULL);
                }
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
    };
}
