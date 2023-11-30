#pragma once
#include "tree.h"

constexpr int BATCHSIZE = 3;
constexpr int MAXWORKER = 65;
constexpr int TERMINATE_FLAG = 0x80000000;
enum PalmStage {
    COLLECT = 0,
    SEARCH  = 1,
    DISTRIBUTE = 2,
};

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

namespace Tree {
    template <typename T>
    class FreeBPlusTree {
        public:
            /**
             * NOTE: These are all async APIs since the lock-free B+ tree
             * will execute all the requests in an asynchronous batch operation
             */
            FreeBPlusTree(int order, int numWorker, SeqNode<T> *rootPtr);
            ~FreeBPlusTree() {
                scheduler_.waitToExit();
            }
            void insert(T key);
            void remove(T key);
            void get(T key);

        private:
            /**
             * NOTE: LeafOp defines the operations to be exeucted on the leaves
             * GET    - get something from the leaf node
             * INSERT - insertt something from the leaf node
             * DELETE - remove something from the leaf node
             * --------------use for internal nodes---------------
             * UPDATE - the internal node need to update (child may have splitted or merged)
             */
            enum TreeOp {GET, INSERT, DELETE, UPDATE};

            /**
             * NOTE: Request class contains the LeafOp and argument (of type T)
             */
            struct Request {
                TreeOp op;
                T key;
                int idx = -1;
            };

            
            class Scheduler {
                public:
                    struct WorkerArgs {
                        Scheduler  *scheduler;
                        SeqNode<T> *node;
                        int threadID;
                    };
                    
                    /**
                     * NOTE: 
                     * Will spawn 1 background thread monitoring the Request queue
                     *      spawn n worker threads executing the Request queue
                     */
                    Scheduler(int numWorker, SeqNode<T> *rootPtr): numWorker_(numWorker), rootPtr(rootPtr) {
                        assert (numWorker_ + 1 < MAXWORKER);
                        setStage(flag, PalmStage::COLLECT);

                        // launch threads
                        for (size_t idx = 0; idx < numWorker_; idx ++) {
                            workers_args[idx].scheduler = this;
                            workers_args[idx].threadID  = idx;
                            workers_args[idx].node      = rootPtr;
                            pthread_create(&workers[idx], NULL, worker_loop, &workers_args[idx]);
                        }

                        workers_args[numWorker_].scheduler = this;
                        workers_args[numWorker_].threadID = numWorker_;
                        workers_args[numWorker_].node = rootPtr;
                        pthread_create(&workers[numWorker_], NULL, background_loop, &workers_args[numWorker_]);

                    };

                    void waitToExit() {
                        std::cout << "Exit" << std::endl;
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
                        while (queue_length >= BATCHSIZE) {}
                        request_queue[queue_length] = request;
                        request_queue[queue_length].idx = queue_length;
                        queue_length ++;
                    }

                private:
                    SeqNode<T> *rootPtr;
                    pthread_t workers[MAXWORKER];
                    WorkerArgs workers_args[MAXWORKER];
                    
                    std::atomic<int> queue_length;
                    Request request_queue[BATCHSIZE];
                    /**
                     * This array stores the leaf nodes used by each request
                     */
                    SeqNode<T> *search_leaf[BATCHSIZE];
                    
                    
                public:
                    int numWorker_;
                    std::atomic<int> flag = 0;
                    std::atomic<int> barrier_cnt = 0;

                private:
                    static void *background_loop(void *args) {
                        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
                        const int threadID = wargs->threadID;
                        Scheduler *scheduler = wargs->scheduler;
                        SeqNode<T> *rootPtr = wargs->node;
                        const int numWorker = scheduler->numWorker_;

                        while (!isTerminate(scheduler->flag)) {
                            setStage(scheduler->flag, PalmStage::COLLECT);
                            // TODO: BATCHSIZE && TIME 
                            while (scheduler->queue_length < BATCHSIZE && !isTerminate(scheduler->flag)) {}
                            scheduler->barrier_cnt = 0;

                            setStage(scheduler->flag, PalmStage::SEARCH);
                            while (scheduler->barrier_cnt < numWorker && !isTerminate(scheduler->flag)) {}
                            for (size_t i = 0; i < BATCHSIZE; i++) {
                                assert(scheduler->search_leaf[i]->isLeaf);
                                scheduler->search_leaf[i]->printKeys();
                                std::cout << std::endl;
                            }
                            scheduler->queue_length = 0;

                            // setStage(scheduler->flag, PalmStage::DISTRIBUTE)
                            // scheduler->queue_length = 0;
                        }
                        return nullptr;
                    }

                    static void *worker_loop(void *args) {
                        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
                        const int threadID = wargs->threadID;
                        Scheduler *scheduler = wargs->scheduler;
                        /**
                         * CAUTION: The rootPtr is dynamic and subject to change (B+tree depth may increase)
                         */
                        SeqNode<T> *rootPtr = wargs->node;

                        std::vector<Request> privateQueue;

                        while (!isTerminate(scheduler->flag)) {
                            // TODO: CHANGE rootPtr
                            
                            privateQueue.clear();
                            while (
                                getStage(scheduler->flag) == PalmStage::COLLECT 
                                && !isTerminate(scheduler->flag)
                            ) {}
                            
                            for (size_t i = threadID; i < BATCHSIZE; i+=scheduler->numWorker_) {
                                privateQueue.push_back(scheduler->request_queue[i]);
                            }
                            // Searching leaf node for each Request. All read operations so no lock required
                            search(privateQueue, rootPtr, scheduler);
                            scheduler->barrier_cnt ++;

                            

                            
                        }
                        
                        return nullptr;
                    }
                    
                    /**
                     * Search leaves in lock-free pattern since all threads are only reading at this time.
                     */
                    inline static void search(
                        std::vector<Request> &privateQueue, 
                        SeqNode<T> *rootPtr, 
                        Scheduler *scheduler
                    ) {
                        for (const Request &request : privateQueue) {
                            SeqNode<T>* leafNode = lockFreeFindLeafNode(rootPtr, request.key);
                            std::cout << "request.idx: " << request.idx << std::endl;
                            assert (request.idx < BATCHSIZE && request.idx >= 0);
                            scheduler->search_leaf[request.idx] = leafNode;
                        }
                    }

                    // background_loop: leaf
                    void distribute() { // search_leaf
                        
                    }
                    void redistribute(); // background_loop: internal    
                    void modify_root(); // background_loop
            };
        private:
            Scheduler scheduler_;
            SeqNode<T> rootPtr;
            int ORDER_;
            int size_;
        
        private:
            static SeqNode<T>* lockFreeFindLeafNode(SeqNode<T>* node, T key) {
                while (!node->isLeaf) {
                    /** getGTKeyIdx will have index = 0 if node is dummy node */
                    size_t index = node->getGtKeyIdx(key);
                    node = node->children[index];
                }
                return node;
            }

        };
}
