#pragma once
#include "tree.h"
#include "timing.h"
#include <boost/lockfree/queue.hpp>

#ifdef DEBUG
std::mutex print_mutex;
#define DBG_PRINT(arg) \
    do { \
        std::lock_guard<std::mutex> lock(print_mutex); \
        arg; \
    } while (0);
#else
#define DBG_PRINT(arg) {}
#endif


/**
 * Using lockfree queue from Boost lilbrary
 * https://www.boost.org/doc/libs/1_76_0/doc/html/boost/lockfree/queue.html
 */

constexpr int BATCHSIZE          = 3;
constexpr int MAXWORKER          = 65;
constexpr int TERMINATE_FLAG     = 0x40000000;
constexpr double COLLECT_TIMEOUT = 0.001;
constexpr size_t QUEUE_SIZE = (1 << 10);


enum PalmStage {
    COLLECT = 0,        // background thread
    SEARCH  = 1,        // worker threads
    DISTRIBUTE = 2,     // background thread
    EXEC_LEAF = 4,      // worker threads
    REDISTRIBUTE = 8,   // background thread

    CYCLE_END = 1024    // Cycle End
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
                TreeOp op;
                T key;
                int idx = -1;
                SeqNode<T> *curr_node = nullptr;

                void print() {
                    std::cout << toString(op) << ", " << key << " at " << idx;
                }
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
                    Scheduler(int numWorker, SeqNode<T> *rootPtr): 
                        numWorker_(numWorker), rootPtr(rootPtr), 
                        request_queue(boost::lockfree::queue<Request>(QUEUE_SIZE)) 
                    {
                        assert (numWorker_ + 1 < MAXWORKER);
                        setStage(flag, PalmStage::COLLECT);

                        workers_args[numWorker_].scheduler = this;
                        workers_args[numWorker_].threadID = numWorker_;
                        workers_args[numWorker_].node = rootPtr;
                        pthread_create(&workers[numWorker_], NULL, background_loop, &workers_args[numWorker_]);

                        // launch threads
                        for (size_t idx = 0; idx < numWorker_; idx ++) {
                            workers_args[idx].scheduler = this;
                            workers_args[idx].threadID  = idx;
                            workers_args[idx].node      = rootPtr;
                            pthread_create(&workers[idx], NULL, worker_loop, &workers_args[idx]);
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
                    SeqNode<T> *rootPtr;
                    pthread_t workers[MAXWORKER];
                    WorkerArgs workers_args[MAXWORKER];
                    
                    boost::lockfree::queue<Request> request_queue;
                    /**
                     * This array stores the leaf nodes used by each request
                     */
                    Request curr_batch[BATCHSIZE];
                    /**
                     * This array stores the worker-request assignment (distribution)
                     */
                    std::vector<Request> request_assign[BATCHSIZE];
                    /**
                     * This map is used to distribute requests to each leaf node in freeTree
                     */
                    std::unordered_map<SeqNode<T> *, std::vector<Request>> assign_node_to_thread;
                    
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
                            {
                                setStage(scheduler->flag, PalmStage::COLLECT);

                                Timer timer = Timer();
                                size_t request_idx = 0;
                                while (request_idx < BATCHSIZE) {
                                    Request req = {TreeOp::NOP};
                                    while (!scheduler->request_queue.pop(req) && timer.elapsed() < COLLECT_TIMEOUT){};
                                    req.idx = request_idx;
                                    scheduler->curr_batch[request_idx++] = req;
                                }
                            }

                            {
                                scheduler->barrier_cnt = 0;
                                setStage(scheduler->flag, PalmStage::SEARCH);
                                while (scheduler->barrier_cnt < numWorker) {}
                                #ifdef DEBUG
                                DBG_PRINT(std::cout << "\n-------SEARCH RESULT-------\n");
                                for (size_t i = 0; i < BATCHSIZE; i++) {
                                    if (scheduler->curr_batch[i].op == TreeOp::NOP) {
                                        DBG_PRINT(std::cout << "NOP" << std::endl;);
                                    } else {
                                        DBG_PRINT(
                                            scheduler->curr_batch[i].curr_node->printKeys(); 
                                            std::cout << ", " << scheduler->curr_batch[i].key << std::endl;
                                        );
                                    }
                                }
                                DBG_PRINT(std::cout << "----------------------------\n");
                                #endif
                                setStage(scheduler->flag, PalmStage::DISTRIBUTE);
                                scheduler->assign_node_to_thread.clear();
                                distribute(scheduler);
                                
                            }
                            
                            {
                                scheduler->barrier_cnt = 0;
                                setStage(scheduler->flag, PalmStage::EXEC_LEAF);
                                while(scheduler->barrier_cnt < numWorker) {}
                            }
                        }
                        return nullptr;
                    }

                    static void *worker_loop(void *args) {
                        WorkerArgs *wargs = static_cast<WorkerArgs*>(args);
                        const int threadID = wargs->threadID;
                        Scheduler *scheduler = wargs->scheduler;
                        const int numWorker = scheduler->numWorker_;
                        /**
                         * CAUTION: The rootPtr is dynamic and subject to change (B+tree depth may increase)
                         */
                        SeqNode<T> *rootPtr = wargs->node;

                        std::vector<Request> privateQueue;

                        while (!isTerminate(scheduler->flag)) {
                            // TODO: CHANGE rootPtr
                            
                            privateQueue.clear();
                            {
                                while (getStage(scheduler->flag) != PalmStage::SEARCH) {}
                                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                                    if (scheduler->curr_batch[i].op == TreeOp::NOP) {
                                        continue;
                                    }
                                    privateQueue.push_back(scheduler->curr_batch[i]);
                                }
                                assert(privateQueue.size() <= BATCHSIZE / numWorker + 1);
                                // Searching leaf node for each Request. All read operations so no lock required
                                search(privateQueue, rootPtr, scheduler);
                                scheduler->barrier_cnt ++;
                                // while(scheduler->barrier_cnt < numWorker);
                            }
                            
                            {
                                while (getStage(scheduler->flag) != PalmStage::EXEC_LEAF) {}
                                for (size_t i = threadID; i < BATCHSIZE; i+=numWorker) {
                                    leaf_execute(scheduler->request_assign[i]);
                                }
                                scheduler->barrier_cnt ++;
                                
                            }

                            // 
                            while(scheduler->barrier_cnt < numWorker);
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
                        for (Request &request : privateQueue) {
                            SeqNode<T>* leafNode = lockFreeFindLeafNode(rootPtr, request.key);
                            scheduler->curr_batch[request.idx].curr_node = leafNode;
                        }
                    }

                    // background_loop: leaf
                    inline static void distribute(Scheduler *scheduler) { // background thread deal with search_leaf 
                        for (size_t i = 0; i < BATCHSIZE; i++) {
                            Request req = scheduler->curr_batch[i];
                            if (req.op == TreeOp::NOP) continue;

                            SeqNode<T> *node = req.curr_node;
                            scheduler->assign_node_to_thread[node].push_back(req);
                        }
                        
                        assert(scheduler->assign_node_to_thread.size() < BATCHSIZE);
                        #ifdef DEBUG
                        DBG_PRINT(std::cout << "\n-------ASSIGN_NODE_TO_THREAD-------\n");
                        for (auto elem: scheduler->assign_node_to_thread) { // FOR DEBUG
                            SeqNode<T> *node = elem.first;
                            std::vector<Request> requests = elem.second;
                            DBG_PRINT(node->printKeys(); std::cout << "\n";);
                            for (size_t i = 0; i < requests.size(); i++) {
                                DBG_PRINT(requests[i].print(); std::cout << "\n";);
                            }
                        }
                        DBG_PRINT(std::cout << "-------------------------------------\n");
                        #endif
                        
                        size_t idx = 0;
                        // Fill the slots with Request (task) queue
                        for (auto elem : scheduler->assign_node_to_thread) {
                            scheduler->request_assign[idx] = elem.second;
                            idx ++;
                        }
                        // For remaining slots, clear them up
                        while (idx < BATCHSIZE) {
                            scheduler->request_assign[idx].clear();
                            idx ++;
                        }
                    }

                    inline static void leaf_execute(std::vector<Request> &requests_in_the_same_node) {
                        for (Request &req : requests_in_the_same_node) {
                            SeqNode<T> *leafNode = req.curr_node;
                            T key = req.key;
                            auto it = std::lower_bound(leafNode->keys.begin(), leafNode->keys.end(), key);
                            
                            switch (req.op)
                            {
                            case TreeOp::INSERT:
                                break;
                            case TreeOp::GET:
                                break;
                            case TreeOp::DELETE:
                                if (it != leafNode->keys.end() && *it == key) {
                                    leafNode->keys.erase(it);
                                }
                                break;
                            case TreeOp::NOP:
                                assert(false);
                            case TreeOp::UPDATE:
                                assert(false);
                            default:
                                assert(false);
                            }
                        }
                        
                        if (requests_in_the_same_node.size() > 0) {
                            DBG_PRINT(
                                requests_in_the_same_node[0].curr_node -> printKeys();
                            )
                        }
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
