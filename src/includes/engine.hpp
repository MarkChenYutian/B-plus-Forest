#pragma once
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <type_traits>
#include <barrier>
#include "node.hpp"

pthread_barrier_t barrier;
pthread_barrier_t barrier2;

enum TestOp {INSERT, REMOVE, GET, BARRIER};

namespace Engine {

struct TestEntry {
    int value;
    TestOp op;
    std::optional<int> expect;
    bool isBarrier() {return op == TestOp::BARRIER;}

    void print() {
        if (op == TestOp::GET) std::cout << "GET ";
        else if (op == TestOp::INSERT) std::cout << "INSERT ";
        else std::cout << "DELETE ";

        if (expect.has_value()) {
            std::cout << "\t" << value << ", expect: " << expect.value() << std::endl;
        } else {
            std::cout << "\t" << value << ", expect: NONE" << std::endl;
        }
    }

    void parse(const std::string &line) {
        if (line == "BARRIER") {
            op = TestOp::BARRIER;
            return;
        }

        std::istringstream iss(line);
        std::string tok;

        std::getline(iss, tok, ',');
        if (tok == "I") op = TestOp::INSERT;
        else if (tok == "G") op = TestOp::GET;
        else if (tok == "D") op = TestOp::REMOVE;

        std::getline(iss, tok, ',');
        value = std::stoi(tok);

        std::getline(iss, tok, ',');
        if (tok == "NONE") {
            expect = std::nullopt;
        } else {
            expect = std::stoi(tok);
        }
    }
};

template <typename T, typename K>
struct WorkerArgs {
    T *concurrent_tree;
    T *seq_tree;
    Tree::SeqBPlusTree<K>  *real_seq_tree;
    std::vector<TestEntry> *currCase;
    int threadID;
    int threadNum;
};

template <typename T, typename K>
class correctnessEngine
{
static_assert(std::is_base_of<Tree::ITree<K>, T>::value, "T must inherit from Tree::ITree");

private:
    int order;
    int numProcess;
    std::vector<pid_t> pids;
    std::vector<std::string> paths;
    std::vector<TestEntry>   currCase;

public:
    correctnessEngine(std::vector<std::string> paths, int order, int numProcess): 
        paths(paths), order(order), numProcess(numProcess)
        {};

    void Run() {
        for (int i = 0; i < numProcess; i ++) {
            pids.push_back(fork());
            if (pids[i] == 0) {
                for (size_t j = i; j < paths.size(); j += numProcess) {
                    const auto testCase = paths[j];
                    loadTestCase(testCase);
                    {
                        auto tree = T(order);
                        bool pass = runTestCase(tree);
                        if (pass) std::cout << "\r\033[1;32mPASS Case " << j << " " << testCase << "\033[0m" << std::endl;
                        else std::cout << "\r\033[1;31mFAIL Case " << j << " " << testCase << "\033[0m" << std::endl;
                    }
                }
                _exit(0);
            }
        }

        int wait_state;
        while (wait(&wait_state) > 0) {}
    }

private:
    void loadTestCase(const std::string &filePath) {
        currCase.clear();
        std::ifstream file(filePath);
        std::string   line;
        if (!file) {
            std::cerr << "Unable to load file at " << filePath;
            assert(false);
        }
        while (std::getline(file, line)) {
            // parseLine(line);
            TestEntry entry;
            entry.parse(line);
            currCase.push_back(entry);
        }
    }

    bool runTestCase(T &tree) {
        for (size_t idx = 0; idx < currCase.size(); idx ++) {
            TestEntry entry = currCase[idx];
            // tree.print();
            // std::cout << "<<<<<<<<<<<<<<<<<<<<<<<<\n\n" << std::endl;
            // entry.print();

            if (idx % 1000 == 0 && !tree.debug_checkIsValid(false)) return false;
            // if (!tree.debug_checkIsValid(true)) return false;

            bool hasKey;
            std::optional<int> key;

            switch (entry.op){
            case TestOp::INSERT:
                tree.insert(entry.value);
                break;
            
            case TestOp::REMOVE:
                hasKey = tree.remove(entry.value);
                if (hasKey != entry.expect.has_value()) {
                    std::cout << "\n(REMOVE) FAILED AT LINE " << idx << " output: " << hasKey <<std::endl;
                    entry.print();
                    tree.print();
                    return false;
                }
                break;

            case TestOp::GET:
                key = tree.get(entry.value);
                if (key.has_value() != entry.expect.has_value()) {
                    std::cout << "\n(GET, CASE1) FAILED AT LINE " << idx << std::endl;
                    entry.print();
                    tree.print();
                    return false;
                }
                if (key.has_value() && (key.value() != entry.expect.value())) {
                    std::cout << "\n(GET, CASE2) FAILED AT LINE " << idx << std::endl;
                    entry.print();
                    return false;
                }
                break;
            }
        }
        return true;
    }
};


template <typename T, typename K>
class threadingEngine
{
static_assert(std::is_base_of<Tree::ITree<K>, T>::value, "T must inherit from Tree::ITree");

private:
    int order;
    int threadNum;
    std::vector<std::string> paths;
    std::vector<TestEntry>   currCase;

public:
    threadingEngine(std::vector<std::string> paths, int order, int threadNum): 
        paths(paths), order(order), threadNum(threadNum)
        {
            currCase = std::vector<TestEntry>();
        };

    void Run() {
        for (size_t i = 0; i < paths.size(); i ++) {
            const auto testCase = paths[i];
            std::cout << "Running " << testCase << " ..." << std::flush;
            loadTestCase(testCase);
            {
                auto concurrent_tree = T(order);
                auto seq_tree = T(order);
                WorkerArgs<T, K> args[threadNum + 2];
                pthread_t threads[threadNum + 2];

                for (int threadId = 0; threadId < threadNum + 2; threadId ++) {
                    args[threadId].concurrent_tree = &concurrent_tree;
                    args[threadId].seq_tree  = &seq_tree;
                    args[threadId].currCase  = &currCase;
                    args[threadId].threadID  = threadId;
                    args[threadId].threadNum = threadNum;
                }

                // initialize the barrier with the number of threads
                pthread_barrier_init(&barrier, NULL, threadNum + 2);
                pthread_barrier_init(&barrier2, NULL, threadNum + 2);
                
                for (int threadId = 0; threadId < threadNum + 2; threadId ++) {
                    if (threadId < threadNum + 1) {
                        // First threadNum threads are concurrent Tree Thread
                        // threadNum+1-th thread is seq Tree Thread
                        pthread_create(&threads[threadId], NULL, runTestCase, &args[threadId]);
                    } else {
                        pthread_create(&threads[threadId], NULL, runReadCheck, &args[threadId]);
                    }
                    
                }

                for (int i = 0; i < threadNum + 2; i++){
                    if (pthread_join(threads[i], NULL) != 0) {
                        std::cerr << "Error joining thread " << i << std::endl;
                        exit(1);
                    }
                }

                // Destroy the barrier, condition, mutex
                pthread_barrier_destroy(&barrier);
                pthread_barrier_destroy(&barrier2);

                bool pass = concurrent_tree.debug_checkIsValid(false);
                if (pass) std::cout << "\r\033[1;32mPASS Case " << i << " " << testCase << "\033[0m" << std::endl;
                else std::cout << "\r\033[1;31mFAIL Case " << i << " " << testCase << "\033[0m" << std::endl;
            }
        }
    }

private:
    void loadTestCase(const std::string &filePath) {
        currCase.clear();

        std::ifstream file(filePath);
        std::string   line;
        if (!file) {
            std::cerr << "Unable to load file at " << filePath;
            assert(false);
        }

        int counter = 0;

        while (std::getline(file, line)) {
            TestEntry entry;
            entry.parse(line);
            currCase.emplace_back(entry);
        }
    }

    static void *runTestCase(void* arg) {
        WorkerArgs<T, K> *warg = static_cast<WorkerArgs<T, K>*>(arg);
        
        T *tree;
        std::vector<TestEntry> *currCase = warg->currCase;
        int thread_id = warg->threadID;
        DBG_ASSERT(thread_id < warg->threadNum + 1);

        bool isConcurrentThread = thread_id < warg->threadNum;
        
        if (isConcurrentThread) tree = warg->concurrent_tree;
        else tree = warg->seq_tree;

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            TestEntry entry = currCase->at(idx);

            if (isConcurrentThread && 
                !entry.isBarrier() && 
                (entry.value % warg->threadNum != thread_id)
            ) {
                continue;
            }
            
            bool hasKey;
            std::optional<int> key;

            switch (entry.op){
            case TestOp::INSERT:
                key = tree->get(entry.value);
                if (key.has_value()) break;
                tree->insert(entry.value);
                break;
            
            case TestOp::REMOVE:
                hasKey = tree->remove(entry.value);
                break;

            case TestOp::GET:
                key = tree->get(entry.value);
                break;
            
            case TestOp::BARRIER:
                pthread_barrier_wait(&barrier);
                pthread_barrier_wait(&barrier2);
            }
        }
        return nullptr;
    }

    static void *runReadCheck(void *arg) {
        WorkerArgs<T, K> *warg = static_cast<WorkerArgs<T, K>*>(arg);
        int thread_id = warg->threadID;
        assert(thread_id == warg->threadNum + 1);

        T *concurrent_tree = warg->concurrent_tree;
        T *seq_tree = warg->seq_tree;
        std::vector<TestEntry> *currCase = warg->currCase;
        

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            TestEntry entry = currCase->at(idx);

            bool hasKey;
            std::optional<int> key;
            switch (entry.op){
            case TestOp::GET:
                key = concurrent_tree->get(entry.value);
                break;
            case TestOp::BARRIER:
                pthread_barrier_wait(&barrier);
                compare(concurrent_tree, seq_tree);
                pthread_barrier_wait(&barrier2);
                break;
            default:
                break;
            }
        }
        return nullptr;
    }

    static bool compare(T *concurrent_tree, T *seq_tree) {
        concurrent_tree->debug_checkIsValid(false);
        seq_tree->debug_checkIsValid(false);

        std::vector<K> concurrent_vec = concurrent_tree->toVec();
        std::vector<K> seq_vec = seq_tree->toVec();

        if (concurrent_vec.size() != seq_vec.size()) {
            throw std::runtime_error("concurrent vec size different from seq_vec size");
        }
        for (size_t i = 0; i < concurrent_vec.size(); i ++) {
            if (concurrent_vec[i] != seq_vec[i]) {
                concurrent_tree->print();
                seq_tree->print();
                throw std::runtime_error("concurrent_vec different from seq_vec");
            }
        }
        return true;
    }
};

template <typename T, typename K>
class lockfreeCheckEngine
{
private:
    int order;
    int numWorker;
    std::vector<std::string> paths;
    std::vector<TestEntry>   currCase;

public:
    lockfreeCheckEngine(std::vector<std::string> paths, int order, int numWorker): 
        paths(paths), order(order), numWorker(numWorker)
        {};

    void Run() {
        for (size_t j = 0; j < paths.size(); j ++) {
            const auto testCase = paths[j];
            loadTestCase(testCase);
            {
                auto tree = T(order, numWorker);
                bool pass = runTestCase(tree);
                if (pass) std::cout << "\r\033[1;32mPASS Case " << j << " " << testCase << "\033[0m" << std::endl;
                else std::cout << "\r\033[1;31mFAIL Case " << j << " " << testCase << "\033[0m" << std::endl;
                // destructor will check automatically
            }
        }
    }

private:
    void loadTestCase(const std::string &filePath) {
        currCase.clear();
        std::ifstream file(filePath);
        std::string   line;
        if (!file) {
            std::cerr << "Unable to load file at " << filePath;
            assert(false);
        }
        while (std::getline(file, line)) {
            TestEntry entry;
            entry.parse(line);
            currCase.push_back(entry);
        }
    }

    bool runTestCase(T &tree) {
        for (size_t idx = 0; idx < currCase.size(); idx ++) {
            TestEntry entry = currCase[idx];

            switch (entry.op){
            case TestOp::INSERT:
                tree.insert(entry.value);
                break;
            
            case TestOp::REMOVE:
                tree.remove(entry.value);
                break;

            case TestOp::GET:
                tree.get(entry.value);
                break;
            }
        }
        return true;
    }
};


}
