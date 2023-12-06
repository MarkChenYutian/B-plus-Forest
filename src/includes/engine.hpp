#pragma once
#include <cmath>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <barrier>
#include <optional>
#include <iostream>
#include <cassert>
#include <type_traits>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <stdio.h>
#include "tree.h"
#include "timing.h"

namespace Engine {

struct EngineConfig {
    int order;
    int numProcess;
    std::vector<std::string> paths;
};

template <template <typename> class T>
class IEngine {
    public:
        enum TestOp {INSERT, REMOVE, GET, BARRIER};

        struct TestEntry {
            int value;
            TestOp op;
            std::optional<int> expect;

            TestEntry(const std::string &line);
            bool isBarrier() {return op == TestOp::BARRIER;};
            void print();
        };

        struct WorkerArgs {
            T<int> *concurrent_tree;
            T<int> *seq_tree;
            std::vector<TestEntry> *currCase;
            int threadID;
            int threadNum;
            pthread_barrier_t *barrierA;
            pthread_barrier_t *barrierB;
        };
    
    public:
        int order;
        int numProcess;
        pthread_barrier_t barrierA;
        pthread_barrier_t barrierB;
        std::vector<std::string> paths;
        std::vector<TestEntry> currCase;
    
    public:
        void Run();
        void loadTestCase(const std::string &filePath);
};

template <template <typename> class T>
class SeqEngine : public IEngine<T> {
    public:
        SeqEngine(const EngineConfig &cfg){
            this->paths = cfg.paths;
            this->order = cfg.order;
            this->numProcess = cfg.numProcess;
        }

        void Run() {
            for (size_t j = 0; j < this->paths.size(); j ++) {
                const auto testCase = this->paths[j];
                IEngine<T>::loadTestCase(testCase);
                {
                    auto tree = T<int>(this->order);
                    bool pass = runTestCase(tree);
                    if (pass) std::cout << "\r\033[1;32mPASS Case " << j << " " << testCase << "\033[0m" << std::endl;
                    else std::cout << "\r\033[1;31mFAIL Case " << j << " " << testCase << "\033[0m" << std::endl;
                    assert(pass);
                }
            }
        }

        bool runTestCase(T<int> &tree) {
            for (size_t idx = 0; idx < this->currCase.size(); idx ++) {
                auto entry = this->currCase[idx];
                bool hasKey;
                std::optional<int> key;
                switch (entry.op){
                case IEngine<T>::TestOp::INSERT:
                    tree.insert(entry.value);
                    break;
                
                case IEngine<T>::TestOp::REMOVE:
                    hasKey = tree.remove(entry.value);
                    if (hasKey != entry.expect.has_value()) return false;
                    break;

                case IEngine<T>::TestOp::GET:
                    key = tree.get(entry.value);
                    if (key.has_value() != entry.expect.has_value()) return false;
                    if (key.has_value() && (key.value() != entry.expect.value())) return false;
                    break;
                }
            }
            return true;
        }
};

template <template <typename> class T>
class ThreadEngine : public IEngine<T> {
    public:

    ThreadEngine(const EngineConfig &cfg) {
        this->paths = cfg.paths;
        this->order = cfg.order;
        this->numProcess = cfg.numProcess;
    };

    void Run() {
        for (size_t i = 0; i < this->paths.size(); i ++) {
            auto testCase = this->paths[i];
            std::cout << "Running " << testCase << " ..." << std::flush;
            IEngine<T>::loadTestCase(testCase);
            {
                int threadNum = this->numProcess;
                auto concurrent_tree = T<int>(this->order);
                auto seq_tree = T<int>(this->order);
                typename IEngine<T>::WorkerArgs args[threadNum + 2];
                pthread_t threads[threadNum + 2];

                for (int threadId = 0; threadId < threadNum + 2; threadId ++) {
                    args[threadId].concurrent_tree = &concurrent_tree;
                    args[threadId].seq_tree  = &seq_tree;
                    args[threadId].currCase  = &this->currCase;
                    args[threadId].threadID  = threadId;
                    args[threadId].threadNum = threadNum;
                    args[threadId].barrierA  = &this->barrierA;
                    args[threadId].barrierB  = &this->barrierB;
                }

                // initialize the barrier with the number of threads
                pthread_barrier_init(&this->barrierA, NULL, threadNum + 2);
                pthread_barrier_init(&this->barrierB, NULL, threadNum + 2);
                
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
                pthread_barrier_destroy(&this->barrierA);
                pthread_barrier_destroy(&this->barrierB);

                bool pass = concurrent_tree.debug_checkIsValid(false);
                if (pass) std::cout << "\r\033[1;32mPASS Case " << i << " " << testCase << "\033[0m" << std::endl;
                else std::cout << "\r\033[1;31mFAIL Case " << i << " " << testCase << "\033[0m" << std::endl;
                assert(pass);
            }
        }
    }

    private:

    static void *runTestCase(void* arg) {
        auto warg = static_cast<typename IEngine<T>::WorkerArgs *>(arg);
        
        T<int> *tree;
        auto currCase = warg->currCase;
        int thread_id = warg->threadID;
        DBG_ASSERT(thread_id < warg->threadNum + 1);

        bool isConcurrentThread = thread_id < warg->threadNum;
        
        if (isConcurrentThread) tree = warg->concurrent_tree;
        else tree = warg->seq_tree;

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            auto entry = currCase->at(idx);

            if (isConcurrentThread && !entry.isBarrier() && 
                (entry.value % warg->threadNum != thread_id)
            ) continue;
            
            bool hasKey;
            std::optional<int> key;

            switch (entry.op){
            case IEngine<T>::TestOp::INSERT:
                key = tree->get(entry.value);
                if (key.has_value()) break;
                tree->insert(entry.value);
                break;
            
            case IEngine<T>::TestOp::REMOVE:
                hasKey = tree->remove(entry.value);
                break;

            case IEngine<T>::TestOp::GET:
                key = tree->get(entry.value);
                break;
            
            case IEngine<T>::TestOp::BARRIER:
                pthread_barrier_wait(warg->barrierA);
                pthread_barrier_wait(warg->barrierB);
            }
        }
        return nullptr;
    }

    static void *runReadCheck(void *arg) {
        auto warg = static_cast<typename IEngine<T>::WorkerArgs *>(arg);

        int thread_id = warg->threadID;
        assert(thread_id == warg->threadNum + 1);

        T<int> *concurrent_tree = warg->concurrent_tree;
        T<int> *seq_tree        = warg->seq_tree;
        auto    currCase        = warg->currCase;
        

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            auto entry = currCase->at(idx);

            bool hasKey;
            std::optional<int> key;
            switch (entry.op){
            case IEngine<T>::TestOp::GET:
                key = concurrent_tree->get(entry.value);
                break;
            case IEngine<T>::TestOp::BARRIER:
                pthread_barrier_wait(warg->barrierA);
                compare(concurrent_tree, seq_tree);
                pthread_barrier_wait(warg->barrierB);
                break;
            default:
                break;
            }
        }
        return nullptr;
    }

    static bool compare(T<int> *concurrent_tree, T<int> *seq_tree) {
        concurrent_tree->debug_checkIsValid(false);
        seq_tree->debug_checkIsValid(false);

        std::vector<int> concurrent_vec = concurrent_tree->toVec();
        std::vector<int> seq_vec = seq_tree->toVec();

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

template <template <typename> class T>
class BenchmarkEngine : public IEngine<T> {
public:
    int repeatNum{};

public:
    BenchmarkEngine(const EngineConfig &cfg) {
        this->order = cfg.order;
        this->paths = cfg.paths;
        this->numProcess = cfg.numProcess;
        this->repeatNum = 1;
    }

    void Run() {
        int threadNum = this->numProcess;
        double average_qps = 0;
        for (size_t i = 0; i < this->paths.size(); i ++) {
            auto testCase = this->paths[i];
            IEngine<T>::loadTestCase(testCase);

            double run_seconds = 0.0f, build_seconds = 0.0f;
            
            for (int repeat = 0; repeat < repeatNum; repeat ++) {
                Timer caseTimer;
                auto concurrent_tree = T<int>(this->order);
                build_seconds += caseTimer.elapsed();

                typename IEngine<T>::WorkerArgs args[threadNum];
                pthread_t threads[threadNum];

                for (int threadId = 0; threadId < threadNum; threadId ++) {
                    args[threadId].concurrent_tree = &concurrent_tree;
                    args[threadId].currCase  = &this->currCase;
                    args[threadId].threadID  = threadId;
                    args[threadId].threadNum = threadNum;
                }
                
                caseTimer.reset();
                for (int threadId = 0; threadId < threadNum; threadId ++) {
                    pthread_create(&threads[threadId], NULL, runTestCase, &args[threadId]);
                }

                for (int i = 0; i < threadNum; i++){
                    if (pthread_join(threads[i], NULL) != 0) {
                        std::cerr << "Error joining thread " << i << std::endl;
                        exit(1);
                    }
                }

                run_seconds += caseTimer.elapsed();
            }

            run_seconds = run_seconds / repeatNum;
            build_seconds = build_seconds / repeatNum;
            float estimated_qps = static_cast<float>(this->currCase.size()) / run_seconds / 1000000.0f;
            average_qps += estimated_qps;

            std::cout << "\r\033[1;32mCase " << i << "\t " << testCase << "\033[0m" << "\t";
            double run_ms = run_seconds * 1000, build_ms = build_seconds * 1000;
            std::cout << "\tBenchmark: " << 
                roundFloat(estimated_qps, 4) << "MQPS\t in " << 
                roundFloat(run_ms       , 4) << "ms, \t prep in " << 
                roundFloat(build_ms     , 4) << "ms" << std::endl;
        }
        std::cout << "Average MQPS:" << roundFloat(average_qps / this->paths.size(), 5) << std::endl;
    }

private:
    float roundFloat(float number, int decimalPlaces) {
        float scale = std::pow(10.0f, decimalPlaces);
        return std::round(number * scale) / scale;
    }

    static void *runTestCase(void* arg) {
        auto warg = static_cast<typename IEngine<T>::WorkerArgs *>(arg);
        T<int> *tree = warg->concurrent_tree;
        auto currCase = warg->currCase;
        int thread_id = warg->threadID;
        DBG_ASSERT(thread_id < warg->threadNum + 1);

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            auto entry = currCase->at(idx);
            if (entry.value % warg->threadNum != thread_id) continue;
            switch (entry.op){
            case IEngine<T>::TestOp::INSERT:
                tree->insert(entry.value);
                break;
            case IEngine<T>::TestOp::REMOVE:
                tree->remove(entry.value);
                break;
            case IEngine<T>::TestOp::GET:
                tree->get(entry.value);
                break;
            case IEngine<T>::TestOp::BARRIER:
                break;
            }
        }
        return nullptr;
    }
};

template <template <typename> class T>
IEngine<T>::TestEntry::TestEntry(const std::string &line) {
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
    if (tok == "NONE") expect = std::nullopt;
    else expect = std::stoi(tok);
}

template <template <typename> class T>
void IEngine<T>::TestEntry::print() {
    if (op == TestOp::GET) std::cout << "GET ";
    else if (op == TestOp::INSERT) std::cout << "INSERT ";
    else if (op == TestOp::REMOVE) std::cout << "DELETE ";
    else std::cout << "BARRIER ";

    if (expect.has_value()) {
        std::cout << "\t" << value << ", expect: " << expect.value() << std::endl;
    } else {
        std::cout << "\t" << value << ", expect: NONE" << std::endl;
    }
}

template <template <typename> class T>
void IEngine<T>::loadTestCase(const std::string &filePath) {
    currCase.clear();
    std::ifstream file(filePath);
    std::string   line;
    if (!file) {
        std::cerr << "Unable to load file at " << filePath;
        assert(false);
    }
    while (std::getline(file, line)) {
        currCase.emplace_back(line);
    }
}

};
