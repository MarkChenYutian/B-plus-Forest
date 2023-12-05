#pragma once
#include <cmath>
#include "engine.hpp"
#include "timing.h"

namespace Engine {

template <typename T, typename K>
class benchmarkEngine {

private:
    int order;
    int threadNum;
    int repeatNum;
    std::vector<std::string> paths;
    std::vector<TestEntry>   currCase;

public:
    benchmarkEngine(std::vector<std::string> paths, int order, int threadNum, int repeatNum): 
        paths(paths), order(order), threadNum(threadNum), repeatNum(repeatNum)
        {
            currCase = std::vector<TestEntry>();
        };
    
    void Run() {
        double average_qps = 0;
        for (size_t i = 0; i < paths.size(); i ++) {
            const auto testCase = paths[i];
            loadTestCase(testCase);

            double run_seconds = 0.0f, build_seconds = 0.0f;
            
            for (int repeat = 0; repeat < repeatNum; repeat ++) {
                Timer caseTimer;
                auto concurrent_tree = T(order);
                build_seconds += caseTimer.elapsed();

                WorkerArgs<T, K> args[threadNum];
                pthread_t threads[threadNum];

                for (int threadId = 0; threadId < threadNum; threadId ++) {
                    args[threadId].concurrent_tree = &concurrent_tree;
                    args[threadId].currCase  = &currCase;
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
            float estimated_qps = static_cast<float>(currCase.size()) / run_seconds / 1000000.0f;
            average_qps += estimated_qps;

            std::cout << "\r\033[1;32mCase " << i << "\t " << testCase << "\033[0m" << "\t";
            double run_ms = run_seconds * 1000, build_ms = build_seconds * 1000;
            std::cout << "\tBenchmark: " << 
                roundFloat(estimated_qps, 4) << "MQPS\t in " << 
                roundFloat(run_ms       , 4) << "ms, \t prep in " << 
                roundFloat(build_ms     , 4) << "ms" << std::endl;
        }
        std::cout << "Average MQPS:" << roundFloat(average_qps / paths.size(), 5) << std::endl;
    }

private:
    float roundFloat(float number, int decimalPlaces) {
        float scale = std::pow(10.0f, decimalPlaces);
        return std::round(number * scale) / scale;
    }

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
        tree = warg->concurrent_tree;

        for (size_t idx = 0; idx < currCase->size(); idx ++) {
            TestEntry entry = currCase->at(idx);

            if (entry.value % warg->threadNum != thread_id) continue;

            switch (entry.op){
            case TestOp::INSERT:
                tree->insert(entry.value);
                break;
            
            case TestOp::REMOVE:
                tree->remove(entry.value);
                break;

            case TestOp::GET:
                tree->get(entry.value);
                break;
            
            case TestOp::BARRIER:
                break;
            }
        }
        return nullptr;
    }
};
};
