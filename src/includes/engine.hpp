#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <pthread.h>
#include <sys/wait.h>
#include <type_traits>
#include "node.hpp"

enum TestOp {INSERT, REMOVE, GET};

namespace Engine {

struct TestEntry {
    int value;
    TestOp op;
    std::optional<int> expect;

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

    int parse(const std::string &line) {
        std::istringstream iss(line);
        std::string tok;

        std::getline(iss, tok, ',');
        if (tok == "I") op = TestOp::INSERT;
        else if (tok == "G") op = TestOp::GET;
        else if (tok == "D") op = TestOp::REMOVE;

        std::getline(iss, tok, ',');
        value = std::stoi(tok);

        std::getline(iss, tok, ',');
        try
        {
            expect = std::stoi(tok);
        }
        catch(const std::exception& e)
        {
            expect = std::nullopt;
        }

        std::getline(iss, tok, ',');
        int thread = std::stoi(tok);
        return thread;
    }
};

template <typename T>
struct WorkerArgs {
    T *tree;
    std::vector<std::vector<TestEntry>> *currCase;
    int threadID;
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

    bool runTestCase(T tree) {
        for (size_t idx = 0; idx < currCase.size(); idx ++) {
            TestEntry entry = currCase[idx];

            if (idx % 1000 == 0 && !tree.debug_checkIsValid(false)) return false;

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
                    return false;
                }
                break;

            case TestOp::GET:
                key = tree.get(entry.value);
                if (key.has_value() != entry.expect.has_value()) {
                    std::cout << "\n(GET, CASE1) FAILED AT LINE " << idx << std::endl;
                    entry.print();
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
    std::vector<std::vector<TestEntry>> currCase;

public:
    threadingEngine(std::vector<std::string> paths, int order, int threadNum): 
        paths(paths), order(order), threadNum(threadNum)
        {
            currCase = std::vector<std::vector<TestEntry>>(threadNum);
        };

    void Run() {
        for (size_t i = 0; i < paths.size(); i ++) {
            const auto testCase = paths[i];
            std::cout << "Running " << testCase << " ...";
            loadTestCase(testCase);
            {
                auto tree = T(order);
                WorkerArgs<T> args[threadNum];
                pthread_t threads[threadNum];

                for (int threadId = 0; threadId < threadNum; threadId ++) {
                    args[threadId].tree = &tree;
                    args[threadId].currCase = &currCase;
                    args[threadId].threadID = threadId;
                }

                for (int threadId = 0; threadId < threadNum; threadId ++) {
                    pthread_create(&threads[threadId], NULL, runTestCase, &args[threadId]);
                }

                for (int i = 0; i < threadNum; i++){
                    if (pthread_join(threads[i], NULL) != 0) {
                        std::cerr << "Error joining thread " << i << std::endl;
                        exit(1);
                    }
                }

                bool pass = tree.debug_checkIsValid(false);
                if (pass) std::cout << "\r\033[1;32mPASS Case " << i << " " << testCase << "\033[0m" << std::endl;
                else std::cout << "\r\033[1;31mFAIL Case " << i << " " << testCase << "\033[0m" << std::endl;
            }
        }
    }

private:
    void loadTestCase(const std::string &filePath) {
        for (int i = 0; i < threadNum; i ++) {
            currCase[i].clear();
        }

        std::ifstream file(filePath);
        std::string   line;
        if (!file) {
            std::cerr << "Unable to load file at " << filePath;
            assert(false);
        }

        int counter = 0;

        while (std::getline(file, line)) {
            TestEntry entry;
            int thread = entry.parse(line);
            currCase[thread].emplace_back(entry);
        }
    }

    static void *runTestCase(void* arg) {
        WorkerArgs<T> *warg = static_cast<WorkerArgs<T>*>(arg);
        T *tree = warg->tree;
        std::vector<std::vector<TestEntry>> *currCase = warg->currCase;
        int thread_id = warg->threadID;


        for (size_t idx = 0; idx < currCase->at(thread_id).size(); idx ++) {
            TestEntry entry = currCase->at(thread_id)[idx];

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
            }
        }
        return nullptr;
    }
};

}
