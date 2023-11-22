#include <vector>
#include <cassert>
#include <iostream>
#include <sstream>
#include <fstream>
#include <optional>
#include "tree.h"
#include "tree.hpp"
#include "node.hpp"

enum TestOp {INSERT, REMOVE, GET};

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
};

class TestEngine {
    private:
        Tree::SeqBPlusTree<int> T;
        std::vector<std::string> testCases;
        std::vector<TestEntry> currCase;

    public:
        TestEngine(std::vector<std::string> paths): testCases(paths) {}
        void Run() {
            
            for (size_t i = 0; i < testCases.size(); i ++) {
                const auto testCase = testCases[i];
                loadTestCase(testCase);
                T = Tree::SeqBPlusTree<int>(5);
                bool pass = runTestCase();
                if (pass) std::cout << "\r\033[1;32mPASS Case " << i << " " << testCase << "\033[0m" << std::endl;
                else {
                    std::cout << "\r\033[1;31mFAIL Case " << i << " " << testCase << "\033[0m" << std::endl;
                    // T.print();
                }
            }
        }

    private:
        TestEntry parseLine(const std::string &line) {
            std::istringstream iss(line);
            std::string tok;
            TestEntry entry;

            std::getline(iss, tok, ',');
            if (tok == "I") entry.op = TestOp::INSERT;
            else if (tok == "G") entry.op = TestOp::GET;
            else if (tok == "D") entry.op = TestOp::REMOVE;

            std::getline(iss, tok, ',');
            entry.value = std::stoi(tok);

            std::getline(iss, tok, ',');
            try
            {
                entry.expect = std::stoi(tok);
            }
            catch(const std::exception& e)
            {
                entry.expect = std::nullopt;
            }
            return entry;
        }

        void loadTestCase(const std::string &filePath) {
            currCase.clear();
            std::ifstream file(filePath);
            std::string   line;
            if (!file) {
                std::cerr << "Unable to load file at " << filePath;
                assert(false);
            }
            while (std::getline(file, line)) {
                if (line.at(0) == '#') break;
                currCase.push_back(parseLine(line));
            }
        }

        bool runTestCase() {
            for (size_t idx = 0; idx < currCase.size(); idx ++) {
                TestEntry entry = currCase[idx];
                // std::cout << idx << " ";
                // entry.print();
                // T.print();
                // T.debug_assertIsValid(false);

                bool hasKey;
                std::optional<int> key;

                switch (entry.op){
                case TestOp::INSERT:
                    T.insert(entry.value);
                    break;
                
                case TestOp::REMOVE:
                    hasKey = T.remove(entry.value);
                    if (hasKey != entry.expect.has_value()) {
                        std::cout << "\n(REMOVE) FAILED AT LINE " << idx << " output: " << hasKey <<std::endl;
                        entry.print();
                        return false;
                    }
                    break;

                case TestOp::GET:
                    key = T.get(entry.value);
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


int main() {
    // std::vector<std::string> Cases = {
    //     "../test/small55.case",
    //     "../test/1.case"
    // };
    std::vector<std::string> Cases;
    for (int i = 10; i < 100; i ++) {
        std::string s = "../test/big" + std::to_string(i) + ".case";
        Cases.push_back(s);
    }

    TestEngine runner = TestEngine(Cases);
    runner.Run();
    return 0;
}
