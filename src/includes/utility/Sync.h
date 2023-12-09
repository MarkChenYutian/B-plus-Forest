// We are referencing Barrier from previous course member's code base
// Citation: https://github.com/runshenzhu/palmtree/blob/master/barrier.h

#pragma once
#include <atomic>

/**
 * Ticket lock, fair and efficient busy lock.
 */
class SpinLock {
public:
    SpinLock(): nextTicket(0), nowServing(0) {}
    void lock() {
        int my_ticket = nextTicket++;
        while(my_ticket != nowServing);
    }

    void unlock() {
        nowServing++;
    }

private:
    std::atomic<int> nextTicket;
    std::atomic<int> nowServing;
};

/**
 * Fast barrier based on Spinlock over threads.
 */
class Barrier {
public:
    explicit Barrier(int n): ExpectThreadNum(n) {
        generation = 0;
        remain = n;
    }

    // wait blocks until all ExpectThreadNum threads arrive the barrier and call it.
    bool wait() {
        lock.lock();
        int curr_gen = generation.load();
        if (--remain == 0) {
            generation++;
            remain = ExpectThreadNum;
            lock.unlock();
            return true;
        }
        lock.unlock();

        while (curr_gen == generation);
        return false;
    }

private:
    SpinLock lock;
    std::atomic<int> remain;
    std::atomic<unsigned long> generation;
    int ExpectThreadNum;
};
