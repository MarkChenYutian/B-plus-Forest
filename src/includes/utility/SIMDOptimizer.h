#pragma once
#include <vector>
#include <cstdio>

#if defined(__x86_64__)
    #include <emmintrin.h>
    #include <xmmintrin.h>
#elif defined(__aarch64__)
    #include <arm_neon.h>
#endif


namespace Tree {
    template <typename T>
    class FreeNode;
    template <typename T>
    class Scheduler;
    template <typename T>
    using NodeMap = std::unordered_map<Tree::FreeNode<T> *, std::vector<uint32_t>>;

    template <typename T>
    class SIMDOptimizer {
    public:

    #if defined(__x86_64__)
        static constexpr size_t simdWidth = sizeof(__m128i) / sizeof(int);
    #elif defined(__aarch64__)
        static constexpr size_t simdWidth = 4;
    #endif
        
    public:
        static inline size_t getGtKeyIdxSpecialized(const std::vector<T> &keys, T key);
        static void processAssignments(NodeMap<T> &assign_node_to_thread, Scheduler<T>* scheduler, size_t BATCHSIZE);
    };


    /**
     * SIMD Optimized node-assignment for worker thread.
     */
    template <typename T>
    void SIMDOptimizer<T>::processAssignments(NodeMap<T> &assign_node_to_thread, Scheduler<T> *scheduler, size_t BATCHSIZE){
#ifdef NOSIMD
        size_t widx = 0;
        // Fill the slots with Request (task) queue
        for (auto &elem : assign_node_to_thread) {
            size_t ridx = 0;
            for (uint32_t &idx: elem.second) {
                scheduler->request_assign[widx][ridx] = idx;
                ridx++;
            }
            scheduler->request_assign_len[widx] = ridx;
            widx++;
        }
#elif defined(__x86_64__)
        size_t widx = 0;
        for (auto &elem : assign_node_to_thread) {
            size_t eidx = 0;
            for (; eidx + simdWidth < elem.second.size(); eidx += simdWidth) {
                __m128i idx_sse = _mm_load_si128(reinterpret_cast<const __m128i*>(&elem.second[eidx]));
                _mm_store_si128(reinterpret_cast<__m128i*>(&scheduler->request_assign[widx][eidx]), idx_sse);
            }
            for (; eidx < elem.second.size(); eidx ++) {
                scheduler->request_assign[widx][eidx] = elem.second[eidx];
            }
            scheduler->request_assign_len[widx] = eidx;
            widx++;
        }
#else
        size_t i = 0;
        for (auto it = assign_node_to_thread.begin(); it != assign_node_to_thread.end() && i < BATCHSIZE; ++it, ++i) {
            auto &elem = *it;
            size_t ridx = 0;
            size_t widx = i;

            for (uint32_t& idx : elem.second) {
                // Example of using Neon intrinsics (assuming float data type)
                uint32x4_t idx_neon = vld1q_u32(&idx); // Load Request data into Neon register
                vst1q_u32(&scheduler->request_assign[widx][ridx], idx_neon); // Store Neon register into the Scheduler data
                ridx++;
            }

            scheduler->request_assign_len[widx] = ridx;
        }
#endif
    }


    /**
     * Generic getGtKeyIdx - scan over the vector for first index
     */
    template <typename T>
    size_t SIMDOptimizer<T>::getGtKeyIdxSpecialized(const std::vector<T> &keys, T key) {
        size_t index = 0, numKey = keys.size();
        while (index < numKey && keys[index] <= key) index ++;
        return index;
    }

    /**
     * Specialized getGtKeyIdx - use SIMD to scan over the vector.
     */
    template <>
    size_t SIMDOptimizer<int>::getGtKeyIdxSpecialized(const std::vector<int> &keys, int key) {
    #ifdef __x86_64__
            size_t index = 0;
            size_t numKeys = keys.size();
            __m128i keyVector = _mm_set1_epi32(key);

            while (index + simdWidth < numKeys) {
                __m128i dataVector = _mm_load_si128(reinterpret_cast<const __m128i*>(&keys[index]));
                __m128i cmpResult = _mm_cmpgt_epi32(dataVector, keyVector);
                int mask = _mm_movemask_epi8(cmpResult);
                if (mask != 0) {
                    // Find the exact element using scalar comparison
                    for (size_t i = 0; i < simdWidth; i++) {
                        if (keys[index + i] > key) return index + i;
                    }
                }
                index += simdWidth;
            }

            // Handle any remaining elements
            while (index < numKeys && keys[index] <= key) index++;
            return index;
    #else
            size_t index = 0;
            size_t numKeys = keys.size();
            int32x4_t keyVector = vdupq_n_s32(key);
            uint32x4_t allZeros = vmovq_n_u32(0);

            while (index + simdWidth < numKeys) {
                int32x4_t dataVector = vld1q_s32(&keys[index]);
                uint32x4_t cmpResult = vcgtq_s32(dataVector, keyVector);
                if (!vceqq_u32(cmpResult, allZeros)[0] || 
                    !vceqq_u32(cmpResult, allZeros)[1] || 
                    !vceqq_u32(cmpResult, allZeros)[2] || 
                    !vceqq_u32(cmpResult, allZeros)[3]) {
                    // Find the exact element using scalar comparison
                    for (size_t i = 0; i < simdWidth; i++) {
                        if (keys[index + i] > key) return index + i;
                    }
                }
                index += simdWidth;
            }

            // Handle any remaining elements
            while (index < numKeys && keys[index] <= key) index++;
            return index;
    #endif
    }
};
