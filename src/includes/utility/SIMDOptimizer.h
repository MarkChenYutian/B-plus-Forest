#pragma once

#include <vector>
#include <cstdio>

#if defined(__x86_64__)
    #include <emmintrin.h>
#elif defined(__aarch64__)
    #include <arm_neon.h>
#endif


namespace Tree {

    template <typename T>
    class SIMDOptimizer {
    private:

    #if defined(__x86_64__)
        static constexpr size_t simdWidth = sizeof(__m128i) / sizeof(int);
    #elif defined(__aarch64__)
        static constexpr size_t simdWidth = 4;
    #endif
        
    public:
        static inline size_t getGtKeyIdxSpecialized(const std::vector<T> &keys, T key);
    };

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
    #ifdef __x86_64__
        template <>
        size_t SIMDOptimizer<int>::getGtKeyIdxSpecialized(const std::vector<int> &keys, int key) {
            size_t index = 0;
            size_t numKeys = keys.size();
            __m128i keyVector = _mm_set1_epi32(key);

            while (index + simdWidth < numKeys) {
                __m128i dataVector = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&keys[index]));
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
        }
    #else
        template <>
        size_t SIMDOptimizer<int>::getGtKeyIdxSpecialized(const std::vector<int> &keys, int key) {
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
        }
    #endif
};

/*
#include <arm_neon.h>


}

 */
