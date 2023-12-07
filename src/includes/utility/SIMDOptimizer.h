#pragma once

#include <vector>
#include <cstdio>
#include <emmintrin.h>

namespace Tree {

    template <typename T>
    class SIMDOptimizer {
    private:
        static constexpr size_t simdWidth = sizeof(__m128i) / sizeof(int);
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
};
