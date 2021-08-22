/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _IGNITE_COMMON_BITS
#define _IGNITE_COMMON_BITS

#include <stdint.h>

#include <ignite/common/common.h>
#include <ignite/common/fixed_size_array.h>

namespace ignite
{
    namespace common
    {
        namespace bits
        {
            /**
             * Maximum number of digits in uint64_t number.
             */
            const int32_t UINT64_MAX_PRECISION = 20;

            /**
             * Get number of trailing zero bits in the two's complement binary
             * representation of the specified 32-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of trailing zero bits in the two's complement
             *     binary representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfTrailingZerosI32(int32_t i);

            /**
             * Get number of leading zero bits in the two's complement binary
             * representation of the specified 32-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of leading zero bits in the two's complement
             *     binary representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfLeadingZerosI32(int32_t i);

            /**
             * Get number of leading zero bits in the two's complement binary
             * representation of the specified 32-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of leading zero bits in the two's complement
             *     binary representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfLeadingZerosU32(uint32_t i);
            
            /**
             * Get number of leading zero bits in the two's complement binary
             * representation of the specified 64-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of leading zero bits in the two's complement
             *     binary representation of the specified 64-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfLeadingZerosI64(int64_t i);
            
            /**
             * Get number of leading zero bits in the two's complement binary
             * representation of the specified 64-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of leading zero bits in the two's complement
             *     binary representation of the specified 64-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfLeadingZerosU64(uint64_t i);

            /**
             * Get the number of one-bits in the two's complement binary
             * representation of the specified 32-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of one-bits in the two's complement binary
             *     representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT  int32_t BitCountI32(int32_t i);

            /**
             * Get bit length for the specified signed 32-bit int value.
             *
             * @param i The value to get bit length for.
             * @return The number of significant bits in the two's complement binary
             *     representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t BitLengthI32(int32_t i);

            /**
             * Get bit length for the specified unsigned 32-bit int value.
             *
             * @param i The value to get bit length for.
             * @return The number of significant bits in the two's complement binary
             *     representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t BitLengthU32(uint32_t i);

            /**
             * Calcutale capasity for required size.
             * Rounds up to the nearest power of two.
             *
             * @param size Needed capasity.
             * @return Recomended capasity to allocate.
             */
            IGNITE_IMPORT_EXPORT int32_t GetCapasityForSize(int32_t size);

            /**
             * Get the number of decimal digits of the integer value.
             *
             * @param x The value.
             * @return The number of decimal digits of the integer value.
             */
            IGNITE_IMPORT_EXPORT int32_t DigitLength(uint64_t x);

            /**
             * Get n-th power of ten.
             *
             * @param n Power. Should be in range [0, UINT64_MAX_PRECISION]
             * @return 10 pow n, if n is in range [0, UINT64_MAX_PRECISION].
             *     Otherwise, behaviour is undefined.
             */
            IGNITE_IMPORT_EXPORT uint64_t TenPowerU64(int32_t n);

            /**
             * Get the signum function of the specified 64-bit integer value.
             * The return value is -1 if the specified value is negative; 0 if the
             * specified value is zero; and 1 if the specified value is positive.
             *
             * @param i the value whose signum is to be computed
             * @return The signum function of the specified value.
             */
            inline int32_t Signum64(int64_t i)
            {
                return (static_cast<uint64_t>(-i) >> 63) | (i >> 63);
            }

            /**
             * Makes single 64-bit integer number out of two 32-bit numbers.
             *
             * @param higher Higher bits part.
             * @param lower Lower bits part.
             * @return New 64-bit integer.
             */
            inline uint64_t MakeU64(uint32_t higher, uint32_t lower)
            {
                return (static_cast<uint64_t>(higher) << 32) | lower;
            }

            /**
             * Makes single 64-bit integer number out of two 32-bit numbers.
             *
             * @param higher Higher bits part.
             * @param lower Lower bits part.
             * @return New 64-bit integer.
             */
            inline int64_t MakeI64(uint32_t higher, uint32_t lower)
            {
                return static_cast<int64_t>(MakeU64(higher, lower));
            }

            /**
             * Makes single 32-bit integer number out of two 32-bit numbers,
             * shifted by specified number of bits. First number x is shifted
             * to the left.
             *     x          y
             * [........][........]
             *   ^[........]
             *   n   res
             *
             * @param x First part.
             * @param y Second part.
             * @param n Number of bits to shift.
             * @return New 32-bit integer.
             */
            inline uint32_t MakeU32(uint32_t x, uint32_t y, int32_t n)
            {
                return (x << n) | (y >> (32 - n));
            }

            /**
             * Makes single 32-bit integer number out of two 32-bit numbers,
             * shifted by specified number of bits. First number x is shifted
             * to the left.
             *     x          y
             * [........][........]
             *   ^[........]
             *   n   res
             *
             * @param x First part.
             * @param y Second part.
             * @param n Number of bits to shift.
             * @return New 32-bit integer.
             */
            inline int32_t MakeI32(uint32_t x, uint32_t y, int32_t n)
            {
                return static_cast<int32_t>(MakeU32(x, y, n));
            }
        }
    }
}

#endif //_IGNITE_COMMON_BITS
