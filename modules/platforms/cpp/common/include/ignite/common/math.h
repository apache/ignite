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
#ifndef _IGNITE_COMMON_MATH
#define _IGNITE_COMMON_MATH

#include <stdint.h>

#include <ignite/common/common.h>
#include <ignite/common/fixed_size_array.h>

namespace ignite
{
    namespace common
    {
        namespace math
        {
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
             * representation of the specified 64-bit int value.
             *
             * @param i The value whose bits are to be counted.
             * @return The number of leading zero bits in the two's complement
             *     binary representation of the specified 64-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t NumberOfLeadingZerosI64(int64_t i);

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
             * Get bit length for the specified 32-bit int value.
             *
             * @param i The value to get bit length for.
             * @return The number of significant bits in the two's complement binary
             *     representation of the specified 32-bit int value.
             */
            IGNITE_IMPORT_EXPORT int32_t BitLengthI32(int32_t i);

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
             * shifted by specified number of bits.
             *     x          y
             * [........][........]
             *    [........]^
             *       res    n
             *
             * @param x First part.
             * @param y Second part.
             * @param n Number of bits to shift from the begginning of the y.
             * @return New 32-bit integer.
             */
            inline int32_t MakeI32(uint32_t x, uint32_t y, int32_t n)
            {
                return static_cast<int32_t>((x << (32 - n)) | (y >> n));
            }
        }
    }
}

#endif //_IGNITE_COMMON_MATH