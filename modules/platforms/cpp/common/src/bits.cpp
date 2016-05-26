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

#include <algorithm>
#include <cassert>

#include "ignite/common/bits.h"

namespace ignite
{
    namespace common
    {
        namespace bits
        {
            int32_t NumberOfTrailingZerosI32(int32_t i)
            {
                int32_t y;

                if (i == 0) return 32;

                int32_t n = 31;

                y = i << 16;

                if (y != 0) {
                    n = n - 16;
                    i = y;
                }

                y = i << 8; 

                if (y != 0) {
                    n = n - 8;
                    i = y;
                }

                y = i << 4;

                if (y != 0) {
                    n = n - 4;
                    i = y;
                }

                y = i << 2;

                if (y != 0) {
                    n = n - 2;
                    i = y;
                }

                return n - static_cast<int32_t>(static_cast<uint32_t>(i << 1) >> 31);
            }

            int32_t NumberOfLeadingZerosI32(int32_t i)
            {
                return NumberOfLeadingZerosU32(static_cast<uint32_t>(i));
            }

            int32_t NumberOfLeadingZerosU32(uint32_t i)
            {
                if (i == 0)
                    return 32;

                int32_t n = 1;

                if (i >> 16 == 0) {
                    n += 16;
                    i <<= 16;
                }

                if (i >> 24 == 0) {
                    n += 8;
                    i <<= 8;
                }

                if (i >> 28 == 0) {
                    n += 4;
                    i <<= 4;
                }

                if (i >> 30 == 0) {
                    n += 2;
                    i <<= 2;
                }

                return n - static_cast<int32_t>(i >> 31);
            }

            int32_t NumberOfLeadingZerosI64(int64_t i)
            {
                if (i == 0)
                    return 64;

                int32_t n = 1;

                uint32_t x = static_cast<uint32_t>(static_cast<uint64_t>(i) >> 32);

                if (x == 0) {
                    n += 32;
                    x = static_cast<uint32_t>(i);
                }

                if (x >> 16 == 0) {
                    n += 16;
                    x <<= 16;
                }

                if (x >> 24 == 0) {
                    n += 8;
                    x <<= 8;
                }

                if (x >> 28 == 0) {
                    n += 4;
                    x <<= 4;
                }

                if (x >> 30 == 0) {
                    n += 2;
                    x <<= 2;
                }

                n -= x >> 31;

                return n;
            }

            int32_t BitCountI32(int32_t i)
            {
                uint32_t ui = static_cast<uint32_t>(i);

                ui -= (ui >> 1) & 0x55555555;
                ui = (ui & 0x33333333) + ((ui >> 2) & 0x33333333);
                ui = (ui + (ui >> 4)) & 0x0f0f0f0f;
                ui += ui >> 8;
                ui += ui >> 16;

                return static_cast<int32_t>(ui & 0x3f);
            }

            int32_t BitLengthI32(int32_t i)
            {
                return 32 - NumberOfLeadingZerosI32(i);
            }

            int32_t BitLengthU32(uint32_t i)
            {
                return 32 - NumberOfLeadingZerosU32(i);
            }

            int32_t GetCapasityForSize(int32_t size)
            {
                assert(size > 0);

                if (size <= 8)
                    return 8;

                int32_t bl = BitLengthI32(size);

                if (bl > 30)
                    return INT32_MAX;

                int32_t res = 1 << bl;

                return size > res ? res << 1 : res;
            }
        }
    }
}
