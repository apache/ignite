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

#ifndef _IGNITE_BIG_INTEGER
#define _IGNITE_BIG_INTEGER

#include <stdint.h>

#include <ignite/common/dynamic_size_array.h>

namespace ignite
{
    /**
     * Big integer number implementation.
     */
    class BigInteger
    {
        friend class BigDecimal;
        friend class MutableBigInteger;

    public:
        /**
         * Default constructor. Constructs zero-value big integer.
         */
        BigInteger();

        /**
         * Constructs big integer with the specified integer value.
         *
         * @param val Value.
         */
        BigInteger(int64_t val);

        /**
         * Constructs big integer from the byte array.
         *
         * @param val Bytes of the integer. Byte order is big-endian.
         * @param len Array length.
         * @param sign Signum. Can be -1 (negative), 1 (positive) or 0 (zero).
         * @throw IgniteError if the value is too big.
         */
        BigInteger(int8_t* val, int32_t len, int32_t sign);

    private:
        /**
         * The sign of this BigInteger: -1 for negative, 0 for zero, or
         * 1 for positive.
         */
        int8_t sign;

        /**
         * The magnitude of this BigInteger. Byte order is little-endian.
         */
        common::DynamicSizeArray<uint32_t> mag;
    };
}

#endif //_IGNITE_BIG_INTEGER