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

#include "ignite/ignite_error.h"
#include "ignite/common/bits.h"
#include "ignite/common/big_integer.h"

using namespace ignite::common::bits;

namespace ignite
{
    typedef common::DynamicSizeArray<int32_t> MagArray;

    BigInteger::BigInteger() :
        sign(0),
        mag(0)
    {
        // No-op.
    }

    BigInteger::BigInteger(int64_t val)
    {
        if (val < 0)
        {
            val = -val;
            sign = -1;
        }
        else
            sign = 1;

        uint32_t highWord = static_cast<uint32_t>(val >> 32);

        if (highWord == 0)
            mag.Resize(1);
        else
        {
            mag.Resize(2);
            mag[1] = highWord;
        }

        mag[0] = static_cast<uint32_t>(val);
    }

    BigInteger::BigInteger(int8_t* val, int32_t len, int32_t sign) :
        sign(sign),
        mag()
    {
        assert(val != 0);
        assert(len > 0);
        assert(sign == 1 || sign == 0 || sign == -1);
        assert(val[0] > 0);

        int32_t firstNonZero = 0;
        while (firstNonZero < len && val[firstNonZero] == 0)
            ++firstNonZero;

        int32_t intLength = (len - firstNonZero + 3) / 4;

        mag.Resize(intLength);

        int32_t b = len - 1;

        for (int32_t i = 0; i < intLength - 1; ++i)
        {
            mag[i] = (val[b] & 0xFF)
                | ((val[b + 1] & 0xFF) << 8)
                | ((val[b + 2] & 0xFF) << 16)
                | ((val[b + 3] & 0xFF) << 24);

            b -= 4;
        }

        int32_t bytesRemaining = b - firstNonZero + 1;

        assert(bytesRemaining > 0 && bytesRemaining <= 4);

        switch (bytesRemaining)
        {
            case 4:
                mag[intLength - 1] |= (val[b - 3] & 0xFF) << 24;

            case 3:
                mag[intLength - 1] |= (val[b - 2] & 0xFF) << 16;

            case 2:
                mag[intLength - 1] |= (val[b - 1] & 0xFF) << 8;

            case 1:
                mag[intLength - 1] |= val[b] & 0xFF;

            default:
                break;
        }
    }
}

