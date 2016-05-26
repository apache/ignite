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

    BigInteger::BigInteger(const BigInteger& other) :
        sign(other.sign),
        mag(other.mag)
    {
        // No-op.
    }

    BigInteger::BigInteger(const int8_t* val, int32_t len, int32_t sign) :
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

    BigInteger& BigInteger::operator=(const BigInteger & other)
    {
        sign = other.sign;
        mag = other.mag;

        return *this;
    }

    int8_t BigInteger::GetSign() const
    {
        return sign;
    }

    void BigInteger::Swap(BigInteger& other)
    {
        using std::swap;

        swap(sign, other.sign);
        mag.Swap(other.mag);
    }

    const BigInteger::MagArray& BigInteger::GetMagnitude() const
    {
        return mag;
    }

    uint32_t BigInteger::GetBitLength() const
    {
        if (mag.IsEmpty())
            return 0;

        int32_t res = BitLengthU32(mag[mag.GetSize() - 1]);

        if (mag.GetSize() > 1)
            res += (mag.GetSize() - 1) * 32;

        return res;
    }

    void BigInteger::MagnitudeToBytes(common::FixedSizeArray<int8_t>& buffer) const
    {
        int32_t bytesNum = static_cast<int32_t>((GetBitLength() + 7) / 8);

        buffer.Reset(bytesNum);

        int32_t i;
        for (i = 0; i < mag.GetSize() - 1; ++i)
        {
            int32_t j = bytesNum - 1 - 4 * i;

            buffer[j] = static_cast<int8_t>(mag[i]);
            buffer[j - 1] = static_cast<int8_t>(mag[i] >> 8);
            buffer[j - 2] = static_cast<int8_t>(mag[i] >> 16);
            buffer[j - 3] = static_cast<int8_t>(mag[i] >> 24);
        }

        int32_t bytesRemaining = bytesNum - 4 * i;

        assert(bytesRemaining >= 0 && bytesRemaining <= 4);

        i = 0;
        switch (bytesRemaining)
        {
            case 4:
                buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 24);

            case 3:
                buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 16);

            case 2:
                buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1] >> 8);

            case 1:
                buffer[i++] |= static_cast<int8_t>(mag[mag.GetSize() - 1]);

            default:
                break;
        }
    }
}

