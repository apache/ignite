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

#include <cstring>
#include <utility>

#include "ignite/common/utils.h"

#include "ignite/odbc/decimal.h"

namespace ignite
{
    Decimal::Decimal() : 
        scale(0), len(0), magnitude(0)
    {
        // No-op.
    }

    Decimal::Decimal(int32_t scale, const int8_t* mag, int32_t len) :
        scale(scale), len(len), magnitude(0)
    {
        magnitude = new int8_t[len];

        memcpy(magnitude, mag, len);
    }

    Decimal::Decimal(const Decimal& other) :
        scale(other.scale), len(other.len), magnitude(0)
    {
        magnitude = new int8_t[len];

        memcpy(magnitude, other.magnitude, len);
    }

    Decimal::~Decimal()
    {
        if (magnitude)
            delete[] magnitude;
    }

    Decimal& Decimal::operator=(const Decimal& other)
    {
        Decimal tmp(other);

        swap(tmp, *this);

        return *this;
    }

    Decimal::operator double() const
    {
        double res = 0;

        for (int32_t i = 0; i < len; ++i)
            res = (res * 256) + static_cast<uint8_t>(magnitude[i]);

        for (int32_t i = 0; i < GetScale(); ++i)
            res /= 10.0;

        return res * GetSign();
    }

    int32_t Decimal::GetScale() const
    {
        return scale & 0x7FFFFFFF;
    }

    int32_t Decimal::GetSign() const
    {
        return IsNegative() ? -1 : 1;
    }

    bool Decimal::IsNegative() const
    {
        return (scale & 0x80000000) != 0;
    }

    int32_t Decimal::GetLength() const
    {
        return len;
    }

    int32_t Decimal::BitLength() const
    {
        using namespace common;

        if (len == 0)
            return 0;

        int32_t bitsLen = (len - 1) * 8 +
            BitLengthForOctet(magnitude[len - 1]);

        if (IsNegative()) {

            // Check if magnitude is a power of two
            bool pow2 = PowerOfTwo(magnitude[len - 1]);
            for (int i = 0; i < len - 1 && pow2; ++i)
                pow2 = (magnitude[i] == 0);

            if (pow2)
                --bitsLen;
        }

        return bitsLen;
    }

    const int8_t* Decimal::GetMagnitude() const
    {
        return magnitude;
    }

    void swap(Decimal& first, Decimal& second)
    {
        using std::swap;

        std::swap(first.scale, second.scale);
        std::swap(first.len, second.len);
        std::swap(first.magnitude, second.magnitude);
    }
}

