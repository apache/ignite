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
        scale(0),
        magnitude(0)
    {
        // No-op.
    }

    Decimal::Decimal(const int8_t* mag, int32_t len, int32_t scale, int32_t sign, bool bigEndian) :
        scale(scale & 0x7FFFFFFF),
        magnitude(mag, len, sign, bigEndian)
    {
        // No-op.
    }

    Decimal::Decimal(const Decimal& other) :
        scale(other.scale),
        magnitude(other.magnitude)
    {
        // No-op.
    }

    Decimal::Decimal(int64_t val) :
        scale(0),
        magnitude(val)
    {
        // No-op.
    }

    Decimal::Decimal(int64_t val, int32_t scale) :
        scale(scale),
        magnitude(val)
    {
        // No-op.
    }

    Decimal::Decimal(const BigInteger& val, int32_t scale) :
        scale(scale),
        magnitude(val)
    {
        // No-op.
    }

    Decimal::~Decimal()
    {
        // No-op.
    }

    Decimal& Decimal::operator=(const Decimal& other)
    {
        scale = other.scale;
        magnitude = other.magnitude;

        return *this;
    }

    Decimal::operator double() const
    {
        return ToDouble();
    }

    Decimal::operator int64_t() const
    {
        return ToInt64();
    }

    double Decimal::ToDouble() const
    {
        return common::LexicalCast<double>(*this);
    }

    int64_t Decimal::ToInt64() const
    {
        if (scale == 0)
            return magnitude.ToInt64();

        Decimal zeroScaled;

        SetScale(0, zeroScaled);

        return zeroScaled.magnitude.ToInt64();
    }

    int32_t Decimal::GetScale() const
    {
        return scale;
    }

    void Decimal::SetScale(int32_t newScale, Decimal& res) const
    {
        if (scale == newScale)
            return;

        int32_t diff = scale - newScale;

        BigInteger adjustment;

        if (diff > 0)
        {
            BigInteger::GetPowerOfTen(diff, adjustment);

            magnitude.Divide(adjustment, res.magnitude);
        }
        else
        {
            BigInteger::GetPowerOfTen(-diff, adjustment);

            magnitude.Multiply(adjustment, res.magnitude);
        }

        res.scale = newScale;
    }

    int32_t Decimal::GetPrecision() const
    {
        return magnitude.GetPrecision();
    }

    const BigInteger& Decimal::GetUnscaledValue() const
    {
        return magnitude;
    }

    void Decimal::Swap(Decimal& second)
    {
        using std::swap;

        swap(scale, second.scale);
        magnitude.Swap(second.magnitude);
    }

    int32_t Decimal::GetMagnitudeLength() const
    {
        return magnitude.mag.GetSize();
    }

    void Decimal::Assign(int64_t val)
    {
        magnitude.Assign(val);

        scale = 0;
    }

    void Decimal::Assign(uint64_t val)
    {
        magnitude.Assign(val);

        scale = 0;
    }

    void Decimal::Assign(double val)
    {
        common::LexicalCast(val, *this);
    }
}

