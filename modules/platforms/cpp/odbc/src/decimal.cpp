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

    Decimal::Decimal(const int8_t* mag, int32_t len, int32_t scale, int32_t sign) :
        scale(scale & 0x7FFFFFFF),
        magnitude(mag, len, sign)
    {
        // No-op.
    }

    Decimal::Decimal(const Decimal& other) :
        scale(other.scale),
        magnitude(other.magnitude)
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
        //double res = 0;

        //for (int32_t i = 0; i < len; ++i)
        //    res = (res * 256) + static_cast<uint8_t>(magnitude[i]);

        //for (int32_t i = 0; i < GetScale(); ++i)
        //    res /= 10.0;

        //return res * GetSign();

        return 0.0;
    }

    int32_t Decimal::GetScale() const
    {
        return scale;
    }

    void Decimal::SetScale(int32_t newScale)
    {
        if (scale == newScale)
            return;

        int32_t diff = newScale - scale;

        BigInteger adjustment(10);

        if (diff > 0)
        {
            adjustment.Pow(diff);

            magnitude.Divide(adjustment, magnitude);
        }
        else
        {
            adjustment.Pow(-diff);

            magnitude.Multiply(adjustment, magnitude);
        }

        scale = newScale;
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
}

