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
#include "ignite/common/decimal.h"

using ignite::common::BigInteger;

namespace ignite
{
    namespace common
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

        Decimal::Decimal(const char* val, int32_t len) :
            scale(0),
            magnitude(0)
        {
            AssignString(val, len);
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
            return LexicalCast<double>(*this);
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

        void Decimal::AssignString(const char* val, int32_t len)
        {
            std::stringstream converter;

            converter.write(val, len);

            converter >> *this;
        }

        void Decimal::AssignInt64(int64_t val)
        {
            magnitude.AssignInt64(val);

            scale = 0;
        }

        void Decimal::AssignDouble(double val)
        {
            std::stringstream converter;

            converter.precision(16);

            converter << val;
            converter >> *this;
        }

        void Decimal::AssignUint64(uint64_t val)
        {
            magnitude.AssignUint64(val);

            scale = 0;
        }

        int32_t Decimal::Compare(const Decimal& other) const
        {
            if (IsZero() && other.IsZero())
                return 0;

            if (scale == other.scale)
                return magnitude.Compare(other.magnitude);
            
            if (scale > other.scale)
            {
                Decimal scaled;

                other.SetScale(scale, scaled);

                return magnitude.Compare(scaled.magnitude);
            }
            
            Decimal scaled;

            SetScale(other.scale, scaled);

            return scaled.magnitude.Compare(other.magnitude);
        }

        bool Decimal::IsNegative() const
        {
            return magnitude.IsNegative();
        }

        bool Decimal::IsZero() const
        {
            return magnitude.IsZero();
        }

        bool Decimal::IsPositive() const
        {
            return magnitude.IsPositive();
        }

        bool operator==(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) == 0;
        }

        bool operator!=(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) != 0;
        }

        bool operator<(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) < 0;
        }

        bool operator<=(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) <= 0;
        }

        bool operator>(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) > 0;
        }

        bool operator>=(const Decimal& val1, const Decimal& val2)
        {
            return val1.Compare(val2) >= 0;
        }

        std::ostream& operator<<(std::ostream& os, const Decimal& val)
        {
            const BigInteger& unscaled = val.GetUnscaledValue();

            // Zero magnitude case. Scale does not matter.
            if (unscaled.GetMagnitude().IsEmpty())
                return os << '0';

            // Scale is zero or negative. No decimal point here.
            if (val.scale <= 0)
            {
                os << unscaled;

                // Adding zeroes if needed.
                for (int32_t i = 0; i < -val.scale; ++i)
                    os << '0';

                return os;
            }

            // Getting magnitude as a string.
            std::stringstream converter;

            converter << unscaled;

            std::string magStr = converter.str();

            int32_t magLen = static_cast<int32_t>(magStr.size());

            int32_t magBegin = 0;

            // If value is negative passing minus sign.
            if (magStr[magBegin] == '-')
            {
                os << magStr[magBegin];

                ++magBegin;
                --magLen;
            }

            // Finding last non-zero char. There is no sense in trailing zeroes
            // beyond the decimal point.
            int32_t lastNonZero = static_cast<int32_t>(magStr.size()) - 1;

            while (lastNonZero >= magBegin && magStr[lastNonZero] == '0')
                --lastNonZero;

            // This is expected as we already covered zero number case.
            assert(lastNonZero >= magBegin);

            int32_t dotPos = magLen - val.scale;

            if (dotPos <= 0)
            {
                // Means we need to add leading zeroes.
                os << '0' << '.';

                while (dotPos < 0)
                {
                    ++dotPos;

                    os << '0';
                }

                os.write(&magStr[magBegin], lastNonZero - magBegin + 1);
            }
            else
            {
                // Decimal point is in the middle of the number.
                // Just output everything before the decimal point.
                os.write(&magStr[magBegin], dotPos);

                int32_t afterDot = lastNonZero - dotPos - magBegin + 1;

                if (afterDot > 0)
                {
                    os << '.';

                    os.write(&magStr[magBegin + dotPos], afterDot);
                }
            }

            return os;
        }

        std::istream& operator>>(std::istream& is, Decimal& val)
        {
            std::istream::sentry sentry(is);

            // Return zero if input failed.
            val.AssignInt64(0);

            if (!is)
                return is;

            // Current char.
            int c = is.peek();

            // Current value parts.
            uint64_t part = 0;
            int32_t partDigits = 0;
            int32_t scale = -1;
            int32_t sign = 1;

            BigInteger& mag = val.magnitude;
            BigInteger pow;
            BigInteger bigPart;

            if (!is)
                return is;

            // Checking sign.
            if (c == '-' || c == '+')
            {
                if (c == '-')
                    sign = -1;

                is.ignore();
                c = is.peek();
            }

            // Reading number itself.
            while (is)
            {
                if (isdigit(c))
                {
                    part = part * 10 + (c - '0');
                    ++partDigits;
                }
                else if (c == '.' && scale < 0)
                {
                    // We have found decimal point. Starting counting scale.
                    scale = 0;
                }
                else
                    break;

                is.ignore();
                c = is.peek();

                if (part >= 1000000000000000000ULL)
                {
                    BigInteger::GetPowerOfTen(partDigits, pow);
                    mag.Multiply(pow, mag);

                    mag.Add(part);

                    part = 0;
                    partDigits = 0;
                }

                // Counting scale if the decimal point have been encountered.
                if (scale >= 0)
                    ++scale;
            }

            // Adding last part of the number.
            if (partDigits)
            {
                BigInteger::GetPowerOfTen(partDigits, pow);

                mag.Multiply(pow, mag);

                mag.Add(part);
            }

            // Adjusting scale.
            if (scale < 0)
                scale = 0;
            else
                --scale;

            // Reading exponent.
            if (c == 'e' || c == 'E')
            {
                is.ignore();

                int32_t exp = 0;
                is >> exp;

                scale -= exp;
            }

            val.scale = scale;

            if (sign < 0)
                mag.Negate();

            return is;
        }
    }
}