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

#ifndef _IGNITE_ODBC_DECIMAL
#define _IGNITE_ODBC_DECIMAL

#include <stdint.h>
#include <sstream>
#include <iostream>

#include <ignite/common/big_integer.h>

namespace ignite
{
    /**
     * Big decimal number implementation.
     * @todo Move to binary or common library.
     */
    class Decimal
    {
    public:
        /**
         * Default constructor.
         */
        Decimal();

        /**
         * Constructor.
         *
         * @param mag Bytes of the magnitude. Should be positive, sign is
         *     passed using separate argument.
         * @param len Magnitude length in bytes.
         * @param scale Scale.
         * @param sign Sign of the decimal. Should be -1 for negative numbers
         *     and 1 otherwise.
         * @param bigEndian If true then magnitude is in big-endian. Otherwise
         *     the byte order of the magnitude considered to be little-endian.
         */
        Decimal(const int8_t* mag, int32_t len, int32_t scale, int32_t sign, bool bigEndian = true);

        /**
         * Copy constructor.
         *
         * @param other Other instance.
         */
        Decimal(const Decimal& other);

        /**
         * Destructor.
         */
        ~Decimal();

        /**
         * Copy operator.
         *
         * @param other Other instance.
         * @return This.
         */
        Decimal& operator=(const Decimal& other);

        /**
         * Convert to double.
         */
        operator double() const;

        /**
         * Convert to int64_t.
         */
        operator int64_t() const;

        /**
         * Convert to double.
         *
         * @return Double value.
         */
        double ToDouble() const;

        /**
         * Convert to int64_t.
         *
         * @return int64_t value.
         */
        int64_t ToInt64() const;

        /**
         * Get scale.
         *
         * @return Scale.
         */
        int32_t GetScale() const;

        /**
         * Set scale.
         *
         * @param scale Scale to set.
         * @param res Result is placed here. Can be *this.
         */
        void SetScale(int32_t scale, Decimal& res) const;

        /**
         * Get precision of the Decimal.
         *
         * @return Number of the decimal digits in the decimal representation
         *     of the value.
         */
        int32_t GetPrecision() const;

        /**
         * Get unscaled value.
         *
         * @return Unscaled value.
         */
        const BigInteger& GetUnscaledValue() const;

        /**
         * Swap function for the Decimal type.
         *
         * @param other Other instance.
         */
        void Swap(Decimal& second);

        /**
         * Get length of the magnitude.
         *
         * @return Length of the magnitude.
         */
        int32_t GetMagnitudeLength() const;

        /**
         * Assign specified value to this Decimal.
         *
         * @param val Value to assign.
         */
        void Assign(int64_t val);

        /**
         * Assign specified value to this Decimal.
         *
         * @param val Value to assign.
         */
        void Assign(uint64_t val);

        /**
         * Assign specified value to this Decimal.
         *
         * @param val Value to assign.
         */
        void Assign(double val, int32_t scale);

        /**
         * Output operator.
         *
         * @param os Output stream.
         * @param val Value to output.
         * @return Reference to the first param.
         */
        friend std::ostream& operator<<(std::ostream& os, const Decimal& val)
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

                int32_t afterDot = lastNonZero - dotPos + magBegin + 1;

                if (afterDot > 0)
                {
                    os << '.';

                    os.write(&magStr[magBegin + dotPos], afterDot);
                }
            }

            return os;
        }

        /**
         * Input operator.
         *
         * @param is Input stream.
         * @param val Value to input.
         * @return Reference to the first param.
         */
        friend std::ostream& operator>>(std::ostream& is, Decimal& val)
        {
            //TODO.

            val.Assign(0ULL);

            return is;
        }

    private:
        /** Scale. */
        int32_t scale;

        /** Magnitude. */
        BigInteger magnitude;
    };
}

#endif //_IGNITE_ODBC_DECIMAL