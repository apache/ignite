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

#ifndef _IGNITE_COMMON_DECIMAL
#define _IGNITE_COMMON_DECIMAL

#include <stdint.h>
#include <cctype>

#include <sstream>
#include <iostream>

#include <ignite/common/big_integer.h>

namespace ignite
{
    namespace common
    {
        /**
         * Big decimal number implementation.
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
             * Integer constructor.
             *
             * @param val Integer value.
             */
            explicit Decimal(int64_t val);

            /**
             * Integer constructor with scale.
             *
             * @param val Integer value.
             * @param scale Scale.
             */
            Decimal(int64_t val, int32_t scale);

            /**
             * BigInteger constructor with scale.
             *
             * @param val BigInteger value.
             * @param scale Scale.
             */
            Decimal(const common::BigInteger& val, int32_t scale);

            /**
             * String constructor.
             *
             * @param val String to assign.
             * @param len String length.
             */
            Decimal(const char* val, int32_t len);

            /**
             * String constructor.
             *
             * @param val String to assign.
             */
            explicit Decimal(const std::string& val) :
                scale(0),
                magnitude(0)
            {
                AssignString(val);
            }

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
            const common::BigInteger& GetUnscaledValue() const;

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
             * @param val String to assign.
             */
            void AssignString(const std::string& val)
            {
                AssignString(val.data(), static_cast<int32_t>(val.size()));
            }

            /**
             * Assign specified value to this Decimal.
             *
             * @param val String to assign.
             * @param len String length.
             */
            void AssignString(const char* val, int32_t len);

            /**
             * Assign specified value to this Decimal.
             *
             * @param val Value to assign.
             */
            void AssignInt64(int64_t val);

            /**
             * Assign specified value to this Decimal.
             *
             * @param val Value to assign.
             */
            void AssignDouble(double val);

            /**
             * Assign specified value to this Decimal.
             *
             * @param val Value to assign.
             */
            void AssignUint64(uint64_t val);

            /**
             * Compare this instance to another.
             *
             * @param other Another instance.
             * @return Comparasion result - 0 if equal, 1 if this is greater, -1 if
             *     this is less.
             */
            int32_t Compare(const Decimal& other) const;

            /**
             * Check whether this value is negative.
             *
             * @return True if this value is negative and false otherwise.
             */
            bool IsNegative() const;

            /**
             * Check whether this value is zero.
             *
             * @return True if this value is negative and false otherwise.
             */
            bool IsZero() const;

            /**
             * Check whether this value is positive.
             *
             * @return True if this value is positive and false otherwise.
             */
            bool IsPositive() const;

            /**
             * Output operator.
             *
             * @param os Output stream.
             * @param val Value to output.
             * @return Reference to the first param.
             */
            friend std::ostream& operator<<(std::ostream& os, const Decimal& val)
            {
                const common::BigInteger& unscaled = val.GetUnscaledValue();

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

            /**
             * Input operator.
             *
             * @param is Input stream.
             * @param val Value to input.
             * @return Reference to the first param.
             */
            friend std::istream& operator>>(std::istream& is, Decimal& val)
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

                common::BigInteger& mag = val.magnitude;
                common::BigInteger pow;
                common::BigInteger bigPart;

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
                        common::BigInteger::GetPowerOfTen(partDigits, pow);
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
                    common::BigInteger::GetPowerOfTen(partDigits, pow);

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

        private:
            /** Scale. */
            int32_t scale;

            /** Magnitude. */
            common::BigInteger magnitude;
        };

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        bool IGNITE_IMPORT_EXPORT operator==(const Decimal& val1, const Decimal& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if not equal.
         */
        bool IGNITE_IMPORT_EXPORT operator!=(const Decimal& val1, const Decimal& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less.
         */
        bool IGNITE_IMPORT_EXPORT operator<(const Decimal& val1, const Decimal& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less or equal.
         */
        bool IGNITE_IMPORT_EXPORT operator<=(const Decimal& val1, const Decimal& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter.
         */
        bool IGNITE_IMPORT_EXPORT operator>(const Decimal& val1, const Decimal& val2);

        /**
         * Comparison operator.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter or equal.
         */
        bool IGNITE_IMPORT_EXPORT operator>=(const Decimal& val1, const Decimal& val2);
    }
}

#endif //_IGNITE_COMMON_DECIMAL