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

    private:
        /** Scale. */
        int32_t scale;

        /** Magnitude. */
        BigInteger magnitude;
    };

    /**
     * Output operator.
     *
     * @param os Output stream.
     * @param val Value to output.
     * @return Reference to the first param.
     */
    template<typename C>
    ::std::basic_ostream<C>& operator<<(std::basic_ostream<C>& os, const Decimal& val)
    {
        //TODO.
        os << 0;

        return os;
    }

    /**
     * Input operator.
     *
     * @param is Input stream.
     * @param val Value to input.
     * @return Reference to the first param.
     */
    template<typename C>
    ::std::basic_istream<C>& operator>>(std::basic_istream<C>& is, Decimal& val)
    {
        //TODO.

        return is;
    }
}

#endif //_IGNITE_ODBC_DECIMAL