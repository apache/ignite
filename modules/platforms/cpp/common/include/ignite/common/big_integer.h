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
        friend class Decimal;
        typedef common::DynamicSizeArray<uint32_t> MagArray;

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
         * Copy constructor.
         *
         * @param other Other value.
         */
        BigInteger(const BigInteger& other);

        /**
         * Constructs big integer from the byte array.
         *
         * @param val Bytes of the integer. Byte order is big-endian.
         * @param len Array length.
         * @param sign Signum. Can be -1 (negative) or 1 (positive or zero).
         * @param bigEndian If true then magnitude is in big-endian. Otherwise
         *     the byte order of the magnitude considered to be little-endian.
         */
        BigInteger(const int8_t* val, int32_t len, int32_t sign, bool bigEndian = true);

        /**
         * Constructs big integer with the specified magnitude.
         * @warning Magnitude is moved. This mean mag left empty after the call.
         *
         * @param mag Magnitude. Moved.
         * @param sign Sign. Can be 1 or -1.
         */
        BigInteger(MagArray& mag, int8_t sign);

        /**
         * Assigment operator.
         *
         * @param other Other value.
         * @return *this.
         */
        BigInteger& operator=(const BigInteger& other);

        /**
         * Assign specified value to this BigInteger.
         *
         * @param val Value to assign.
         */
        void Assign(const BigInteger& val);

        /**
         * Assign specified value to this BigInteger.
         *
         * @param val Value to assign.
         */
        void Assign(int64_t val);

        /**
         * Assign specified value to this BigInteger.
         *
         * @param val Value to assign.
         */
        void Assign(uint64_t val);

        /**
         * Get number sign. Returns -1 if negative and 1 otherwise.
         *
         * @return Sign of the number.
         */
        int8_t GetSign() const;

        /**
         * Swap function for the BigInteger type.
         *
         * @param other Other instance.
         */
        void Swap(BigInteger& other);

        /**
         * Get magnitude array.
         *
         * @return magnitude array.
         */
        const MagArray& GetMagnitude() const;

        /**
         * Get this number length in bits.
         *
         * @return Number length in bits.
         */
        uint32_t GetBitLength() const;

        /**
         * Get precision of the BigInteger.
         *
         * @return Number of the decimal digits in the decimal representation
         *     of the value.
         */
        int32_t GetPrecision() const;

        /**
         * Fills specified buffer with data of this BigInteger converted to
         * bytes in big-endian byte order. Sign is not considered when this
         * operation is performed.
         *
         * @param buffer Buffer to fill.
         */
        void MagnitudeToBytes(common::FixedSizeArray<int8_t>& buffer) const;

        /**
         * Mutates this BigInteger so its value becomes exp power of this.
         *
         * @param exp Exponent.
         */
        void Pow(int32_t exp);

        /**
         * Muitiply this to another big integer.
         *
         * @param other Another instance. Can be *this.
         * @param res Result placed there. Can be *this.
         */
        void Multiply(const BigInteger& other, BigInteger& res) const;

        /**
         * Divide this to another big integer.
         *
         * @param divisor Divisor. Can be *this.
         * @param res Result placed there. Can be *this.
         */
        void Divide(const BigInteger& divisor, BigInteger& res) const;

        /**
         * Compare this instance to another.
         *
         * @param other Another instance.
         * @param ignoreSign If set to true than only magnitudes are compared.
         * @return Comparasion result - 0 if equal, 1 if this is greater, -1 if
         *     this is less.
         */
        int32_t Compare(const BigInteger& other, bool ignoreSign = false) const;

        /**
         * Convert to int64_t.
         *
         * @return int64_t value.
         */
        int64_t ToInt64() const;

    private:
        /**
         * Get n-th integer of the magnitude.
         *
         * @param n Index.
         * @return Value of the n-th int of the magnitude.
         */
        int32_t GetMagInt(int32_t n) const;

        /**
         * The sign of this BigInteger: -1 for negative, 0 for zero, or
         * 1 for positive.
         */
        int8_t sign;

        /**
         * The magnitude of this BigInteger. Byte order is little-endian.
         */
        MagArray mag;
    };
}

#endif //_IGNITE_BIG_INTEGER