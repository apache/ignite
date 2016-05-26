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
         * @param mag Bytes of the magnitude. Byte order is big-endian. Should
         *     be positive, sign is passed using separate argument.
         * @param len Magnitude length in bytes.
         * @param scale Scale.
         * @param sign Sign of the decimal. Should be -1 for negative numbers
         * and 1 otherwise.
         */
        Decimal(const int8_t* mag, int32_t len, int32_t scale, int32_t sign);

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
         * Get scale.
         *
         * @return Scale.
         */
        int32_t GetScale() const;

        /**
         * Set scale.
         *
         * @param scale Scale to set.
         */
        void SetScale(int32_t scale);

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

    private:
        /** Scale. */
        int32_t scale;

        /** Magnitude. */
        BigInteger magnitude;
    };
}

#endif //_IGNITE_ODBC_DECIMAL