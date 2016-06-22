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

namespace ignite
{
    
    /**
     * Big decimal number implementation.
     * @todo Move to binary or common library.
     */
    class Decimal
    {
        friend void swap(Decimal& first, Decimal& second);
    public:
        /**
         * Default constructor.
         */
        Decimal();

        /**
         * Constructor.
         *
         * @param scale Scale.
         * @param mag Magnitude. Value is copied.
         * @param len Magnitude length in bytes.
         */
        Decimal(int32_t scale, const int8_t* mag, int32_t len);

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
         * Get sign.
         *
         * @return Sign: -1 if negative and 1 if positive.
         */
        int32_t GetSign() const;

        /**
         * Check if the value is negative.
         * 
         * @return True if negative and false otherwise.
         */
        bool IsNegative() const;

        /**
         * Get magnitude length in bytes.
         *
         * @return Magnitude length in bytes.
         */
        int32_t GetLength() const;

        /**
         * Get number of significant bits of the magnitude.
         *
         * @return Number of significant bits of the magnitude.
         */
        int32_t BitLength() const;

        /**
         * Get magnitude pointer.
         *
         * @return Magnitude pointer.
         */
        const int8_t* GetMagnitude() const;

    private:
        /** Scale. */
        int32_t scale;

        /** Magnitude lenght. */
        int32_t len;

        /** Magnitude. */
        int8_t* magnitude;
    };

    /**
     * Swap function for the Decimal type.
     *
     * @param first First instance.
     * @param second Second instance.
     */
    void swap(Decimal& first, Decimal& second);
}



#endif //_IGNITE_ODBC_DECIMAL