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

/**
 * @file
 * Declares ignite::Timestamp class.
 */

#ifndef _IGNITE_TIMESTAMP
#define _IGNITE_TIMESTAMP

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/date.h>

namespace ignite
{
    /**
     * %Timestamp type.
     */
    class IGNITE_IMPORT_EXPORT Timestamp
    {
    public:
        /**
         * Default constructor.
         */
        Timestamp();

        /**
         * Copy constructor.
         *
         * @param another Another instance.
         */
        Timestamp(const Timestamp& another);

        /**
         * Constructor.
         *
         * @param ms Number of milliseconds since 00:00 hours, Jan 1, 1970 UTC.
         */
        Timestamp(int64_t ms);

        /**
         * Constructor.
         *
         * @param seconds Number of seconds since 00:00 hours, Jan 1, 1970 UTC.
         * @param fractionNs Fractional second component in nanoseconds.
         *     Must be in range [0..999999999].
         */
        Timestamp(int64_t seconds, int32_t fractionNs);

        /**
         * Copy operator.
         *
         * @param another Another instance.
         * @return This.
         */
        Timestamp& operator=(const Timestamp& another);

        /**
         * Returns number of milliseconds since 00:00 hours, Jan 1, 1970 UTC.
         *
         * @return Number of milliseconds since 00:00 hours, Jan 1, 1970 UTC.
         */
        int64_t GetMilliseconds() const;

        /**
         * Returns number of seconds since 00:00 hours, Jan 1, 1970 UTC.
         *
         * @return Number of seconds since 00:00 hours, Jan 1, 1970 UTC.
         */
        int64_t GetSeconds() const;

        /**
         * Returns number of nanoseconds - fractional seconds component.
         *
         * @return Fractional second component expressed in nanoseconds.
         */
        int32_t GetSecondFraction() const;

        /**
         * Returns corresponding date.
         *
         * @return Corresponding date.
         */
        Date GetDate() const;

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator==(const Timestamp& val1, const Timestamp& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if not equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator!=(const Timestamp& val1, const Timestamp& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<(const Timestamp& val1, const Timestamp& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<=(const Timestamp& val1, const Timestamp& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>(const Timestamp& val1, const Timestamp& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>=(const Timestamp& val1, const Timestamp& val2);
    private:
        /** Number of seconds since 00:00 hours, Jan 1, 1970 UTC. */
        int64_t seconds;

        /** Fractional second component in nanoseconds. */
        int32_t fractionNs;
    };
}

#endif //_IGNITE_TIMESTAMP
