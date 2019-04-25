/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file
 * Declares ignite::Date class.
 */

#ifndef _IGNITE_DATE
#define _IGNITE_DATE

#include <stdint.h>

#include <ignite/common/common.h>

namespace ignite
{
    /**
     * %Date type.
     */
    class IGNITE_IMPORT_EXPORT Date
    {
    public:
        /**
         * Default constructor.
         */
        Date();

        /**
         * Copy constructor.
         *
         * @param another Another instance.
         */
        Date(const Date& another);

        /**
         * Constructor.
         *
         * @param ms Number of milliseconds since 00:00 hours, Jan 1, 1970 UTC.
         */
        Date(int64_t ms);

        /**
         * Copy operator.
         *
         * @param another Another instance.
         * @return This.
         */
        Date& operator=(const Date& another);

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
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator==(const Date& val1, const Date& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if not equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator!=(const Date& val1, const Date& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<(const Date& val1, const Date& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<=(const Date& val1, const Date& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>(const Date& val1, const Date& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>=(const Date& val1, const Date& val2);
    private:
        /** Number of milliseconds since 00:00 hours, Jan 1, 1970 UTC. */
        int64_t milliseconds;  
    };
}

#endif //_IGNITE_DATE
