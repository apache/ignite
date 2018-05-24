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
 * Declares ignite::Time class.
 */

#ifndef _IGNITE_TIME
#define _IGNITE_TIME

#include <stdint.h>

#include <ignite/common/common.h>

namespace ignite
{
    /**
     * %Time type.
     */
    class IGNITE_IMPORT_EXPORT Time
    {
    public:
        /**
         * Default constructor.
         */
        Time();

        /**
         * Copy constructor.
         *
         * @param another Another instance.
         */
        Time(const Time& another);

        /**
         * Constructor.
         *
         * @param ms Time in milliseconds since 00:00:00 UTC.
         */
        Time(int64_t ms);

        /**
         * Copy operator.
         *
         * @param another Another instance.
         * @return This.
         */
        Time& operator=(const Time& another);

        /**
         * Returns number of milliseconds since 00:00:00 UTC.
         *
         * @return Number of milliseconds since 00:00:00 UTC.
         */
        int64_t GetMilliseconds() const;

        /**
         * Returns number of seconds since 00:00:00 UTC.
         *
         * @return Number of seconds since 00:00:00 UTC.
         */
        int64_t GetSeconds() const;

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator==(const Time& val1, const Time& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if not equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator!=(const Time& val1, const Time& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<(const Time& val1, const Time& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if less or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator<=(const Time& val1, const Time& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>(const Time& val1, const Time& val2);

        /**
         * Comparison operator override.
         *
         * @param val1 First value.
         * @param val2 Second value.
         * @return True if gretter or equal.
         */
        friend bool IGNITE_IMPORT_EXPORT operator>=(const Time& val1, const Time& val2);
    private:
        /** Time in milliseconds since 00:00:00 UTC. */
        int64_t milliseconds;
    };
}

#endif //_IGNITE_TIME
