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

#include "test_utils.h"

namespace test
{

    /**
     * Make Date in human understandable way.
     *
     * @param year Year.
     * @param month Month.
     * @param day Day.
     * @param hour Hour.
     * @param min Min.
     * @param sec Sec.
     * @return Date.
     */
    Date MakeDate(int year = 1900, int month = 1, int day = 1, int hour = 0, int min = 0, int sec = 0)
    {
        tm date;

        date.tm_year = year - 1900;
        date.tm_mon = month - 1;
        date.tm_mday = day;
        date.tm_hour = hour;
        date.tm_min = min;
        date.tm_sec = sec;

        time_t ct = mktime(&date);

        return Date(ct * 1000);
    }

    /**
     * Make Date in human understandable way.
     *
     * @param year Year.
     * @param month Month.
     * @param day Day.
     * @param hour Hour.
     * @param min Minute.
     * @param sec Second.
     * @param ns Nanosecond.
     * @return Timestamp.
     */
    Timestamp MakeTimestamp(int year = 1900, int month = 1, int day = 1, int hour = 0, int min = 0, int sec = 0, long ns = 0)
    {
        tm date;

        date.tm_year = year - 1900;
        date.tm_mon = month - 1;
        date.tm_mday = day;
        date.tm_hour = hour;
        date.tm_min = min;
        date.tm_sec = sec;

        time_t ct = mktime(&date);

        return Timestamp(ct, ns);
    }

} // namespace test
