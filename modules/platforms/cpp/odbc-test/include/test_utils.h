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

#ifndef _IGNITE_ODBC_TEST_TEST_UTILS_H
#define _IGNITE_ODBC_TEST_TEST_UTILS_H

#include "ignite/date.h"
#include "ignite/timestamp.h"

namespace test_utils
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
    ignite::Date MakeDate(int year = 1900, int month = 1, int day = 1,
        int hour = 0, int min = 0, int sec = 0);

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
    ignite::Timestamp MakeTimestamp(int year = 1900, int month = 1, int day = 1,
        int hour = 0, int min = 0, int sec = 0, long ns = 0);
} // namespace test_utils

#endif // _IGNITE_ODBC_TEST_TEST_UTILS_H

