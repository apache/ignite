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

#include "ignite/date.h"

namespace ignite
{
    Date::Date() : milliseconds(0)
    {
        // No-op.
    }

    Date::Date(const Date& another) : milliseconds(another.milliseconds)
    {
        // No-op.
    }

    Date::Date(int64_t ms) : milliseconds(ms)
    {
        // No-op.
    }

    Date& Date::operator=(const Date& another)
    {
        milliseconds = another.milliseconds;

        return *this;
    }

    int64_t Date::GetMilliseconds() const
    {
        return milliseconds;
    }

    int64_t Date::GetSeconds() const
    {
        return milliseconds / 1000;
    }

    bool operator==(const Date& val1, const Date& val2)
    {
        return val1.milliseconds == val2.milliseconds;
    }

    bool operator!=(const Date& val1, const Date& val2)
    {
        return val1.milliseconds != val2.milliseconds;
    }

    bool operator<(const Date& val1, const Date& val2)
    {
        return val1.milliseconds < val2.milliseconds;
    }

    bool operator<=(const Date& val1, const Date& val2)
    {
        return val1.milliseconds <= val2.milliseconds;
    }

    bool operator>(const Date& val1, const Date& val2)
    {
        return val1.milliseconds > val2.milliseconds;
    }

    bool operator>=(const Date& val1, const Date& val2)
    {
        return val1.milliseconds >= val2.milliseconds;
    }
}
