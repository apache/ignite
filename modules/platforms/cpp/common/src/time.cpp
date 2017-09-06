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

#include "ignite/time.h"

namespace ignite
{
    Time::Time() : milliseconds(0)
    {
        // No-op.
    }

    Time::Time(const Time& another) : milliseconds(another.milliseconds)
    {
        // No-op.
    }

    Time::Time(int64_t ms) : milliseconds(ms)
    {
        // No-op.
    }

    Time& Time::operator=(const Time& another)
    {
        milliseconds = another.milliseconds;

        return *this;
    }

    int64_t Time::GetMilliseconds() const
    {
        return milliseconds;
    }

    int64_t Time::GetSeconds() const
    {
        return milliseconds / 1000;
    }

    bool operator==(const Time& val1, const Time& val2)
    {
        return val1.milliseconds == val2.milliseconds;
    }

    bool operator!=(const Time& val1, const Time& val2)
    {
        return val1.milliseconds != val2.milliseconds;
    }

    bool operator<(const Time& val1, const Time& val2)
    {
        return val1.milliseconds < val2.milliseconds;
    }

    bool operator<=(const Time& val1, const Time& val2)
    {
        return val1.milliseconds <= val2.milliseconds;
    }

    bool operator>(const Time& val1, const Time& val2)
    {
        return val1.milliseconds > val2.milliseconds;
    }

    bool operator>=(const Time& val1, const Time& val2)
    {
        return val1.milliseconds >= val2.milliseconds;
    }
}
