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

#include "ignite/timestamp.h"

namespace ignite
{
    Timestamp::Timestamp() :
        seconds(0),
        fractionNs(0)
    {
        // No-op.
    }

    Timestamp::Timestamp(const Timestamp& another) : 
        seconds(another.seconds),
        fractionNs(another.fractionNs)
    {
        // No-op.
    }

    Timestamp::Timestamp(int64_t ms) :
        seconds(ms / 1000),
        fractionNs((ms % 1000) * 1000000)
    {
        // No-op.
    }

    Timestamp::Timestamp(int64_t seconds, int32_t fractionNs) :
        seconds(seconds),
        fractionNs(fractionNs)
    {
        // No-op.
    }

    Timestamp& Timestamp::operator=(const Timestamp& another)
    {
        seconds = another.seconds;
        fractionNs = another.fractionNs;

        return *this;
    }

    int64_t Timestamp::GetMilliseconds() const
    {
        return seconds * 1000 + fractionNs / 1000000;
    }

    int64_t Timestamp::GetSeconds() const
    {
        return seconds;
    }

    int32_t Timestamp::GetSecondFraction() const
    {
        return fractionNs;
    }

    Date Timestamp::GetDate() const
    {
        return Date(GetMilliseconds());
    }

    bool operator==(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds == val2.seconds &&
            val1.fractionNs == val2.fractionNs;
    }

    bool operator!=(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds != val2.seconds ||
            val1.fractionNs != val2.fractionNs;
    }

    bool operator<(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds < val2.seconds ||
            (val1.seconds == val2.seconds &&
                val1.fractionNs < val2.fractionNs);
    }

    bool operator<=(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds < val2.seconds ||
            (val1.seconds == val2.seconds &&
                val1.fractionNs <= val2.fractionNs);
    }

    bool operator>(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds > val2.seconds ||
            (val1.seconds == val2.seconds &&
                val1.fractionNs > val2.fractionNs);
    }

    bool operator>=(const Timestamp& val1, const Timestamp& val2)
    {
        return val1.seconds > val2.seconds ||
            (val1.seconds == val2.seconds &&
                val1.fractionNs >= val2.fractionNs);
    }
}
