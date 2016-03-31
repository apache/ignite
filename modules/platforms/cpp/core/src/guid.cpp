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

#include "ignite/guid.h"

namespace ignite
{
    Guid::Guid() : most(0), least(0)
    {
        // No-op.
    }

    Guid::Guid(int64_t most, int64_t least) : most(most), least(least)
    {
        // No-op.
    }

    int64_t Guid::GetMostSignificantBits() const
    {
        return most;
    }

    int64_t Guid::GetLeastSignificantBits() const
    {
        return least;
    }

    int32_t Guid::GetVersion() const
    {
        return static_cast<int32_t>((most >> 12) & 0x0f);
    }

    int32_t Guid::GetVariant() const
    {
        uint64_t least0 = static_cast<uint64_t>(least);

        return static_cast<int32_t>((least0 >> (64 - (least0 >> 62))) & (least >> 63));
    }

    int32_t Guid::GetHashCode() const
    {
        int64_t hilo = most ^ least;

        return static_cast<int32_t>(hilo >> 32) ^ static_cast<int32_t>(hilo);
    }

    bool operator==(Guid& val1, Guid& val2)
    {
        return val1.least == val2.least && val1.most == val2.most;
    }
}