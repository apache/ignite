/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    bool operator==(const Guid& val1, const Guid& val2)
    {
        return val1.least == val2.least && val1.most == val2.most;
    }
}