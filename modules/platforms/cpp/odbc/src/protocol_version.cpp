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

#include "ignite/odbc/protocol_version.h"
#include <ignite/common/concurrent.h>
#include <ignite/common/utils.h>
#include <ignite/ignite_error.h>

namespace ignite
{
    namespace odbc
    {
        const ProtocolVersion ProtocolVersion::VERSION_1_6_0(1);
        const ProtocolVersion ProtocolVersion::VERSION_1_8_0(MakeVersion(1,8,0));
        const ProtocolVersion ProtocolVersion::VERSION_UNKNOWN(INT64_MIN);

        ProtocolVersion::StringToVersionMap::value_type s2vInitVals[] = {
            std::make_pair("1.6.0", ProtocolVersion::VERSION_1_6_0),
            std::make_pair("1.8.0", ProtocolVersion::VERSION_1_8_0)
        };

        const ProtocolVersion::StringToVersionMap ProtocolVersion::stringToVersionMap(s2vInitVals,
            s2vInitVals + (sizeof(s2vInitVals) / sizeof(s2vInitVals[0])));

        ProtocolVersion::VersionToStringMap::value_type v2sInitVals[] = {
            std::make_pair(ProtocolVersion::VERSION_1_6_0, "1.6.0"),
            std::make_pair(ProtocolVersion::VERSION_1_8_0, "1.8.0")
        };

        const ProtocolVersion::VersionToStringMap ProtocolVersion::versionToStringMap(v2sInitVals,
            v2sInitVals + (sizeof(v2sInitVals) / sizeof(v2sInitVals[0])));

        ProtocolVersion::ProtocolVersion(int64_t val) :
            val(val)
        {
            // No-op.
        }

        int64_t ProtocolVersion::MakeVersion(uint16_t major, uint16_t minor, uint16_t maintenance)
        {
            const static int64_t MASK = 0x000000000000FFFFLL;
            return ((major & MASK) << 48) | ((minor & MASK) << 32) | ((maintenance & MASK) << 16);
        }

        const ProtocolVersion& ProtocolVersion::GetCurrent()
        {
            return VERSION_1_8_0;
        }

        ProtocolVersion ProtocolVersion::FromString(const std::string& version)
        {
            StringToVersionMap::const_iterator it = stringToVersionMap.find(common::ToLower(version));

            if (it == stringToVersionMap.end())
            {
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Invalid version format. Valid format is X.Y.Z, where X, Y and Z are major, "
                    "minor and maintenance versions of Ignite since which protocol is introduced.");
            }

            return it->second;
        }

        const std::string& ProtocolVersion::ToString() const
        {
            VersionToStringMap::const_iterator it = versionToStringMap.find(*this);

            if (it == versionToStringMap.end())
            {
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Unknown protocol version can not be converted to string.");
            }

            return it->second;
        }

        int64_t ProtocolVersion::GetIntValue() const
        {
            assert(!IsUnknown());

            return val;
        }

        bool ProtocolVersion::IsUnknown() const
        {
            return *this == VERSION_UNKNOWN;
        }

        bool operator==(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val == val2.val;
        }

        bool operator!=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val != val2.val;
        }

        bool operator<(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val < val2.val;
        }

        bool operator<=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val <= val2.val;
        }

        bool operator>(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val > val2.val;
        }

        bool operator>=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.val >= val2.val;
        }
    }
}

