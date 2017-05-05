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

#include <sstream>

#include <ignite/ignite_error.h>

#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        const ProtocolVersion ProtocolVersion::VERSION_2_1_0(ProtocolVersion(2,1,0));

        ProtocolVersion::VersionSet::value_type supportedArray[] = {
            ProtocolVersion::VERSION_2_1_0
        };

        const ProtocolVersion::VersionSet ProtocolVersion::supported(supportedArray,
            supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

        ProtocolVersion::ProtocolVersion(int16_t major, int16_t minor, int16_t maintenance) :
            major(major),
            minor(minor),
            maintenance(maintenance)
        {
            // No-op.
        }

        ProtocolVersion::ProtocolVersion() :
            major(0),
            minor(0),
            maintenance(0)
        {
            // No-op.
        }

        const ProtocolVersion::VersionSet& ProtocolVersion::GetSupported()
        {
            return supported;
        }

        const ProtocolVersion& ProtocolVersion::GetCurrent()
        {
            return VERSION_2_1_0;
        }

        void ThrowParseError()
        {
            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                "Invalid version format. Valid format is X.Y.Z, where X, Y and Z are major, "
                "minor and maintenance version parts of Ignite since which protocol is introduced.");
        }

        ProtocolVersion ProtocolVersion::FromString(const std::string& version)
        {
            ProtocolVersion res;

            std::stringstream buf(version);

            buf >> res.major;

            if (!buf.good())
                ThrowParseError();

            if (buf.get() != '.' || !buf.good())
                ThrowParseError();

            buf >> res.minor;

            if (!buf.good())
                ThrowParseError();

            if (buf.get() != '.' || !buf.good())
                ThrowParseError();

            buf >> res.maintenance;

            if (buf.bad())
                ThrowParseError();

            return res;
        }

        std::string ProtocolVersion::ToString() const
        {
            std::stringstream buf;
            buf << major << '.' << minor << '.' << maintenance;

            return buf.str();
        }

        int16_t ProtocolVersion::GetMajor() const
        {
            return major;
        }

        int16_t ProtocolVersion::GetMinor() const
        {
            return minor;
        }

        int16_t ProtocolVersion::GetMaintenance() const
        {
            return maintenance;
        }

        bool ProtocolVersion::IsSupported() const
        {
            return supported.count(*this) != 0;
        }

        int32_t ProtocolVersion::Compare(const ProtocolVersion& other) const
        {
            int32_t res = major - other.major;

            if (res == 0)
                res = minor - other.minor;

            if (res == 0)
                res = maintenance - other.maintenance;

            return res;
        }

        bool operator==(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) == 0;
        }

        bool operator!=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) != 0;
        }

        bool operator<(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) < 0;
        }

        bool operator<=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) <= 0;
        }

        bool operator>(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) > 0;
        }

        bool operator>=(const ProtocolVersion& val1, const ProtocolVersion& val2)
        {
            return val1.Compare(val2) >= 0;
        }
    }
}

