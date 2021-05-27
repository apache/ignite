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
        const ProtocolVersion ProtocolVersion::VERSION_2_1_0(2, 1, 0);
        const ProtocolVersion ProtocolVersion::VERSION_2_1_5(2, 1, 5);
        const ProtocolVersion ProtocolVersion::VERSION_2_3_0(2, 3, 0);
        const ProtocolVersion ProtocolVersion::VERSION_2_3_2(2, 3, 2);
        const ProtocolVersion ProtocolVersion::VERSION_2_5_0(2, 5, 0);
        const ProtocolVersion ProtocolVersion::VERSION_2_7_0(2, 7, 0);
        const ProtocolVersion ProtocolVersion::VERSION_2_8_0(2, 8, 0);

        ProtocolVersion::VersionSet::value_type supportedArray[] = {
            ProtocolVersion::VERSION_2_1_0,
            ProtocolVersion::VERSION_2_1_5,
            ProtocolVersion::VERSION_2_3_0,
            ProtocolVersion::VERSION_2_3_2,
            ProtocolVersion::VERSION_2_5_0,
            ProtocolVersion::VERSION_2_7_0,
            ProtocolVersion::VERSION_2_8_0
        };

        const ProtocolVersion::VersionSet ProtocolVersion::supported(supportedArray,
            supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

        ProtocolVersion::ProtocolVersion(int16_t vmajor, int16_t vminor, int16_t vmaintenance) :
            vmajor(vmajor),
            vminor(vminor),
            vmaintenance(vmaintenance)
        {
            // No-op.
        }

        ProtocolVersion::ProtocolVersion() :
            vmajor(0),
            vminor(0),
            vmaintenance(0)
        {
            // No-op.
        }

        const ProtocolVersion::VersionSet& ProtocolVersion::GetSupported()
        {
            return supported;
        }

        const ProtocolVersion& ProtocolVersion::GetCurrent()
        {
            return VERSION_2_8_0;
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

            buf >> res.vmajor;

            if (!buf.good())
                ThrowParseError();

            if (buf.get() != '.' || !buf.good())
                ThrowParseError();

            buf >> res.vminor;

            if (!buf.good())
                ThrowParseError();

            if (buf.get() != '.' || !buf.good())
                ThrowParseError();

            buf >> res.vmaintenance;

            if (buf.bad())
                ThrowParseError();

            return res;
        }

        std::string ProtocolVersion::ToString() const
        {
            std::stringstream buf;
            buf << vmajor << '.' << vminor << '.' << vmaintenance;

            return buf.str();
        }

        int16_t ProtocolVersion::GetMajor() const
        {
            return vmajor;
        }

        int16_t ProtocolVersion::GetMinor() const
        {
            return vminor;
        }

        int16_t ProtocolVersion::GetMaintenance() const
        {
            return vmaintenance;
        }

        bool ProtocolVersion::IsSupported() const
        {
            return supported.count(*this) != 0;
        }

        int32_t ProtocolVersion::Compare(const ProtocolVersion& other) const
        {
            int32_t res = vmajor - other.vmajor;

            if (res == 0)
                res = vminor - other.vminor;

            if (res == 0)
                res = vmaintenance - other.vmaintenance;

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

