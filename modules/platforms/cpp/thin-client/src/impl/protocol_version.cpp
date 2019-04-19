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

#include <sstream>

#include <ignite/ignite_error.h>

#include "impl/protocol_version.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
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
}

