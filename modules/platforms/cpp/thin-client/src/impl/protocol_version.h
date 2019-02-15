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

#ifndef _IGNITE_IMPL_THIN_PROTOCOL_VERSION
#define _IGNITE_IMPL_THIN_PROTOCOL_VERSION

#include <stdint.h>

#include <string>
#include <set>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Protocol version. */
            class ProtocolVersion
            {
            public:
                /**
                 * Parse string and extract protocol version.
                 *
                 * @throw IgniteException if version can not be parsed.
                 * @param version Version string to parse.
                 * @return Protocol version.
                 */
                static ProtocolVersion FromString(const std::string& version);

                /**
                 * Convert to string value.
                 *
                 * @return Protocol version.
                 */
                std::string ToString() const;

                /**
                 * Default constructor.
                 */
                ProtocolVersion();

                /**
                 * Constructor.
                 *
                 * @param vmajor Major version part.
                 * @param vminor Minor version part.
                 * @param vmaintenance Maintenance version part.
                 */
                ProtocolVersion(int16_t vmajor, int16_t vminor, int16_t vmaintenance);

                /**
                 * Get major part.
                 *
                 * @return Major part.
                 */
                int16_t GetMajor() const;

                /**
                 * Get minor part.
                 *
                 * @return Minor part.
                 */
                int16_t GetMinor() const;

                /**
                 * Get maintenance part.
                 *
                 * @return Maintenance part.
                 */
                int16_t GetMaintenance() const;

                /**
                 * Compare to another value.
                 *
                 * @param other Instance to compare to.
                 * @return Zero if equeals, negative number if less and positive if more.
                 */
                int32_t Compare(const ProtocolVersion& other) const;

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if equal.
                 */
                friend bool operator==(const ProtocolVersion& val1, const ProtocolVersion& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if not equal.
                 */
                friend bool operator!=(const ProtocolVersion& val1, const ProtocolVersion& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less.
                 */
                friend bool operator<(const ProtocolVersion& val1, const ProtocolVersion& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less or equal.
                 */
                friend bool operator<=(const ProtocolVersion& val1, const ProtocolVersion& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if gretter.
                 */
                friend bool operator>(const ProtocolVersion& val1, const ProtocolVersion& val2);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if gretter or equal.
                 */
                friend bool operator>=(const ProtocolVersion& val1, const ProtocolVersion& val2);

            private:
                /** Major part. */
                int16_t vmajor;

                /** Minor part. */
                int16_t vminor;

                /** Maintenance part. */
                int16_t vmaintenance;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_PROTOCOL_VERSION