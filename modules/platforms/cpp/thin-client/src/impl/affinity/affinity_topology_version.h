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

#ifndef _IGNITE_IMPL_THIN_AFFINITY_TOPOLOGY_VERSION
#define _IGNITE_IMPL_THIN_AFFINITY_TOPOLOGY_VERSION

#include <stdint.h>

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            // Forward declaration.
            class BinaryReaderImpl;
        }

        namespace thin
        {
            /** Affinity topology version. */
            class AffinityTopologyVersion
            {
            public:
                /**
                 * Default constructor.
                 */
                AffinityTopologyVersion() :
                    vmajor(0),
                    vminor(0)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param vmajor Major version part.
                 * @param vminor Minor version part.
                 */
                AffinityTopologyVersion(int64_t vmajor, int32_t vminor) :
                    vmajor(vmajor),
                    vminor(vminor)
                {
                    // No-op.
                }

                /**
                 * Get major part.
                 *
                 * @return Major part.
                 */
                int64_t GetMajor() const
                {
                    return vmajor;
                }

                /**
                 * Get minor part.
                 *
                 * @return Minor part.
                 */
                int32_t GetMinor() const
                {
                    return vminor;
                }

                /**
                 * Compare to another value.
                 *
                 * @param other Instance to compare to.
                 * @return Zero if equals, negative number if less and positive if more.
                 */
                int64_t Compare(const AffinityTopologyVersion& other) const
                {
                    int64_t res = vmajor - other.vmajor;

                    if (res == 0)
                        res = vminor - other.vminor;

                    return res;
                }

                /**
                 * Read using provided reader.
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl &reader);

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if equal.
                 */
                friend bool operator==(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) == 0;
                }

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if not equal.
                 */
                friend bool operator!=(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) != 0;
                }

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less.
                 */
                friend bool operator<(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) < 0;
                }

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if less or equal.
                 */
                friend bool operator<=(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) <= 0;
                }

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if greater.
                 */
                friend bool operator>(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) > 0;
                }

                /**
                 * Comparison operator.
                 *
                 * @param val1 First value.
                 * @param val2 Second value.
                 * @return True if greater or equal.
                 */
                friend bool operator>=(const AffinityTopologyVersion& val1, const AffinityTopologyVersion& val2)
                {
                    return val1.Compare(val2) >= 0;
                }

            private:
                /** Major part. */
                int64_t vmajor;

                /** Minor part. */
                int32_t vminor;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_AFFINITY_TOPOLOGY_VERSION