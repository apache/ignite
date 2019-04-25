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

#ifndef _IGNITE_IMPL_THIN_UTILITY
#define _IGNITE_IMPL_THIN_UTILITY

#include <vector>

#include <ignite/network/tcp_range.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace utility
            {
                /**
                 * Parse address.
                 *
                 * @param value String value to parse.
                 * @param endPoints End ponts list.
                 * @param dfltPort Default port.
                 */
                void ParseAddress(const std::string& value,
                    std::vector<network::TcpRange>& endPoints,
                    uint16_t dfltPort);

                /**
                 * Parse single address.
                 *
                 * @param value String value to parse.
                 * @param tcpRange TCP range.
                 * @param dfltPort Default port.
                 * @return @c true, if parsed successfully, and @c false otherwise.
                 */
                bool ParseSingleAddress(const std::string& value, network::TcpRange& tcpRange, uint16_t dfltPort);

                /**
                 * Parse single network port.
                 *
                 * @param value String value to parse.
                 * @param port Port range begin.
                 * @param range Number of ports in range.
                 * @return @c Port value on success and zero on failure.
                 */
                bool ParsePortRange(const std::string& value, uint16_t& port, uint16_t& range);

                /**
                 * Parse single network port.
                 *
                 * @param value String value to parse.
                 * @return @c Port value on success and zero on failure.
                 */
                uint16_t ParsePort(const std::string& value);

                /**
                 * Get cache ID for the name.
                 *
                 * @param cacheName Cache name.
                 * @return Cache ID.
                 */
                int32_t GetCacheId(const char* cacheName);
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_UTILITY
