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

#include <cctype>
#include <algorithm>
#include <sstream>

#include <ignite/binary/binary.h>

#include <ignite/common/utils.h>

#include "impl/utility.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace utility
            {
                bool ParseSingleAddress(const std::string& value, network::TcpRange& tcpRange, uint16_t dfltPort)
                {
                    int64_t colonNum = std::count(value.begin(), value.end(), ':');

                    if (colonNum == 0)
                    {
                        tcpRange.host = value;
                        tcpRange.port = dfltPort;
                        tcpRange.range = 0;

                        return true;
                    }

                    if (colonNum != 1)
                        return false;

                    size_t colonPos = value.find(':');

                    tcpRange.host = value.substr(0, colonPos);

                    if (colonPos == value.size() - 1)
                        return false;

                    std::string portRange = value.substr(colonPos + 1);

                    return ParsePortRange(portRange, tcpRange.port, tcpRange.range);
                }

                void ParseAddress(
                    const std::string& value,
                    std::vector<network::TcpRange>& endPoints,
                    uint16_t dfltPort)
                {
                    size_t addrNum = std::count(value.begin(), value.end(), ',') + 1;

                    endPoints.reserve(endPoints.size() + addrNum);

                    std::string parsedAddr(value);

                    while (!parsedAddr.empty())
                    {
                        size_t addrBeginPos = parsedAddr.rfind(',');

                        if (addrBeginPos == std::string::npos)
                            addrBeginPos = 0;
                        else
                            ++addrBeginPos;

                        const char* addrBegin = parsedAddr.data() + addrBeginPos;
                        const char* addrEnd = parsedAddr.data() + parsedAddr.size();

                        std::string addr = common::StripSurroundingWhitespaces(addrBegin, addrEnd);

                        if (!addr.empty())
                        {
                            network::TcpRange tcpRange;

                            bool success = ParseSingleAddress(addr, tcpRange, dfltPort);

                            if (success)
                                endPoints.push_back(tcpRange);
                        }

                        if (!addrBeginPos)
                            break;

                        parsedAddr.erase(addrBeginPos - 1);
                    }
                }

                bool ParsePortRange(const std::string& value, uint16_t& port, uint16_t& range)
                {
                    size_t sepPos = value.find('.');

                    if (sepPos == std::string::npos)
                    {
                        range = 0;
                        port = ParsePort(value);

                        return port != 0;
                    }

                    if (sepPos + 2 > value.size() || value[sepPos + 1] != '.')
                        return false;

                    uint16_t rangeBegin = ParsePort(value.substr(0, sepPos));

                    if (!rangeBegin)
                        return false;

                    uint16_t rangeEnd = ParsePort(value.substr(sepPos + 2));

                    if (!rangeEnd)
                        return false;

                    if (rangeEnd < rangeBegin)
                        return false;

                    port = rangeBegin;
                    range = rangeEnd - rangeBegin;

                    return true;
                }

                uint16_t ParsePort(const std::string& value)
                {
                    std::string port = common::StripSurroundingWhitespaces(value.begin(), value.end());

                    if (!common::AllDigits(port))
                        return 0;

                    if (port.size() >= sizeof("65535"))
                        return 0;

                    int32_t intPort = 0;
                    std::stringstream conv;

                    conv << port;
                    conv >> intPort;

                    if (intPort <= 0 || intPort > 0xFFFF)
                        return 0;

                    return static_cast<uint16_t>(intPort);
                }

                int32_t GetCacheId(const char* cacheName)
                {
                    if (!cacheName)
                        return 0;

                    int32_t hash = 0;

                    int i = 0;

                    while (cacheName[i])
                    {
                        hash = 31 * hash + cacheName[i];

                        ++i;
                    }

                    return hash;
                }
            }
        }
    }
}

