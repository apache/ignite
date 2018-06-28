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

#include <ignite/common/utils.h>
#include <ignite/impl/thin/utility.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace utility
            {
                bool ParseSingleAddress(const std::string& value, net::EndPoint& endPoint, uint16_t dfltPort)
                {
                    int64_t colonNum = std::count(value.begin(), value.end(), ':');

                    if (colonNum == 0)
                    {
                        endPoint.host = value;
                        endPoint.port = dfltPort;

                        return true;
                    }

                    if (colonNum != 1)
                    {
                        //TODO: implement logging
                        //stream << "Unexpected number of ':' characters in the following address: '"
                        //   << value << "'. Ignoring address.";

                        return false;
                    }

                    size_t colonPos = value.find(':');

                    endPoint.host = value.substr(0, colonPos);

                    if (colonPos == value.size() - 1)
                    {
                        //TODO: implement logging
                        //stream << "Port is missing in the following address: '" << value << "'. Ignoring address.";

                        return false;
                    }

                    std::string portRange = value.substr(colonPos + 1);

                    return ParsePortRange(portRange, endPoint.port, endPoint.range);
                }

                void ParseAddress(const std::string& value, std::vector<net::EndPoint>& endPoints, uint16_t dfltPort)
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
                            net::EndPoint endPoint;

                            bool success = ParseSingleAddress(addr, endPoint, dfltPort);

                            if (success)
                                endPoints.push_back(endPoint);
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
                    {
                        //TODO: implement logging
                        //stream << "Unexpected number of '.' characters in the following address: '"
                        //    << value << "'. Ignoring address.";

                        return false;
                    }

                    uint16_t rangeBegin = ParsePort(value.substr(0, sepPos));

                    if (!rangeBegin)
                        return false;

                    uint16_t rangeEnd = ParsePort(value.substr(sepPos + 2));

                    if (!rangeEnd)
                        return false;

                    if (rangeEnd < rangeBegin)
                    {
                        //TODO: implement logging
                        //stream << "Port range end is less than port range begin in the following address: '"
                        //    << value << "'. Ignoring address.";

                        return false;
                    }

                    port = rangeBegin;
                    range = rangeEnd - rangeBegin;

                    return true;
                }

                uint16_t ParsePort(const std::string& value)
                {
                    std::string port = common::StripSurroundingWhitespaces(value.begin(), value.end());

                    if (!common::AllOf(port.begin(), port.end(), &isdigit))
                    {
                        //TODO: implement logging
                        //stream << "Unexpected port characters: '" << port << "'. Ignoring address.";

                        return 0;
                    }

                    if (port.size() >= sizeof("65535"))
                    {
                        //TODO: implement logging
                        //stream << "Port value is too large: '" << port << "'. Ignoring address.";

                        return 0;
                    }

                    int32_t intPort = 0;
                    std::stringstream conv;

                    conv << port;
                    conv >> intPort;

                    if (intPort <= 0 || intPort > 0xFFFF)
                    {
                        //TODO: implement logging
                        //stream << "Port value is out of range: '" << port << "'. Ignoring address.";

                        return 0;
                    }

                    return static_cast<uint16_t>(intPort);
                }
            }
        }
    }
}

