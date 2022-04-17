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

#include <ignite/odbc/utility.h>
#include <ignite/odbc/config/config_tools.h>
#include <ignite/odbc/config/configuration.h>

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            std::string AddressesToString(const std::vector<EndPoint>& addresses)
            {
                std::stringstream stream;

                std::vector<EndPoint>::const_iterator it = addresses.begin();

                if (it != addresses.end())
                {
                    stream << it->host << ':' << it->port;
                    ++it;
                }

                for (; it != addresses.end(); ++it)
                {
                    stream << ',' << it->host << ':' << it->port;
                }

                return stream.str();
            }

            void ParseAddress(const std::string& value, std::vector<EndPoint>& endPoints,
                diagnostic::DiagnosticRecordStorage* diag)
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
                        EndPoint endPoint;

                        bool success = ParseSingleAddress(addr, endPoint, diag);

                        if (success)
                            endPoints.push_back(endPoint);
                    }

                    if (!addrBeginPos)
                        break;

                    parsedAddr.erase(addrBeginPos - 1);
                }
            }

            bool ParseSingleAddress(const std::string& value, EndPoint& endPoint,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                int64_t colonNum = std::count(value.begin(), value.end(), ':');

                if (colonNum == 0)
                {
                    endPoint.host = value;
                    endPoint.port = Configuration::DefaultValue::port;

                    return true;
                }

                if (colonNum != 1)
                {
                    std::stringstream stream;

                    stream << "Unexpected number of ':' characters in the following address: '"
                        << value << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                size_t colonPos = value.find(':');

                endPoint.host = value.substr(0, colonPos);

                if (colonPos == value.size() - 1)
                {
                    std::stringstream stream;

                    stream << "Port is missing in the following address: '" << value << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                std::string portRange = value.substr(colonPos + 1);

                if (!ParsePortRange(portRange, endPoint.port, endPoint.range, diag))
                    return false;

                return true;
            }

            bool ParsePortRange(const std::string& value, uint16_t& port, uint16_t& range,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                size_t sepPos = value.find('.');

                if (sepPos == value.npos)
                {
                    range = 0;
                    port = ParsePort(value, diag);

                    if (!port)
                        return false;

                    return true;
                }

                if (sepPos + 2 > value.size() || value[sepPos + 1] != '.')
                {
                    std::stringstream stream;

                    stream << "Unexpected number of '.' characters in the following address: '"
                        << value << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                uint16_t rangeBegin = ParsePort(value.substr(0, sepPos), diag);

                if (!rangeBegin)
                    return false;

                uint16_t rangeEnd = ParsePort(value.substr(sepPos + 2), diag);

                if (!rangeEnd)
                    return false;

                if (rangeEnd < rangeBegin)
                {
                    std::stringstream stream;

                    stream << "Port range end is less than port range begin in the following address: '"
                        << value << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                port = rangeBegin;
                range = rangeEnd - rangeBegin;

                return true;
            }

            uint16_t ParsePort(const std::string& value, diagnostic::DiagnosticRecordStorage* diag)
            {
                std::string port = common::StripSurroundingWhitespaces(value.begin(), value.end());

                if (!common::AllDigits(port))
                {
                    std::stringstream stream;

                    stream << "Unexpected port characters: '" << port << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return 0;
                }

                if (port.size() >= sizeof("65535"))
                {
                    std::stringstream stream;

                    stream << "Port value is too large: '" << port << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return 0;
                }

                int32_t intPort = 0;
                std::stringstream conv;

                conv << port;
                conv >> intPort;

                if (intPort <= 0 || intPort > 0xFFFF)
                {
                    std::stringstream stream;

                    stream << "Port value is out of range: '" << port << "'. Ignoring address.";

                    if (diag)
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return 0;
                }

                return static_cast<uint16_t>(intPort);
            }
        }
    }
}

