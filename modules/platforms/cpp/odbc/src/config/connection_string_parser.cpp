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

#include <vector>
#include <string>

#include "ignite/common/utils.h"

#include "ignite/odbc/utility.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/ssl/ssl_mode.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            ConnectionStringParser::ConnectionStringParser(Configuration& cfg):
                cfg(cfg)
            {
                // No-op.
            }

            void ConnectionStringParser::HandleAttributePair(const std::string &key, const std::string &value,
                diagnostic::Diagnosable* diag)
            {
                std::string lKey = common::ToLower(key);

                if (lKey == Configuration::Key::dsn)
                {
                    cfg.SetDsn(value);
                }
                else if (lKey == Configuration::Key::schema)
                {
                    cfg.SetSchema(value);
                }
                else if (lKey == Configuration::Key::address)
                {
                    std::vector<Configuration::EndPoint> endPoints;

                    ParseAddress(value, endPoints, diag);

                    if (endPoints.empty())
                    {
                        throw OdbcError(SqlState::SHY000_GENERAL_ERROR,
                            "No valid addresses found. Aborting connection.");
                    }

                    cfg.SetAddresses(endPoints);
                }
                else if (lKey == Configuration::Key::server)
                {
                    cfg.SetServer(value);
                }
                else if (lKey == Configuration::Key::port)
                {
                    if (!common::AllOf(value.begin(), value.end(), std::isdigit))
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Port attribute value contains unexpected characters."
                                " Using default value.", key, value));

                        return;
                    }

                    if (value.size() >= sizeof("65535"))
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Port attribute value is too large. Using default value.", key, value));

                        return;
                    }

                    int32_t numValue = 0;
                    std::stringstream conv;

                    conv << value;
                    conv >> numValue;

                    if (numValue <= 0 || numValue > 0xFFFF)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Port attribute value is out of range. Using default value.", key, value));

                        return;
                    }

                    cfg.SetTcpPort(static_cast<uint16_t>(numValue));
                }
                else if (lKey == Configuration::Key::distributedJoins)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetDistributedJoins(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::enforceJoinOrder)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetEnforceJoinOrder(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::protocolVersion)
                {
                    ProtocolVersion version = ProtocolVersion::FromString(value);

                    if (!version.IsSupported())
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            "Specified version is not supported. Default value used.");
                    }
                    else
                        cfg.SetProtocolVersion(version);
                }
                else if (lKey == Configuration::Key::pageSize)
                {
                    if (!common::AllOf(value.begin(), value.end(), std::isdigit))
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Page size attribute value contains unexpected characters."
                                " Using default value.", key, value));

                        return;
                    }

                    if (value.size() >= sizeof("4294967295"))
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Page size attribute value is too large."
                                " Using default value.", key, value));

                        return;
                    }

                    int64_t numValue = 0;
                    std::stringstream conv;

                    conv << value;
                    conv >> numValue;

                    if (numValue <= 0 || numValue > 0xFFFFFFFFLL)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Page size attribute value is out of range."
                                " Using default value.", key, value));

                        return;
                    }

                    cfg.SetPageSize(numValue);
                }
                else if (lKey == Configuration::Key::replicatedOnly)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetReplicatedOnly(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::collocated)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetCollocated(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::lazy)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetLazy(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::skipReducerOnUpdate)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                    }

                    cfg.SetSkipReducerOnUpdate(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Configuration::Key::sslMode)
                {
                    ssl::SslMode::Type mode = ssl::SslMode::FromString(value);

                    if (mode == ssl::SslMode::UNKNOWN)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            "Specified SSL mode is not supported. Default value used ('disable').");
                    }
                    else
                        cfg.SetSslMode(mode);
                }
                else if (lKey == Configuration::Key::sslKeyFile)
                {
                    cfg.SetSslKeyFile(value);
                }
                else if (lKey == Configuration::Key::sslCertFile)
                {
                    cfg.SetSslCertFile(value);
                }
                else if (lKey == Configuration::Key::sslCaFile)
                {
                    cfg.SetSslCaFile(value);
                }
                else if (lKey != Configuration::Key::driver)
                {
                    if (diag)
                    {
                        std::stringstream stream;

                        stream << "Unknown attribute: '" << key << "'. Ignoring.";

                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());
                    }
                }
            }

            void ConnectionStringParser::ParseAddress(const std::string& value,
                std::vector<Configuration::EndPoint>& endPoints, diagnostic::Diagnosable* diag)
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

                    std::string addr = utility::RemoveSurroundingSpaces(addrBegin, addrEnd);

                    if (!addr.empty())
                    {
                        Configuration::EndPoint endPoint;

                        bool success = ParseSingleAddress(addr, endPoint, diag);

                        if (success)
                            endPoints.push_back(endPoint);
                    }

                    if (!addrBeginPos)
                        break;

                    parsedAddr.erase(addrBeginPos - 1);
                }
            }

            bool ConnectionStringParser::ParseSingleAddress(const std::string& value, Configuration::EndPoint& endPoint,
                diagnostic::Diagnosable* diag)
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

                    diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                size_t pos = value.find(':');

                endPoint.host = value.substr(0, pos);

                if (pos == value.size() - 1)
                {
                    endPoint.port = Configuration::DefaultValue::port;

                    return true;
                }

                std::string port = value.substr(pos + 1);

                if (!common::AllOf(port.begin(), port.end(), std::isdigit))
                {
                    std::stringstream stream;

                    stream << "Unexpected port characters: '" << port 
                        << "' in the following address: '" << value << "'. Ignoring address.";

                    diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                if (port.size() >= sizeof("65535"))
                {
                    std::stringstream stream;

                    stream << "Port value is too large: '" << port
                        << "' in the following address: '" << value << "'. Ignoring address.";

                    diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                int32_t intPort = 0;
                std::stringstream conv;

                conv << port;
                conv >> intPort;

                if (intPort <= 0 || intPort > 0xFFFF)
                {
                    std::stringstream stream;

                    stream << "Port value is too large: '" << port
                        << "' in the following address: '" << value << "'. Ignoring address.";

                    diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());

                    return false;
                }

                endPoint.port = static_cast<uint16_t>(intPort);

                return true;
            }

            ConnectionStringParser::BoolParseResult::Type ConnectionStringParser::StringToBool(const std::string& value)
            {
                std::string lower = common::ToLower(value);

                if (lower == "true")
                    return BoolParseResult::AI_TRUE;

                if (lower == "false")
                    return BoolParseResult::AI_FALSE;

                return BoolParseResult::AI_UNRECOGNIZED;
            }

            std::string ConnectionStringParser::MakeErrorMessage(const std::string& msg, const std::string& key,
                const std::string& value)
            {
                std::stringstream stream;

                stream << msg << " [key='" << key << "', value='" << value << "']";

                return stream.str();
            }
        }
    }
}

