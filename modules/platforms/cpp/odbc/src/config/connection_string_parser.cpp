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

#include "ignite/common/utils.h"

#include "ignite/odbc/utility.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/config/config_tools.h"
#include "ignite/odbc/nested_tx_mode.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            const std::string ConnectionStringParser::Key::dsn                    = "dsn";
            const std::string ConnectionStringParser::Key::driver                 = "driver";
            const std::string ConnectionStringParser::Key::schema                 = "schema";
            const std::string ConnectionStringParser::Key::address                = "address";
            const std::string ConnectionStringParser::Key::server                 = "server";
            const std::string ConnectionStringParser::Key::port                   = "port";
            const std::string ConnectionStringParser::Key::distributedJoins       = "distributed_joins";
            const std::string ConnectionStringParser::Key::enforceJoinOrder       = "enforce_join_order";
            const std::string ConnectionStringParser::Key::protocolVersion        = "protocol_version";
            const std::string ConnectionStringParser::Key::pageSize               = "page_size";
            const std::string ConnectionStringParser::Key::replicatedOnly         = "replicated_only";
            const std::string ConnectionStringParser::Key::collocated             = "collocated";
            const std::string ConnectionStringParser::Key::lazy                   = "lazy";
            const std::string ConnectionStringParser::Key::skipReducerOnUpdate    = "skip_reducer_on_update";
            const std::string ConnectionStringParser::Key::sslMode                = "ssl_mode";
            const std::string ConnectionStringParser::Key::sslKeyFile             = "ssl_key_file";
            const std::string ConnectionStringParser::Key::sslCertFile            = "ssl_cert_file";
            const std::string ConnectionStringParser::Key::sslCaFile              = "ssl_ca_file";
            const std::string ConnectionStringParser::Key::user                   = "user";
            const std::string ConnectionStringParser::Key::password               = "password";
            const std::string ConnectionStringParser::Key::uid                    = "uid";
            const std::string ConnectionStringParser::Key::pwd                    = "pwd";
            const std::string ConnectionStringParser::Key::nestedTxMode           = "nested_tx_mode";

            ConnectionStringParser::ConnectionStringParser(Configuration& cfg):
                cfg(cfg)
            {
                // No-op.
            }

            ConnectionStringParser::~ConnectionStringParser()
            {
                // No-op.
            }

            void ConnectionStringParser::ParseConnectionString(const char* str, size_t len, char delimiter,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                std::string connect_str(str, len);

                while (connect_str.rbegin() != connect_str.rend() && *connect_str.rbegin() == 0)
                    connect_str.erase(connect_str.size() - 1);

                while (!connect_str.empty())
                {
                    size_t attr_begin = connect_str.rfind(delimiter);

                    if (attr_begin == std::string::npos)
                        attr_begin = 0;
                    else
                        ++attr_begin;

                    size_t attr_eq_pos = connect_str.rfind('=');

                    if (attr_eq_pos == std::string::npos)
                        attr_eq_pos = 0;

                    if (attr_begin < attr_eq_pos)
                    {
                        const char* key_begin = connect_str.data() + attr_begin;
                        const char* key_end = connect_str.data() + attr_eq_pos;

                        const char* value_begin = connect_str.data() + attr_eq_pos + 1;
                        const char* value_end = connect_str.data() + connect_str.size();

                        std::string key = common::StripSurroundingWhitespaces(key_begin, key_end);
                        std::string value = common::StripSurroundingWhitespaces(value_begin, value_end);

                        if (value[0] == '{' && value[value.size() - 1] == '}')
                            value = value.substr(1, value.size() - 2);

                        HandleAttributePair(key, value, diag);
                    }

                    if (!attr_begin)
                        break;

                    connect_str.erase(attr_begin - 1);
                }
            }

            void ConnectionStringParser::ParseConnectionString(const std::string& str,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                ParseConnectionString(str.data(), str.size(), ';', diag);
            }

            void ConnectionStringParser::ParseConfigAttributes(const char* str,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                size_t len = 0;

                // Getting list length. List is terminated by two '\0'.
                while (str[len] || str[len + 1])
                    ++len;

                ++len;

                ParseConnectionString(str, len, '\0', diag);
            }

            void ConnectionStringParser::HandleAttributePair(const std::string &key, const std::string &value,
                diagnostic::DiagnosticRecordStorage* diag)
            {
                std::string lKey = common::ToLower(key);

                if (lKey == Key::dsn)
                {
                    cfg.SetDsn(value);
                }
                else if (lKey == Key::schema)
                {
                    cfg.SetSchema(value);
                }
                else if (lKey == Key::address)
                {
                    std::vector<EndPoint> endPoints;

                    ParseAddress(value, endPoints, diag);

                    cfg.SetAddresses(endPoints);
                }
                else if (lKey == Key::server)
                {
                    cfg.SetHost(value);
                }
                else if (lKey == Key::port)
                {
                    if (value.empty())
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Port attribute value is empty. Using default value.", key, value));
                        }

                        return;
                    }

                    if (!common::AllDigits(value))
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Port attribute value contains unexpected characters."
                                    " Using default value.", key, value));
                        }

                        return;
                    }

                    if (value.size() >= sizeof("65535"))
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Port attribute value is too large. Using default value.", key, value));
                        }

                        return;
                    }

                    int32_t numValue = 0;
                    std::stringstream conv;

                    conv << value;
                    conv >> numValue;

                    if (numValue <= 0 || numValue > 0xFFFF)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Port attribute value is out of range. "
                                    "Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetTcpPort(static_cast<uint16_t>(numValue));
                }
                else if (lKey == Key::distributedJoins)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetDistributedJoins(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::enforceJoinOrder)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetEnforceJoinOrder(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::protocolVersion)
                {
                    try
                    {
                        ProtocolVersion version = ProtocolVersion::FromString(value);

                        if (!version.IsSupported())
                        {
                            if (diag)
                            {
                                diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                    "Specified version is not supported. Default value used.");
                            }

                            return;
                        }

                        cfg.SetProtocolVersion(version);
                    }
                    catch (IgniteError& err)
                    {
                        if (diag)
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, err.GetText());
                    }
                }
                else if (lKey == Key::pageSize)
                {
                    if (!common::AllDigits(value))
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Page size attribute value contains unexpected characters."
                                    " Using default value.", key, value));
                        }

                        return;
                    }

                    if (value.size() >= sizeof("4294967295"))
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Page size attribute value is too large."
                                    " Using default value.", key, value));
                        }

                        return;
                    }

                    int64_t numValue = 0;
                    std::stringstream conv;

                    conv << value;
                    conv >> numValue;

                    if (numValue <= 0 || numValue > 0xFFFFFFFFL)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Page size attribute value is out of range."
                                    " Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetPageSize(static_cast<int32_t>(numValue));
                }
                else if (lKey == Key::replicatedOnly)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetReplicatedOnly(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::collocated)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Using default value.", key, value));
                        }

                        return;
                    }

                    cfg.SetCollocated(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::lazy)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                        }

                        return;
                    }

                    cfg.SetLazy(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::skipReducerOnUpdate)
                {
                    BoolParseResult::Type res = StringToBool(value);

                    if (res == BoolParseResult::AI_UNRECOGNIZED)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                MakeErrorMessage("Unrecognized bool value. Defaulting to 'false'.", key, value));
                        }

                        return;
                    }

                    cfg.SetSkipReducerOnUpdate(res == BoolParseResult::AI_TRUE);
                }
                else if (lKey == Key::sslMode)
                {
                    ssl::SslMode::Type mode = ssl::SslMode::FromString(value);

                    if (mode == ssl::SslMode::UNKNOWN)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                "Specified SSL mode is not supported. Default value used ('disable').");
                        }

                        return;
                    }

                    cfg.SetSslMode(mode);
                }
                else if (lKey == Key::sslKeyFile)
                {
                    cfg.SetSslKeyFile(value);
                }
                else if (lKey == Key::sslCertFile)
                {
                    cfg.SetSslCertFile(value);
                }
                else if (lKey == Key::sslCaFile)
                {
                    cfg.SetSslCaFile(value);
                }
                else if (lKey == Key::driver)
                {
                    cfg.SetDriver(value);
                }
                else if (lKey == Key::user || lKey == Key::uid)
                {
                    if (!cfg.GetUser().empty() && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            "Re-writing USER (have you specified it several times?");
                    }

                    cfg.SetUser(value);
                }
                else if (lKey == Key::password || lKey == Key::pwd)
                {
                    if (!cfg.GetPassword().empty() && diag)
                    {
                        diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                            "Re-writing PASSWORD (have you specified it several times?");
                    }

                    cfg.SetPassword(value);
                }
                else if (lKey == Key::nestedTxMode)
                {
                    NestedTxMode::Type mode = NestedTxMode::FromString(value);

                    if (mode == NestedTxMode::AI_UNKNOWN)
                    {
                        if (diag)
                        {
                            diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                                "Specified nested transaction mode is not supported. Default value used ('error').");
                        }

                        return;
                    }

                    cfg.SetNestedTxMode(mode);
                }
                else if (diag)
                {
                    std::stringstream stream;

                    stream << "Unknown attribute: '" << key << "'. Ignoring.";

                    diag->AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, stream.str());
                }
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

