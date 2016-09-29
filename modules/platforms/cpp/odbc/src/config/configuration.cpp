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

#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>

#include "ignite/common/common.h"
#include "ignite/common/utils.h"

#include "ignite/odbc/utility.h"
#include "ignite/odbc/config/configuration.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            const std::string Configuration::Key::dsn               = "dsn";
            const std::string Configuration::Key::driver            = "driver";
            const std::string Configuration::Key::cache             = "cache";
            const std::string Configuration::Key::address           = "address";
            const std::string Configuration::Key::server            = "server";
            const std::string Configuration::Key::port              = "port";
            const std::string Configuration::Key::distributedJoins  = "distributed_joins";
            const std::string Configuration::Key::enforceJoinOrder  = "enforce_join_order";
            const std::string Configuration::Key::protocolVersion   = "protocol_version";
            const std::string Configuration::Key::pageSize          = "page_size";

            const std::string Configuration::DefaultValue::dsn      = "Apache Ignite DSN";
            const std::string Configuration::DefaultValue::driver   = "Apache Ignite";
            const std::string Configuration::DefaultValue::cache    = "";
            const std::string Configuration::DefaultValue::address  = "";
            const std::string Configuration::DefaultValue::server   = "";

            const uint16_t Configuration::DefaultValue::port    = 10800;
            const int32_t Configuration::DefaultValue::pageSize = 1024;

            const bool Configuration::DefaultValue::distributedJoins = false;
            const bool Configuration::DefaultValue::enforceJoinOrder = false;

            const ProtocolVersion& Configuration::DefaultValue::protocolVersion = ProtocolVersion::GetCurrent();

            Configuration::Configuration() :
                arguments()
            {
                ParseAddress(DefaultValue::address, endPoint);
            }

            Configuration::~Configuration()
            {
                // No-op.
            }

            void Configuration::FillFromConnectString(const char* str, size_t len)
            {
                // Initializing map.
                arguments.clear();

                // Initializing DSN to empty string.
                arguments[Key::dsn].clear();

                // Ignoring terminating zero byte if present.
                // Some Driver Managers pass zero-terminated connection string
                // while others don't.
                if (len && !str[len - 1])
                    --len;

                ParseAttributeList(str, len, ';', arguments);

                ArgumentMap::const_iterator it = arguments.find(Key::address);
                if (it != arguments.end())
                {
                    // Parsing address.
                    ParseAddress(it->second, endPoint);
                }
                else
                {
                    endPoint.host = GetStringValue(Key::server, DefaultValue::server);
                    endPoint.port = static_cast<uint16_t>(GetIntValue(Key::port, DefaultValue::port));
                }
            }

            std::string Configuration::ToConnectString() const
            {
                std::stringstream connect_string_buffer;

                for (ArgumentMap::const_iterator it = arguments.begin(); it != arguments.end(); ++it)
                {
                    const std::string& key = it->first;
                    const std::string& value = it->second;

                    if (value.empty())
                        continue;

                    if (value.find(' ') == std::string::npos)
                        connect_string_buffer << key << '=' << value << ';';
                    else
                        connect_string_buffer << key << "={" << value << "};";
                }

                return connect_string_buffer.str();
            }

            void Configuration::FillFromConfigAttributes(const char* attributes)
            {
                // Initializing map.
                arguments.clear();

                size_t len = 0;

                // Getting list length. List is terminated by two '\0'.
                while (attributes[len] || attributes[len + 1])
                    ++len;

                ++len;

                ParseAttributeList(attributes, len, '\0', arguments);

                ArgumentMap::const_iterator it = arguments.find(Key::address);
                if (it != arguments.end())
                {
                    // Parsing address.
                    ParseAddress(it->second, endPoint);
                }
                else
                {
                    endPoint.host = GetStringValue(Key::server, DefaultValue::server);
                    endPoint.port = static_cast<uint16_t>(GetIntValue(Key::port, DefaultValue::port));
                }
            }

            void Configuration::SetTcpPort(uint16_t port)
            {
                arguments[Key::port] = common::LexicalCast<std::string>(port);

                ArgumentMap::const_iterator it = arguments.find(Key::address);

                if (it == arguments.end())
                    endPoint.port = port;
            }

            void Configuration::SetHost(const std::string& server)
            {
                arguments[Key::server] = server;

                ArgumentMap::const_iterator it = arguments.find(Key::address);

                if (it == arguments.end())
                    endPoint.host = server;
            }

            void Configuration::SetAddress(const std::string& address)
            {
                arguments[Key::address] = address;

                ParseAddress(address, endPoint);
            }

            ProtocolVersion Configuration::GetProtocolVersion() const
            {
                ArgumentMap::const_iterator it = arguments.find(Key::protocolVersion);

                if (it != arguments.end())
                    return ProtocolVersion::FromString(it->second);

                return DefaultValue::protocolVersion;
            }

            const std::string& Configuration::GetStringValue(const std::string& key, const std::string& dflt) const
            {
                ArgumentMap::const_iterator it = arguments.find(common::ToLower(key));

                if (it != arguments.end())
                    return it->second;

                return dflt;
            }

            int64_t Configuration::GetIntValue(const std::string& key, int64_t dflt) const
            {
                ArgumentMap::const_iterator it = arguments.find(common::ToLower(key));

                if (it != arguments.end())
                {
                    const std::string& val = it->second;

                    if (!common::AllOf(val.begin(), val.end(), isdigit))
                        IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid argument value: Integer value is expected.", "key", key);

                    return common::LexicalCast<int64_t>(val);
                }

                return dflt;
            }

            bool Configuration::GetBoolValue(const std::string& key, bool dflt) const
            {
                ArgumentMap::const_iterator it = arguments.find(common::ToLower(key));

                if (it != arguments.end())
                {
                    std::string lowercaseVal = common::ToLower(it->second);

                    if (lowercaseVal != "true" && lowercaseVal != "false")
                        IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid argument value: Boolean value is expected (true or false).", "key", key);

                    return lowercaseVal == "true";
                }

                return dflt;
            }

            void Configuration::SetBoolValue(const std::string& key, bool val)
            {
                arguments[key] = val ? "true" : "false";
            }

            void Configuration::ParseAttributeList(const char * str, size_t len, char delimeter, ArgumentMap & args)
            {
                std::string connect_str(str, len);

                while (!connect_str.empty())
                {
                    size_t attr_begin = connect_str.rfind(delimeter);

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

                        std::string key = utility::RemoveSurroundingSpaces(key_begin, key_end);
                        std::string value = utility::RemoveSurroundingSpaces(value_begin, value_end);

                        utility::IntoLower(key);

                        if (value.front() == '{' && value.back() == '}')
                            value = value.substr(1, value.size() - 2);

                        args[key] = value;
                    }

                    if (!attr_begin)
                        break;

                    connect_str.erase(attr_begin - 1);
                }
            }

            void Configuration::ParseAddress(const std::string& address, EndPoint& res)
            {
                int64_t colonNum = std::count(address.begin(), address.end(), ':');

                if (colonNum == 0)
                {
                    res.host = address;
                    res.port = DefaultValue::port;
                }
                else if (colonNum == 1)
                {
                    size_t pos = address.find(':');

                    if (pos == address.size() - 1)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid address format: no port after colon");

                    res.host = address.substr(0, pos);

                    std::string port = address.substr(pos + 1);

                    if (!common::AllOf(port.begin(), port.end(), isdigit))
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid address format: port can only contain digits");

                    int32_t intPort = common::LexicalCast<int32_t>(port);

                    if (port.size() > sizeof("65535") - 1 || intPort > UINT16_MAX)
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid address format: Port value is too large,"
                            " valid value should be in range from 1 to 65535");
                    }

                    if (intPort == 0)
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Invalid address format: Port value can not be zero");

                    res.port = static_cast<uint16_t>(intPort);
                }
                else
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                        "Invalid address format: too many colons");
            }
        }
    }
}

