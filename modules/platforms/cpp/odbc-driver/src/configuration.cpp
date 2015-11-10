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

#include <cstring>

#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>

#include "utility.h"
#include "configuration.h"

namespace ignite
{
    namespace odbc
    {
        /** Default values for configuration. */
        namespace dflt
        {
            /** Default value for host param. */
            const std::string host = "localhost";

            /** Default value for port param. */
            const uint16_t port = 11443;
        }

        /** Connection attribute keywords. */
        namespace attrkey
        {
            /** Connection attribute keyword for server host. */
            const std::string host = "server";

            /** Connection attribute keyword for server port. */
            const std::string port = "port";
        }

        Configuration::Configuration() : host(dflt::host), port(dflt::port)
        {
            // No-op.
        }

        Configuration::Configuration(const char * str, size_t len) : host(dflt::host), port(dflt::port)
        {
            FillFromConnectString(str, len);
        }

        Configuration::~Configuration()
        {
            // No-op.
        }

        void Configuration::FillFromConnectString(const char* str, size_t len)
        {
            ArgumentMap connect_arguments;

            ParseConnectString(str, len, connect_arguments);

            ArgumentMap::const_iterator it;

            it = connect_arguments.find(attrkey::host);
            if (it != connect_arguments.end())
                host = it->second;

            it = connect_arguments.find(attrkey::port);
            if (it != connect_arguments.end())
                port = atoi(it->second.c_str());
        }

        std::string Configuration::ToConnectString() const
        {
            std::stringstream connect_string_buffer;

            connect_string_buffer << "Driver={Apache Ignite};";
            connect_string_buffer << attrkey::host << '=' << host << ';';
            connect_string_buffer << attrkey::port << '=' << port << ';';

            return connect_string_buffer.str();
        }

        void Configuration::ParseConnectString(const char * str, size_t len, ArgumentMap & args) const
        {
            std::string connect_str(str, len);
            args.clear();

            while (!connect_str.empty())
            {
                size_t attr_begin = connect_str.rfind(';');

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
    }
}

