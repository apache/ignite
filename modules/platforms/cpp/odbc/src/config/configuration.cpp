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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/config/configuration.h"

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            const std::string Configuration::DefaultValue::dsn    = "Apache Ignite DSN";
            const std::string Configuration::DefaultValue::driver = "Apache Ignite";
            const std::string Configuration::DefaultValue::host   = "localhost";
            const std::string Configuration::DefaultValue::port   = "10800";
            const std::string Configuration::DefaultValue::cache  = "";

            const std::string Configuration::Key::dsn    = "dsn";
            const std::string Configuration::Key::driver = "driver";
            const std::string Configuration::Key::host   = "server";
            const std::string Configuration::Key::port   = "port";
            const std::string Configuration::Key::cache  = "cache";

            Configuration::Configuration() :
                arguments()
            {
                // No-op.
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
            }

            void Configuration::FillFromConnectString(const std::string& str)
            {
                FillFromConnectString(str.data(), str.size());
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
                        connect_string_buffer << it->first << '=' << it->second << ';';
                    else
                        connect_string_buffer << it->first << "={" << it->second << "};";
                }

                return connect_string_buffer.str();
            }

            void Configuration::FillFromConfigAttributes(const char * attributes)
            {
                // Initializing map.
                arguments.clear();

                size_t len = 0;

                // Getting list length. List is terminated by two '\0'.
                while (attributes[len] || attributes[len + 1])
                    ++len;

                ++len;

                ParseAttributeList(attributes, len, '\0', arguments);
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

            const std::string& Configuration::GetStringValue(const std::string& key, const std::string& dflt) const
            {
                ArgumentMap::const_iterator it = arguments.find(common::ToLower(key));

                if (it != arguments.end())
                    return it->second;

                return dflt;
            }
        }
    }
}

