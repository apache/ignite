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
            /** Default values for configuration. */
            namespace dflt
            {
                /** Default value for DSN attribute. */
                const std::string dsn = "Default Apache Ignite DSN";

                /** Default value for Driver attribute. */
                const std::string driver = "Apache Ignite";

                /** Default value for host attribute. */
                const std::string host = "";

                /** Default value for port attribute. */
                const uint16_t port = 10800;

                /** Default value for cache attribute. */
                const std::string cache = "";
            }

            /** Connection attribute keywords. */
            namespace attrkey
            {
                /** Connection attribute keyword for DSN attribute. */
                const std::string dsn = "dsn";
            
                /** Connection attribute keyword for Driver attribute. */
                const std::string driver = "driver";

                /** Connection attribute keyword for server host attribute. */
                const std::string host = "server";

                /** Connection attribute keyword for server port attribute. */
                const std::string port = "port";

                /** Connection attribute keyword for cache attribute. */
                const std::string cache = "cache";
            }

            Configuration::Configuration() :
                dsn(dflt::dsn), driver(dflt::driver),
                host(dflt::host), port(dflt::port),
                cache(dflt::cache)
            {
                // No-op.
            }

            Configuration::~Configuration()
            {
                // No-op.
            }

            void Configuration::FillFromConnectString(const char* str, size_t len)
            {
                ArgumentMap connect_attributes;

                // Ignoring terminating zero byte if present.
                // Some Driver Managers pass zero-terminated connection string
                // while others don't.
                if (len && !str[len - 1])
                    --len;

                ParseAttributeList(str, len, ';', connect_attributes);

                ArgumentMap::const_iterator it;

                it = connect_attributes.find(attrkey::dsn);
                if (it != connect_attributes.end())
                    dsn = it->second;
                else
                    dsn.clear();

                it = connect_attributes.find(attrkey::driver);
                if (it != connect_attributes.end())
                    driver = it->second;
                else
                    driver = dflt::driver;

                it = connect_attributes.find(attrkey::host);
                if (it != connect_attributes.end())
                    host = it->second;
                else
                    host = dflt::host;

                it = connect_attributes.find(attrkey::port);
                if (it != connect_attributes.end())
                    port = atoi(it->second.c_str());
                else
                    port = dflt::port;

                it = connect_attributes.find(attrkey::cache);
                if (it != connect_attributes.end())
                    cache = it->second;
                else
                    cache = dflt::cache;
            }

            void Configuration::FillFromConnectString(const std::string& str)
            {
                FillFromConnectString(str.data(), str.size());
            }

            std::string Configuration::ToConnectString() const
            {
                std::stringstream connect_string_buffer;

                if (!driver.empty())
                    connect_string_buffer << attrkey::driver << "={" << driver << "};";

                if (!host.empty())
                    connect_string_buffer << attrkey::host << '=' << host << ';';

                if (port)
                    connect_string_buffer << attrkey::port << '=' << port << ';';

                if (!dsn.empty())
                    connect_string_buffer << attrkey::dsn << '=' << dsn << ';';

                if (!cache.empty())
                    connect_string_buffer << attrkey::cache << '=' << cache << ';';

                return connect_string_buffer.str();
            }

            void Configuration::FillFromConfigAttributes(const char * attributes)
            {
                ArgumentMap config_attributes;

                size_t len = 0;

                // Getting list length. List is terminated by two '\0'.
                while (attributes[len] || attributes[len + 1])
                    ++len;

                ++len;

                ParseAttributeList(attributes, len, '\0', config_attributes);

                ArgumentMap::const_iterator it;

                it = config_attributes.find(attrkey::dsn);
                if (it != config_attributes.end())
                    dsn = it->second;
                else
                    dsn = dflt::dsn;

                it = config_attributes.find(attrkey::driver);
                if (it != config_attributes.end())
                    driver = it->second;
                else
                    driver.clear();

                it = config_attributes.find(attrkey::host);
                if (it != config_attributes.end())
                    host = it->second;
                else
                    host.clear();

                it = config_attributes.find(attrkey::port);
                if (it != config_attributes.end())
                    port = atoi(it->second.c_str());
                else
                    port = 0;

                it = config_attributes.find(attrkey::cache);
                if (it != config_attributes.end())
                    cache = it->second;
                else
                    cache.clear();
            }

            void Configuration::ParseAttributeList(const char * str, size_t len, char delimeter, ArgumentMap & args) const
            {
                std::string connect_str(str, len);
                args.clear();

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
        }
    }
}

