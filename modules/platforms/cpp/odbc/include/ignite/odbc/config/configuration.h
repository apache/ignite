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

#ifndef _IGNITE_ODBC_CONFIG_CONFIGURATION
#define _IGNITE_ODBC_CONFIG_CONFIGURATION

#include <stdint.h>
#include <string>
#include <map>

#include <ignite/common/common.h>
#include <ignite/common/utils.h>

namespace ignite
{
    namespace odbc
    {
        namespace config
        {
            /**
             * ODBC configuration abstraction.
             */
            class Configuration
            {
            public:
                /** Map containing connect arguments. */
                typedef std::map<std::string, std::string> ArgumentMap;

                /** Connection attribute keywords. */
                struct Key
                {
                    /** Connection attribute keyword for DSN attribute. */
                    static const std::string dsn;

                    /** Connection attribute keyword for Driver attribute. */
                    static const std::string driver;

                    /** Connection attribute keyword for cache attribute. */
                    static const std::string cache;

                    /** Connection attribute keyword for address attribute. */
                    static const std::string address;

                    /** Connection attribute keyword for server attribute. */
                    static const std::string server;

                    /** Connection attribute keyword for port attribute. */
                    static const std::string port;
                };

                /** Default values for configuration. */
                struct DefaultValue
                {
                    /** Default value for DSN attribute. */
                    static const std::string dsn;

                    /** Default value for Driver attribute. */
                    static const std::string driver;

                    /** Default value for cache attribute. */
                    static const std::string cache;

                    /** Default value for address attribute. */
                    static const std::string address;

                    /** Default value for server attribute. */
                    static const std::string server;

                    /** Default value for port attribute. */
                    static const std::string port;

                    /** Default value for port attribute. Uint16 value. */
                    static const uint16_t uintPort;
                };

                /**
                 * Connection end point structure.
                 */
                struct EndPoint
                {
                    /** Remote host. */
                    std::string host;

                    /** TCP port. */
                    uint16_t port;
                };

                /**
                 * Default constructor.
                 */
                Configuration();

                /**
                 * Destructor.
                 */
                ~Configuration();

                /**
                 * Fill configuration data using connection string.
                 *
                 * @param str Pointer to string data.
                 * @param len String length.
                 */
                void FillFromConnectString(const char* str, size_t len);
                
                /**
                 * Fill configuration data using connection string.
                 *
                 * @param str Connect string.
                 */
                void FillFromConnectString(const std::string& str);

                /**
                 * Convert configure to connect string.
                 *
                 * @return Connect string.
                 */
                std::string ToConnectString() const;

                /**
                 * Fill configuration data using config attributes string.
                 *
                 * @param str Pointer to list of zero-terminated strings.
                 *            Terminated by two zero bytes.
                 */
                void FillFromConfigAttributes(const char* attributes);

                /**
                 * Get server port.
                 *
                 * @return Server port.
                 */
                uint16_t GetPort() const
                {
                    return endPoint.port;
                }

                /**
                 * Get DSN.
                 *
                 * @return Data Source Name.
                 */
                const std::string& GetDsn() const
                {
                    return GetStringValue(Key::dsn, DefaultValue::dsn);
                }

                /**
                 * Get Driver.
                 *
                 * @return Driver name.
                 */
                const std::string& GetDriver() const
                {
                    return GetStringValue(Key::driver, DefaultValue::driver);
                }

                /**
                 * Get server host.
                 *
                 * @return Server host.
                 */
                const std::string& GetHost() const
                {
                    return endPoint.host;
                }

                /**
                 * Get cache.
                 *
                 * @return Cache name.
                 */
                const std::string& GetCache() const
                {
                    return GetStringValue(Key::cache, DefaultValue::cache);
                }

                /**
                 * Get address.
                 *
                 * @return Address.
                 */
                const std::string& GetAddress() const
                {
                    return GetStringValue(Key::address, DefaultValue::address);
                }

                /**
                 * Get string value from the config.
                 *
                 * @param key Configuration key.
                 * @param dflt Default value to be returned if there is no value stored.
                 * @return Found or default value.
                 */
                const std::string& GetStringValue(const std::string& key, const std::string& dflt) const;

            private:
                /**
                 * Parse connect string into key-value storage.
                 *
                 * @param str String to parse.
                 * @param len String length.
                 * @param params Parsing result.
                 */
                static void ParseAttributeList(const char* str, size_t len, char delimeter, ArgumentMap& args);

                /**
                 * Parse address and extract connection end-point.
                 *
                 * @throw IgniteException if address can not be parsed.
                 * @param address Address string to parse.
                 * @param res Result is placed here.
                 */
                static void ParseAddress(const std::string& address, EndPoint& res);

                /** Arguments. */
                ArgumentMap arguments;

                /** Connection end-point. */
                EndPoint endPoint;
            };
        }

    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIGURATION