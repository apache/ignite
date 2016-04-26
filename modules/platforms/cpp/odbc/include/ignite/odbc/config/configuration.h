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
                    return port;
                }

                /**
                 * Get DSN.
                 *
                 * @return Data Source Name.
                 */
                const std::string& GetDsn() const
                {
                    return dsn;
                }

                /**
                 * Get Driver.
                 *
                 * @return Driver name.
                 */
                const std::string& GetDriver() const
                {
                    return driver;
                }

                /**
                 * Get server host.
                 *
                 * @return Server host.
                 */
                const std::string& GetHost() const
                {
                    return host;
                }

                /**
                 * Get cache.
                 *
                 * @return Cache name.
                 */
                const std::string& GetCache() const
                {
                    return cache;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(Configuration);

                /** Map containing connect arguments. */
                typedef std::map<std::string, std::string> ArgumentMap;

                /**
                 * Parse connect string into key-value storage.
                 *
                 * @param str String to parse.
                 * @param len String length.
                 * @param params Parsing result.
                 */
                void ParseAttributeList(const char* str, size_t len, char delimeter, ArgumentMap& args) const;

                /** Data Source Name. */
                std::string dsn;

                /** Driver name. */
                std::string driver;

                /** Server hostname. */
                std::string host;

                /** Port of the server. */
                uint16_t port;

                /** Cache name. */
                std::string cache;
            };
        }

    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIGURATION