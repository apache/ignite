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

#ifndef _IGNITE_ODBC_DRIVER_CONFIGURATION
#define _IGNITE_ODBC_DRIVER_CONFIGURATION

#include <stdint.h>
#include <string>
#include <map>

namespace ignite
{
    namespace odbc
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
             * Connection string constructor.
             * Constructs configuration taking values from the specified string.
             * Equivalent to default construction with following call to 
             * FillFromConnectString() method.
             *
             * @param str Pointer to string data.
             * @param len String length.
             */
            Configuration(const char* str, size_t len);

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
             * Convert configure to connect string.
             *
             * @return Connect string.
             */
            std::string ToConnectString() const;

            /**
             * Get server port.
             *
             * @return Server port.
             */
            uint16_t GetServetPort() const
            {
                return port;
            }

            /**
             * Get server host.
             *
             * @return Server host.
             */
            const std::string& GetServetHost() const
            {
                return host;
            }

        private:
            /** Map containing connect arguments. */
            typedef std::map<std::string, std::string> ArgumentMap;

            /**
             * Parse connect string into key-value storage.
             *
             * @param str String to parse.
             * @param len String length.
             * @param params Parsing result.
             */
            void ParseConnectString(const char* str, size_t len, ArgumentMap& args) const;

            /** Server hostname. */
            std::string host;

            /** Port of the server. */
            uint16_t port;
        };
    }
}

#endif