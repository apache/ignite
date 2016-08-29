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

#ifndef _IGNITE_ODBC_PROTOCOL_VERSION
#define _IGNITE_ODBC_PROTOCOL_VERSION

#include <stdint.h>

#include <string>
#include <map>

namespace ignite
{
    namespace odbc
    {
        /** Protocol version. */
        class ProtocolVersion
        {
        public:
            /** String to version map type alias. */
            typedef std::map<std::string, ProtocolVersion> StringToVersionMap;

            /** Version to string map type alias. */
            typedef std::map<ProtocolVersion, std::string> VersionToStringMap;

            /** First version of the protocol that was introduced in Ignite 1.6.0. */
            static const ProtocolVersion VERSION_1_6_0;

            /** First version of the protocol that was introduced in Ignite 1.8.0. */
            static const ProtocolVersion VERSION_1_8_0;

            /** Unknown version of the protocol. */
            static const ProtocolVersion VERSION_UNKNOWN;

            /**
             * Get string to version map.
             *
             * @return String to version map.
             */
            static const StringToVersionMap& GetMap();

            /**
             * Get current version.
             *
             * @return Current version.
             */
            static const ProtocolVersion& GetCurrent();

            /**
             * Parse string and extract protocol version.
             *
             * @throw IgniteException if version can not be parsed.
             * @param version Version string to parse.
             * @return Protocol version.
             */
            static ProtocolVersion FromString(const std::string& version);

            /**
             * Convert to string value.
             *
             * @throw IgniteException if version is unknow parsed.
             * @param version Version string to parse.
             * @return Protocol version.
             */
            const std::string& ToString() const;

            /**
             * Get int value.
             *
             * @return Integer value.
             */
            int64_t GetIntValue() const;

            /**
             * Check if the version is unknown.
             *
             * @return True if the version is unknown.
             */
            bool IsUnknown() const;

            /**
             * Check if the distributed joins supported.
             *
             * @retuen True if the distributed joins supported.
             */
            bool IsDistributedJoinsSupported() const;

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if equal.
             */
            friend bool operator==(const ProtocolVersion& val1, const ProtocolVersion& val2);

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if not equal.
             */
            friend bool operator!=(const ProtocolVersion& val1, const ProtocolVersion& val2);

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if less.
             */
            friend bool operator<(const ProtocolVersion& val1, const ProtocolVersion& val2);

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if less or equal.
             */
            friend bool operator<=(const ProtocolVersion& val1, const ProtocolVersion& val2);

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if gretter.
             */
            friend bool operator>(const ProtocolVersion& val1, const ProtocolVersion& val2);

            /**
             * Comparison operator.
             *
             * @param val1 First value.
             * @param val2 Second value.
             * @return True if gretter or equal.
             */
            friend bool operator>=(const ProtocolVersion& val1, const ProtocolVersion& val2);

        private:
            /**
             * Constructor.
             *
             * @param val Underlying value.
             */
            explicit ProtocolVersion(int64_t val);
            
            /**
             * Make int value for the version.
             *
             * @param major Major version.
             * @param minor Minor version.
             * @param revision Revision.
             * @return Int value for the version.
             */
            static int64_t MakeVersion(uint16_t major, uint16_t minor, uint16_t revision);

            ProtocolVersion();

            /** String to version map. */
            static const StringToVersionMap stringToVersionMap;

            /** Version to string map. */
            static const VersionToStringMap versionToStringMap;

            /** Underlying int value. */
            int64_t val;
        };
    }
}

#endif //_IGNITE_ODBC_PROTOCOL_VERSION