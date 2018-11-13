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

#ifndef _IGNITE_ODBC_CONFIG_CONFIG_TOOLS
#define _IGNITE_ODBC_CONFIG_CONFIG_TOOLS

#include <string>
#include <vector>

#include "ignite/odbc/end_point.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            // Forward declaration.
            class DiagnosticRecordStorage;
        }

        namespace config
        {
            /**
             * Convert address list to string.
             *
             * @param addresses Addresses.
             * @return Resulting string.
             */
            std::string AddressesToString(const std::vector<EndPoint>& addresses);

            /**
             * Parse address.
             *
             * @param value String value to parse.
             * @param endPoints End ponts list.
             * @param diag Diagnostics collector.
             */
            void ParseAddress(const std::string& value, std::vector<EndPoint>& endPoints,
                diagnostic::DiagnosticRecordStorage* diag);

            /**
             * Parse single address.
             *
             * @param value String value to parse.
             * @param endPoint End pont.
             * @param diag Diagnostics collector.
             * @return @c true, if parsed successfully, and @c false otherwise.
             */
            bool ParseSingleAddress(const std::string& value, EndPoint& endPoint,
                diagnostic::DiagnosticRecordStorage* diag);

            /**
             * Parse single network port.
             *
             * @param value String value to parse.
             * @param port Port range begin.
             * @param range Number of ports in range.
             * @param diag Diagnostics collector.
             * @return @c Port value on success and zero on failure.
             */
            bool ParsePortRange(const std::string& value, uint16_t& port, uint16_t& range,
                diagnostic::DiagnosticRecordStorage* diag);

            /**
             * Parse single network port.
             *
             * @param value String value to parse.
             * @param diag Diagnostics collector.
             * @return @c Port value on success and zero on failure.
             */
            uint16_t ParsePort(const std::string& value, diagnostic::DiagnosticRecordStorage* diag);
        }
    }
}

#endif //_IGNITE_ODBC_CONFIG_CONFIG_TOOLS