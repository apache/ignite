/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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