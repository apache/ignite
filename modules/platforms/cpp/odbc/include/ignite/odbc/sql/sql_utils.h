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

#ifndef _IGNITE_ODBC_SQL_SQL_UTILS
#define _IGNITE_ODBC_SQL_SQL_UTILS

#include <string>

#include <ignite/odbc/odbc_error.h>
#include <ignite/odbc/sql/sql_token.h>

namespace ignite
{
    namespace odbc
    {
        namespace sql_utils
        {
            /**
             * Parse token to boolean value.
             *
             * @return Boolean value.
             */
            inline OdbcExpected<bool> TokenToBoolean(const SqlToken& token)
            {
                std::string lower = token.ToLower();

                if (lower == "1" || lower == "on")
                    return true;

                if (lower == "0" || lower == "off")
                    return false;

                return OdbcError(SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,
                    "Unexpected token: '" + token.ToString() + "', ON, OFF, 1 or 0 expected.");
            }

            /**
             * Check if the SQL is internal command.
             *
             * @param sql SQL request string.
             * @return @c true if internal.
             */
            bool IsInternalCommand(const std::string& sql);
        }
    }
}

#endif //_IGNITE_ODBC_SQL_SQL_UTILS