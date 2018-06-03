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

#ifndef _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD
#define _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD

#include <stdint.h>

#include <vector>

#include <ignite/common/common.h>
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/app/application_data_buffer.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            /**
             * Status diagnostic record.
             */
            class DiagnosticRecord
            {
            public:
                /**
                 * Default constructor.
                 */
                DiagnosticRecord();

                /**
                 * Constructor.
                 *
                 * @param sqlState SQL state code.
                 * @param message Message.
                 * @param connectionName Connection name.
                 * @param serverName Server name.
                 * @param rowNum Associated row number.
                 * @param columnNum Associated column number.
                 */
                DiagnosticRecord(SqlState::Type sqlState, const std::string& message,
                    const std::string& connectionName, const std::string& serverName,
                    int32_t rowNum = 0, int32_t columnNum = 0);

                /**
                 * Destructor.
                 */
                ~DiagnosticRecord();

                /**
                 * Get class origin.
                 *
                 * @return A string that indicates the document that defines the
                 *         class portion of the SQLSTATE value in this record.
                 */
                const std::string& GetClassOrigin() const;

                /**
                 * Get subclass origin.
                 *
                 * @return A string with the same format and valid values as origin,
                 *         that identifies the defining portion of the subclass
                 *         portion of the SQLSTATE code.
                 */
                const std::string& GetSubclassOrigin() const;

                /**
                 * Get record message text.
                 *
                 * @return An informational message on the error or warning.
                 */
                const std::string& GetMessageText() const;

                /**
                 * Get connection name.
                 *
                 * @return A string that indicates the name of the connection that
                 *         the diagnostic record relates to.
                 */
                const std::string& GetConnectionName() const;

                /**
                 * Get server name.
                 *
                 * @return A string that indicates the server name that the
                 *         diagnostic record relates to.
                 */
                const std::string& GetServerName() const;

                /**
                 * Get SQL state of the record.
                 *
                 * @return A five-character SQLSTATE diagnostic code.
                 */
                const std::string& GetSqlState() const;

                /**
                 * Get row number.
                 *
                 * @return The row number in the rowset, or the parameter number in
                 *         the set of parameters, with which the status record is
                 *         associated.
                 */
                int32_t GetRowNumber() const;

                /**
                 * Get column number.
                 *
                 * @return Contains the value that represents the column number
                 *         in the result set or the parameter number in the set
                 *         of parameters.
                 */
                int32_t GetColumnNumber() const;

                /**
                 * Check if the record was retrieved with the SQLError previously.
                 *
                 * return True if the record was retrieved with the SQLError
                 *  previously.
                 */
                bool IsRetrieved() const;

                /**
                 * Mark record as retrieved with the SQLError.
                 */
                void MarkRetrieved();

            private:
                /** SQL state diagnostic code. */
                SqlState::Type sqlState;

                /** An informational message on the error or warning. */
                std::string message;

                /**
                 * A string that indicates the name of the connection that
                 * the diagnostic record relates to.
                 */
                std::string connectionName;

                /**
                 * A string that indicates the server name that the
                 * diagnostic record relates to.
                 */
                std::string serverName;

                /**
                 * The row number in the rowset, or the parameter number in the
                 * set of parameters, with which the status record is associated.
                 */
                int32_t rowNum;

                /**
                 * Contains the value that represents the column number in the
                 * result set or the parameter number in the set of parameters.
                 */
                int32_t columnNum;

                /**
                 * Flag that shows if the record was retrieved with the 
                 * SQLError previously.
                 */
                bool retrieved;
            };
        }
    }
}

#endif //_IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD