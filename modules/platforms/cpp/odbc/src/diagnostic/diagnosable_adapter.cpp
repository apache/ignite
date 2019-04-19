/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ignite/odbc/log.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            void DiagnosableAdapter::AddStatusRecord(SqlState::Type  sqlState,
                const std::string& message, int32_t rowNum, int32_t columnNum)
            {
                LOG_MSG("Adding new record: " << message << ", rowNum: " << rowNum << ", columnNum: " << columnNum);

                if (connection)
                {
                    diagnosticRecords.AddStatusRecord(
                        connection->CreateStatusRecord(sqlState, message, rowNum, columnNum));
                }
                else
                {
                    diagnosticRecords.AddStatusRecord(
                        DiagnosticRecord(sqlState, message, "", "", rowNum, columnNum));
                }
            }

            void DiagnosableAdapter::AddStatusRecord(SqlState::Type  sqlState, const std::string& message)
            {
                AddStatusRecord(sqlState, message, 0, 0);
            }

            void DiagnosableAdapter::AddStatusRecord(const OdbcError& err)
            {
                AddStatusRecord(err.GetStatus(), err.GetErrorMessage(), 0, 0);
            }

            void DiagnosableAdapter::AddStatusRecord(const DiagnosticRecord& rec)
            {
                diagnosticRecords.AddStatusRecord(rec);
            }
        }
    }
}

