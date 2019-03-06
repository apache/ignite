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

#include <set>
#include <string>

#include "ignite/odbc/diagnostic/diagnostic_record_storage.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            DiagnosticRecordStorage::DiagnosticRecordStorage() :
                rowCount(0),
                dynamicFunction(),
                dynamicFunctionCode(0),
                result(SqlResult::AI_SUCCESS),
                rowsAffected(0)
            {
                // No-op.
            }

            DiagnosticRecordStorage::~DiagnosticRecordStorage()
            {
                // No-op.
            }

            void DiagnosticRecordStorage::SetHeaderRecord(SqlResult::Type result)
            {
                rowCount = 0;
                dynamicFunction.clear();
                dynamicFunctionCode = 0;
                this->result = result;
                rowsAffected = 0;
            }

            void DiagnosticRecordStorage::AddStatusRecord(SqlState::Type sqlState, const std::string& message)
            {
                statusRecords.push_back(DiagnosticRecord(sqlState, message, "", "", 0, 0));
            }

            void DiagnosticRecordStorage::AddStatusRecord(const DiagnosticRecord& record)
            {
                statusRecords.push_back(record);
            }

            void DiagnosticRecordStorage::Reset()
            {
                SetHeaderRecord(SqlResult::AI_ERROR);

                statusRecords.clear();
            }

            SqlResult::Type DiagnosticRecordStorage::GetOperaionResult() const
            {
                return result;
            }

            int DiagnosticRecordStorage::GetReturnCode() const
            {
                return SqlResultToReturnCode(result);
            }

            int64_t DiagnosticRecordStorage::GetRowCount() const
            {
                return rowCount;
            }

            const std::string & DiagnosticRecordStorage::GetDynamicFunction() const
            {
                return dynamicFunction;
            }

            int32_t DiagnosticRecordStorage::GetDynamicFunctionCode() const
            {
                return dynamicFunctionCode;
            }

            int32_t DiagnosticRecordStorage::GetRowsAffected() const
            {
                return rowsAffected;
            }

            int32_t DiagnosticRecordStorage::GetStatusRecordsNumber() const
            {
                return static_cast<int32_t>(statusRecords.size());
            }

            const DiagnosticRecord& DiagnosticRecordStorage::GetStatusRecord(int32_t idx) const
            {
                return statusRecords[idx - 1];
            }

            DiagnosticRecord& DiagnosticRecordStorage::GetStatusRecord(int32_t idx)
            {
                return statusRecords[idx - 1];
            }

            int32_t DiagnosticRecordStorage::GetLastNonRetrieved() const
            {
                for (size_t i = 0; i < statusRecords.size(); ++i)
                {
                    const DiagnosticRecord& record = statusRecords[i];

                    if (!record.IsRetrieved())
                        return static_cast<int32_t>(i + 1);
                }

                return 0;
            }

            bool DiagnosticRecordStorage::IsSuccessful() const
            {
                return result == SqlResult::AI_SUCCESS || 
                       result == SqlResult::AI_SUCCESS_WITH_INFO;
            }

            SqlResult::Type DiagnosticRecordStorage::GetField(int32_t recNum, DiagnosticField::Type field, app::ApplicationDataBuffer& buffer) const
            {
                // Header record.
                switch (field)
                {
                    case DiagnosticField::HEADER_CURSOR_ROW_COUNT:
                    {
                        buffer.PutInt64(GetRowCount());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::HEADER_DYNAMIC_FUNCTION:
                    {
                        buffer.PutString(GetDynamicFunction());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::HEADER_DYNAMIC_FUNCTION_CODE:
                    {
                        buffer.PutInt32(GetDynamicFunctionCode());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::HEADER_NUMBER:
                    {
                        buffer.PutInt32(GetStatusRecordsNumber());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::HEADER_RETURNCODE:
                    {
                        buffer.PutInt32(GetReturnCode());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::HEADER_ROW_COUNT:
                    {
                        buffer.PutInt64(GetRowsAffected());

                        return SqlResult::AI_SUCCESS;
                    }

                    default:
                        break;
                }

                if (recNum < 1 || static_cast<size_t>(recNum) > statusRecords.size())
                    return SqlResult::AI_NO_DATA;

                // Status record.
                const DiagnosticRecord& record = GetStatusRecord(recNum);

                switch (field)
                {
                    case DiagnosticField::STATUS_CLASS_ORIGIN:
                    {
                        buffer.PutString(record.GetClassOrigin());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_COLUMN_NUMBER:
                    {
                        buffer.PutInt32(record.GetColumnNumber());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_CONNECTION_NAME:
                    {
                        buffer.PutString(record.GetConnectionName());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_MESSAGE_TEXT:
                    {
                        buffer.PutString(record.GetMessageText());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_NATIVE:
                    {
                        buffer.PutInt32(0);

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_ROW_NUMBER:
                    {
                        buffer.PutInt64(record.GetRowNumber());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_SERVER_NAME:
                    {
                        buffer.PutString(record.GetServerName());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_SQLSTATE:
                    {
                        buffer.PutString(record.GetSqlState());

                        return SqlResult::AI_SUCCESS;
                    }

                    case DiagnosticField::STATUS_SUBCLASS_ORIGIN:
                    {
                        buffer.PutString(record.GetSubclassOrigin());

                        return SqlResult::AI_SUCCESS;
                    }

                    default:
                        break;
                }

                return SqlResult::AI_ERROR;
            }

        }
    }
}
