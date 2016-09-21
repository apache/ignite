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
                result(SQL_RESULT_SUCCESS),
                rowsAffected(0)
            {
                // No-op.
            }

            DiagnosticRecordStorage::~DiagnosticRecordStorage()
            {
                // No-op.
            }

            void DiagnosticRecordStorage::SetHeaderRecord(SqlResult result)
            {
                rowCount = 0;
                dynamicFunction.clear();
                dynamicFunctionCode = 0;
                this->result = result;
                rowsAffected = 0;
            }

            void DiagnosticRecordStorage::AddStatusRecord(const DiagnosticRecord& record)
            {
                statusRecords.push_back(record);
            }

            void DiagnosticRecordStorage::Reset()
            {
                SetHeaderRecord(SQL_RESULT_ERROR);

                statusRecords.clear();
            }

            SqlResult DiagnosticRecordStorage::GetOperaionResult() const
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

            bool DiagnosticRecordStorage::IsSuccessful() const
            {
                return result == SQL_RESULT_SUCCESS || 
                       result == SQL_RESULT_SUCCESS_WITH_INFO;
            }

            SqlResult DiagnosticRecordStorage::GetField(int32_t recNum, DiagnosticField field, app::ApplicationDataBuffer& buffer) const
            {
                // Header record.
                switch (field)
                {
                    case IGNITE_SQL_DIAG_HEADER_CURSOR_ROW_COUNT:
                    {
                        buffer.PutInt64(GetRowCount());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION:
                    {
                        buffer.PutString(GetDynamicFunction());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION_CODE:
                    {
                        buffer.PutInt32(GetDynamicFunctionCode());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_HEADER_NUMBER:
                    {
                        buffer.PutInt32(GetStatusRecordsNumber());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_HEADER_RETURNCODE:
                    {
                        buffer.PutInt32(GetReturnCode());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_HEADER_ROW_COUNT:
                    {
                        buffer.PutInt64(GetRowsAffected());

                        return SQL_RESULT_SUCCESS;
                    }

                    default:
                        break;
                }

                if (recNum < 1 || static_cast<size_t>(recNum) > statusRecords.size())
                    return SQL_RESULT_NO_DATA;

                // Status record.
                const DiagnosticRecord& record = GetStatusRecord(recNum);

                switch (field)
                {
                    case IGNITE_SQL_DIAG_STATUS_CLASS_ORIGIN:
                    {
                        buffer.PutString(record.GetClassOrigin());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_COLUMN_NUMBER:
                    {
                        buffer.PutInt32(record.GetColumnNumber());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_CONNECTION_NAME:
                    {
                        buffer.PutString(record.GetConnectionName());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_MESSAGE_TEXT:
                    {
                        buffer.PutString(record.GetMessageText());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_NATIVE:
                    {
                        buffer.PutInt32(0);

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_ROW_NUMBER:
                    {
                        buffer.PutInt64(record.GetRowNumber());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_SERVER_NAME:
                    {
                        buffer.PutString(record.GetServerName());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_SQLSTATE:
                    {
                        buffer.PutString(record.GetSqlState());

                        return SQL_RESULT_SUCCESS;
                    }

                    case IGNITE_SQL_DIAG_STATUS_SUBCLASS_ORIGIN:
                    {
                        buffer.PutString(record.GetSubclassOrigin());

                        return SQL_RESULT_SUCCESS;
                    }

                    default:
                        break;
                }

                return SQL_RESULT_ERROR;
            }

        }
    }
}
