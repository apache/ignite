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

#ifndef _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD_STORAGE
#define _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD_STORAGE

#include <stdint.h>

#include <vector>

#include <ignite/common/common.h>
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/diagnostic/diagnostic_record.h"

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            /**
             * Diagnostic record.
             *
             * Associated with each environment, connection, statement, and
             * descriptor handle are diagnostic records. These records contain
             * diagnostic information about the last function called that used
             * a particular handle. The records are replaced only when another
             * function is called using that handle. There is no limit to the
             * number of diagnostic records that can be stored at any one time.
             *
             * This class provides interface for interaction with all handle
             * diagnostic records. That means both header and status records.
             */
            class DiagnosticRecordStorage
            {
            public:
                /**
                 * Default constructor.
                 */
                DiagnosticRecordStorage();

                /**
                 * Destructor.
                 */
                ~DiagnosticRecordStorage();

                /**
                 * Set header record values.
                 *
                 * @param result Operation return code.
                 */
                void SetHeaderRecord(SqlResult::Type result);

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 */
                void AddStatusRecord(SqlState::Type  sqlState, const std::string& message);

                /**
                 * Add status record to diagnostic records.
                 *
                 * @param record Status record.
                 */
                void AddStatusRecord(const DiagnosticRecord& record);

                /**
                 * Reset diagnostic records state.
                 */
                void Reset();

                /**
                 * Get result of the last operation.
                 *
                 * @return Result of the last operation.
                 */
                SqlResult::Type GetOperaionResult() const;

                /**
                 * Get return code of the last operation.
                 *
                 * @return Return code of the last operation.
                 */
                int GetReturnCode() const;

                /**
                 * Get row count.
                 *
                 * @return Count of rows in cursor.
                 */
                int64_t GetRowCount() const;

                /**
                 * Get dynamic function.
                 *
                 * @return String that describes the SQL statement
                 *         that the underlying function executed.
                 */
                const std::string& GetDynamicFunction() const;

                /**
                 * Get dynamic function code.
                 *
                 * @return Numeric code that describes the
                 *         SQL statement that was executed.
                 */
                int32_t GetDynamicFunctionCode() const;

                /**
                 * Get number of rows affected.
                 *
                 * @return The number of rows affected by an insert,
                 *         delete, or update performed by the last operation.
                 */
                int32_t GetRowsAffected() const;

                /**
                 * Get status records number.
                 *
                 * @return Number of status records.
                 */
                int32_t GetStatusRecordsNumber() const;

                /**
                 * Get specified status record.
                 *
                 * @param idx Status record index.
                 * @return Status record instance reference.
                 */
                const DiagnosticRecord& GetStatusRecord(int32_t idx) const;

                /**
                 * Get specified status record.
                 *
                 * @param idx Status record index.
                 * @return Status record instance reference.
                 */
                DiagnosticRecord& GetStatusRecord(int32_t idx);

                /**
                 * Get last non-retrieved status record index.
                 *
                 * @return Index of the last non-retrieved status record or zero
                 *  if nothing was found.
                 */
                int32_t GetLastNonRetrieved() const;

                /**
                 * Check if the record is in the success state.
                 *
                 * @return True if the record is in the success state.
                 */
                bool IsSuccessful() const;

                /**
                 * Get value of the field and put it in buffer.
                 *
                 * @param recNum Diagnostic record number.
                 * @param field Record field.
                 * @param buffer Buffer to put data to.
                 * @return Operation result.
                 */
                SqlResult::Type GetField(int32_t recNum, DiagnosticField::Type field,
                    app::ApplicationDataBuffer& buffer) const;

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DiagnosticRecordStorage);

                /**
                 * Header record field. This field contains the count of rows
                 * in the cursor.
                 */
                int64_t rowCount;

                /**
                 * Header record field. String that describes the SQL statement
                 * that the underlying function executed.
                 */
                std::string dynamicFunction;

                /**
                 * Header record field. Numeric code that describes the
                 * SQL statement that was executed.
                 */
                int32_t dynamicFunctionCode;

                /**
                 * Operation result. This field is mapped to "Return code" header
                 * record field.
                 */
                SqlResult::Type result;

                /**
                 * Header record field. The number of rows affected by an insert,
                 * delete, or update performed by the last operation.
                 */
                int32_t rowsAffected;

                /** Status records. */
                std::vector<DiagnosticRecord> statusRecords;
            };
        }
    }
}

#endif //_IGNITE_ODBC_DIAGNOSTIC_DIAGNOSTIC_RECORD_STORAGE