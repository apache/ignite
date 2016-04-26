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

#ifndef _IGNITE_ODBC_DRIVER_DIAGNOSABLE_ADAPTER
#define _IGNITE_ODBC_DRIVER_DIAGNOSABLE_ADAPTER

#include "ignite/odbc/diagnostic/diagnosable.h"

#define IGNITE_ODBC_API_CALL(...)                   \
        diagnosticRecords.Reset();                  \
        SqlResult result = (__VA_ARGS__);           \
        diagnosticRecords.SetHeaderRecord(result)

#define IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS                     \
        diagnosticRecords.Reset();                              \
        diagnosticRecords.SetHeaderRecord(SQL_RESULT_SUCCESS)

namespace ignite
{
    namespace odbc
    {
        class Connection;

        namespace diagnostic
        {
            /**
             * Diagnosable interface.
             */
            class DiagnosableAdapter : public Diagnosable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~DiagnosableAdapter()
                {
                    // No-op.
                }

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual const diagnostic::DiagnosticRecordStorage& GetDiagnosticRecords() const
                {
                    return diagnosticRecords;
                }

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 * @param rowNum Associated row number.
                 * @param columnNum Associated column number.
                 */
                virtual void AddStatusRecord(SqlState sqlState, const std::string& message,
                    int32_t rowNum, int32_t columnNum);

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 */
                virtual void AddStatusRecord(SqlState sqlState, const std::string& message);

            protected:
                /**
                 * Constructor.
                 *
                 * @param connection Pointer to connection. Used to create
                 *                   diagnostic records with connection info.
                 */
                DiagnosableAdapter(const Connection* connection = 0) :
                    connection(connection)
                {
                    // No-op.
                }

                /** Diagnostic records. */
                diagnostic::DiagnosticRecordStorage diagnosticRecords;

            private:
                /** Connection. */
                const Connection* connection;
            };
        }
    }
}

#endif