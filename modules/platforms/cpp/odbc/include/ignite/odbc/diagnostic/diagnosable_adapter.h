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

#ifndef _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER
#define _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER

#include "ignite/odbc/diagnostic/diagnosable.h"

#define IGNITE_ODBC_API_CALL(...)                   \
        diagnosticRecords.Reset();                  \
        SqlResult::Type result = (__VA_ARGS__);     \
        diagnosticRecords.SetHeaderRecord(result)

#define IGNITE_ODBC_API_CALL_ALWAYS_SUCCESS                     \
        diagnosticRecords.Reset();                              \
        diagnosticRecords.SetHeaderRecord(SqlResult::AI_SUCCESS)

namespace ignite
{
    namespace odbc
    {
        class OdbcError;
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
                 * Constructor.
                 *
                 * @param connection Pointer to connection. Used to create
                 *     diagnostic records with connection info.
                 */
                DiagnosableAdapter(const Connection* connection = 0) :
                    connection(connection)
                {
                    // No-op.
                }

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
                virtual const DiagnosticRecordStorage& GetDiagnosticRecords() const
                {
                    return diagnosticRecords;
                }

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual DiagnosticRecordStorage& GetDiagnosticRecords()
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
                virtual void AddStatusRecord(SqlState::Type  sqlState, const std::string& message,
                    int32_t rowNum, int32_t columnNum);

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 */
                virtual void AddStatusRecord(SqlState::Type  sqlState, const std::string& message);

                /**
                 * Add new status record.
                 *
                 * @param err Error.
                 */
                virtual void AddStatusRecord(const OdbcError& err);

                /**
                 * Add new status record.
                 *
                 * @param rec Record.
                 */
                virtual void AddStatusRecord(const DiagnosticRecord& rec);

            protected:
                /** Diagnostic records. */
                DiagnosticRecordStorage diagnosticRecords;

            private:
                /** Connection. */
                const Connection* connection;
            };
        }
    }
}

#endif //_IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE_ADAPTER