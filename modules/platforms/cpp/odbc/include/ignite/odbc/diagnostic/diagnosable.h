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

#ifndef _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE
#define _IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE

#include "ignite/odbc/diagnostic/diagnostic_record_storage.h"

namespace ignite
{
    namespace odbc
    {
        class OdbcError;

        namespace diagnostic
        {
            /**
             * Diagnosable interface.
             */
            class Diagnosable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Diagnosable()
                {
                    // No-op.
                }

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual const DiagnosticRecordStorage& GetDiagnosticRecords() const = 0;

                /**
                 * Get diagnostic record.
                 *
                 * @return Diagnostic record.
                 */
                virtual DiagnosticRecordStorage& GetDiagnosticRecords() = 0;

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 * @param rowNum Associated row number.
                 * @param columnNum Associated column number.
                 */
                virtual void AddStatusRecord(SqlState::Type sqlState, const std::string& message,
                    int32_t rowNum, int32_t columnNum) = 0;

                /**
                 * Add new status record.
                 *
                 * @param sqlState SQL state.
                 * @param message Message.
                 */
                virtual void AddStatusRecord(SqlState::Type sqlState, const std::string& message) = 0;

                /**
                 * Add new status record.
                 *
                 * @param err Error.
                 */
                virtual void AddStatusRecord(const OdbcError& err) = 0;

                /**
                 * Add new status record.
                 *
                 * @param rec Record.
                 */
                virtual void AddStatusRecord(const DiagnosticRecord& rec) = 0;

            protected:
                /**
                 * Default constructor.
                 */
                Diagnosable()
                {
                    // No-op.
                }
            };
        }
    }
}

#endif //_IGNITE_ODBC_DIAGNOSTIC_DIAGNOSABLE