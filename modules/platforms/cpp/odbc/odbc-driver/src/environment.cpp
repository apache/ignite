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

#include "ignite/odbc/connection.h"
#include "ignite/odbc/environment.h"

#define IGNITE_ODBC_ENVIRONMENT_API_CALL(x) IGNITE_ODBC_API_CALL(diagnosticRecords, (x))

namespace ignite
{
    namespace odbc
    {
        Environment::Environment()
        {
            // HACK: move it to library-wide initialisation.
            InitNetworking();
            // No-op.
        }

        Environment::~Environment()
        {
            // No-op.
        }

        void Environment::AddStatusRecord(SqlState sqlState, const std::string & message)
        {
            diagnosticRecords.AddStatusRecord(diagnostic::DiagnosticRecord(sqlState, message, "", ""));
        }

        Connection* Environment::CreateConnection()
        {
            Connection* connection;

            IGNITE_ODBC_ENVIRONMENT_API_CALL(InternalCreateConnection(connection));

            return connection;
        }


        SqlResult Environment::InternalCreateConnection(Connection*& connection)
        {
            connection = new Connection;

            if (!connection)
            {
                AddStatusRecord(SQL_STATE_HY001_MEMORY_ALLOCATION, "Not enough memory.");

                return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
        }

        const diagnostic::DiagnosticRecordStorage & Environment::GetDiagnosticRecords() const
        {
            return diagnosticRecords;
        }
    }
}

