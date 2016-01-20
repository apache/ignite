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

namespace ignite
{
    namespace odbc
    {
        Environment::Environment()
        {
            // No-op.
        }

        Environment::~Environment()
        {
            // No-op.
        }

        Connection* Environment::CreateConnection()
        {
            Connection* connection;

            IGNITE_ODBC_API_CALL(InternalCreateConnection(connection));

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

        void Environment::TransactionCommit()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionCommit());
        }

        SqlResult Environment::InternalTransactionCommit()
        {
            return SQL_RESULT_SUCCESS;
        }

        void Environment::TransactionRollback()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionRollback());
        }

        SqlResult Environment::InternalTransactionRollback()
        {
            AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Rollback operation is not supported.");

            return SQL_RESULT_ERROR;
        }

        void Environment::SetAttribute(int32_t attr, void* value, int32_t len)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, len));
        }

        SqlResult Environment::InternalSetAttribute(int32_t attr, void* value, int32_t len)
        {
            return SQL_RESULT_SUCCESS;
        }

        void Environment::GetAttribute(int32_t attr, app::ApplicationDataBuffer& buffer)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buffer));
        }

        SqlResult Environment::InternalGetAttribute(int32_t attr, app::ApplicationDataBuffer& buffer)
        {
            return SQL_RESULT_SUCCESS;
        }
    }
}

