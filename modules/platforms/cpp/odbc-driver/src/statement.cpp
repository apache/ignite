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

#include "connection.h"

#include "utility.h"
#include "message.h"
#include "statement.h"

// Temporary solution
#define DEFAULT_PAGE_SIZE 32

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent), columnBindings(), resultQueryId(0), parser(), resultMeta()
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindResultColumn(uint16_t columnIdx, const ApplicationDataBuffer& buffer)
        {
            columnBindings[columnIdx] = buffer;
        }

        void Statement::PrepareSqlQuery(const char* query, size_t len)
        {
            sql.assign(query, len);
        }

        bool Statement::ExecuteSqlQuery(const char* query, size_t len)
        {
            using namespace ignite::impl::interop;

            PrepareSqlQuery(query, len);

            return ExecuteSqlQuery();
        }

        bool Statement::ExecuteSqlQuery()
        {
            const std::string& cacheName = connection.GetCache();

            if (sql.empty())
                return false;

            QueryExecuteRequest req(cacheName, sql);
            QueryExecuteResponse rsp;

            bool success = SyncMessage(req, rsp);

            if (!success)
                return false;

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                return false;
            }

            resultQueryId = rsp.GetQueryId();

            resultMeta.assign(rsp.GetMeta().begin(), rsp.GetMeta().end());

            LOG_MSG("Query id: %lld\n", resultQueryId);

            for (int i = 0; i < rsp.GetMeta().size(); ++i)
            {
                LOG_MSG("[%d] SchemaName:    %s\n", i, rsp.GetMeta()[i].GetSchemaName().c_str());
                LOG_MSG("[%d] TypeName:      %s\n", i, rsp.GetMeta()[i].GetTypeName().c_str());
                LOG_MSG("[%d] FieldName:     %s\n", i, rsp.GetMeta()[i].GetFieldName().c_str());
                LOG_MSG("[%d] FieldTypeName: %s\n", i, rsp.GetMeta()[i].GetFieldTypeName().c_str());
            }

            return true;
        }

        SqlResult Statement::FetchRow()
        {
            //
            return SQL_RESULT_NO_DATA;
        }
    }
}

