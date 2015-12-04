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

#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/statement.h"

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent), columnBindings(), currentQuery()
        {
            // No-op.
        }

        Statement::~Statement()
        {
            // No-op.
        }

        void Statement::BindColumn(uint16_t columnIdx, const ApplicationDataBuffer& buffer)
        {
            columnBindings[columnIdx] = buffer;
        }

        void Statement::UnbindColumn(uint16_t columnIdx)
        {
            columnBindings.erase(columnIdx);
        }

        void Statement::UnbindAllColumns()
        {
            columnBindings.clear();
        }

        void Statement::PrepareSqlQuery(const char* query, size_t len)
        {
            if (currentQuery.get())
                currentQuery->Close();

            std::string sql(query, len);

            currentQuery.reset(new query::DataQuery(connection, sql));
        }

        bool Statement::ExecuteSqlQuery(const char* query, size_t len)
        {
            using namespace ignite::impl::interop;

            PrepareSqlQuery(query, len);

            return ExecuteSqlQuery();
        }

        bool Statement::ExecuteSqlQuery()
        {
            if (!currentQuery.get())
                return false;

            return currentQuery->Execute();
        }

        bool Statement::ExecuteGetColumnsMetaQuery(const std::string& cache, const std::string& table, const std::string& column)
        {
            // TODO: implement me.
            return false;
        }

        bool Statement::Close()
        {
            if (!currentQuery.get())
                return false;

            currentQuery->Close();

            currentQuery.reset();

            return true;
        }

        SqlResult Statement::FetchRow()
        {
            if (!currentQuery.get())
                return SQL_RESULT_ERROR;

            return currentQuery->FetchNextRow(columnBindings);
        }

        const ColumnMetaVector * Statement::GetMeta() const
        {
            if (!currentQuery.get())
                return 0;

            return &currentQuery->GetMeta();
        }

        bool Statement::MakeRequestGetColumnsMeta(const std::string& cache, const std::string& table, const std::string& column)
        {
            //std::auto_ptr<ResultPage> resultPage(new ResultPage());

            QueryGetColumnsMetaRequest req(cache, table, column);
            QueryGetColumnsMetaResponse rsp;

            bool success = connection.SyncMessage(req, rsp);

            if (!success)
                return false;

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                return false;
            }

            //cursor->UpdateData(resultPage);

            return true;
        }
    }
}

