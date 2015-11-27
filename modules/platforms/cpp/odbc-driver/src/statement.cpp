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

// Default result page size.
#define DEFAULT_PAGE_SIZE 32

namespace ignite
{
    namespace odbc
    {
        Statement::Statement(Connection& parent) :
            connection(parent), columnBindings(), resultMeta()
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
            if (sql.empty())
                return false;

            bool success = MakeRequestExecute();

            if (!success)
                return false;

            opened = true;

            return true;
        }

        bool Statement::Close()
        {
            if (!cursor.get())
                return false;

            bool success = MakeRequestClose();

            cursor.reset();

            return success;
        }

        SqlResult Statement::FetchRow()
        {
            if (!cursor.get())
                return SQL_RESULT_ERROR;

            if (!cursor->HasNext())
            {
                return SQL_RESULT_NO_DATA;
            }

            if (cursor->NeedDataUpdate())
            {
                bool success = MakeRequestFetch();

                if (!success)
                    return SQL_RESULT_ERROR;
            }
            else
                cursor->Increment();


            Row* row = cursor->GetRow();

            if (!row)
                return SQL_RESULT_ERROR;

            for (int32_t i = 1; i < row->GetSize() + 1; ++i)
            {
                ColumnBindingMap::iterator it = columnBindings.find(i);

                bool success;

                if (it != columnBindings.end())
                    success = row->ReadColumnToBuffer(it->second);
                else
                    success = row->SkipColumn();

                if (!success)
                    return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
        }

        bool Statement::MakeRequestExecute()
        {
            const std::string& cacheName = connection.GetCache();

            QueryExecuteRequest req(cacheName, sql);
            QueryExecuteResponse rsp;

            bool success = connection.SyncMessage(req, rsp);

            if (!success)
                return false;

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                return false;
            }

            cursor.reset(new Cursor(rsp.GetQueryId()));

            resultMeta.assign(rsp.GetMeta().begin(), rsp.GetMeta().end());

            LOG_MSG("Query id: %lld\n", cursor->GetQueryId());

            for (int i = 0; i < rsp.GetMeta().size(); ++i)
            {
                LOG_MSG("[%d] SchemaName:    %s\n", i, rsp.GetMeta()[i].GetSchemaName().c_str());
                LOG_MSG("[%d] TypeName:      %s\n", i, rsp.GetMeta()[i].GetTypeName().c_str());
                LOG_MSG("[%d] FieldName:     %s\n", i, rsp.GetMeta()[i].GetFieldName().c_str());
                LOG_MSG("[%d] FieldTypeName: %s\n", i, rsp.GetMeta()[i].GetFieldTypeName().c_str());
                LOG_MSG("\n");
            }

            return true;
        }

        bool Statement::MakeRequestClose()
        {
            QueryCloseRequest req(cursor->GetQueryId());
            QueryCloseResponse rsp;

            bool success = connection.SyncMessage(req, rsp);

            if (!success)
                return false;

            LOG_MSG("Query id: %lld\n", rsp.GetQueryId());

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                return false;
            }

            return true;
        }

        bool Statement::MakeRequestFetch()
        {
            std::auto_ptr<ResultPage> resultPage(new ResultPage());

            QueryFetchRequest req(cursor->GetQueryId(), DEFAULT_PAGE_SIZE);
            QueryFetchResponse rsp(*resultPage);

            bool success = connection.SyncMessage(req, rsp);

            LOG_MSG("Query id: %lld\n", rsp.GetQueryId());

            if (!success)
                return false;

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                return false;
            }

            cursor->UpdateData(resultPage);

            return true;
        }
    }
}

