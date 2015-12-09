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
#include "ignite/odbc/message.h"
#include "ignite/odbc/query/data_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            DataQuery::DataQuery(Connection& connection, const std::string& sql, 
                const app::ParameterBindingMap& params) :
                connection(connection), sql(sql), params(params)
            {
                // No-op.
            }

            DataQuery::~DataQuery()
            {
                Close();
            }
            
            bool DataQuery::Execute()
            {
                if (cursor.get())
                    return false;

                return MakeRequestExecute();
            }

            const meta::ColumnMetaVector & DataQuery::GetMeta() const
            {
                return resultMeta;
            }

            SqlResult DataQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
            {
                if (!cursor.get())
                    return SQL_RESULT_ERROR;

                if (!cursor->HasNext())
                    return SQL_RESULT_NO_DATA;

                if (cursor->NeedDataUpdate())
                {
                    bool success = MakeRequestFetch();

                    if (!success)
                        return SQL_RESULT_ERROR;

                    if (!cursor->HasNext())
                        return SQL_RESULT_NO_DATA;
                }
                else
                    cursor->Increment();

                Row* row = cursor->GetRow();

                if (!row)
                    return SQL_RESULT_ERROR;

                for (int32_t i = 1; i < row->GetSize() + 1; ++i)
                {
                    app::ColumnBindingMap::iterator it = columnBindings.find(i);

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

            bool DataQuery::Close()
            {
                if (!cursor.get())
                    return false;

                MakeRequestClose();

                cursor.reset();

                return false;
            }

            bool DataQuery::DataAvailable() const
            {
                return cursor.get() && cursor->HasNext();
            }

            bool DataQuery::MakeRequestExecute()
            {
                const std::string& cacheName = connection.GetCache();

                QueryExecuteRequest req(cacheName, sql, params);
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
                    LOG_MSG("[%d] SchemaName:     %s\n", i, rsp.GetMeta()[i].GetSchemaName().c_str());
                    LOG_MSG("[%d] TypeName:       %s\n", i, rsp.GetMeta()[i].GetTableName().c_str());
                    LOG_MSG("[%d] ColumnName:     %s\n", i, rsp.GetMeta()[i].GetColumnName().c_str());
                    LOG_MSG("[%d] ColumnTypeName: %s\n", i, rsp.GetMeta()[i].GetColumnTypeName().c_str());
                    LOG_MSG("\n");
                }

                return true;
            }

            bool DataQuery::MakeRequestClose()
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

            bool DataQuery::MakeRequestFetch()
            {
                std::auto_ptr<ResultPage> resultPage(new ResultPage());

                QueryFetchRequest req(cursor->GetQueryId(), ResultPage::DEFAULT_SIZE);
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
}

