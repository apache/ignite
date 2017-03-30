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
#include "ignite/odbc/log.h"
#include "ignite/odbc/query/data_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            DataQuery::DataQuery(diagnostic::Diagnosable& diag,
                Connection& connection, const std::string& sql,
                const app::ParameterBindingMap& params) :
                Query(diag, DATA),
                connection(connection),
                sql(sql),
                params(params)
            {
                // No-op.
            }

            DataQuery::~DataQuery()
            {
                Close();
            }

            SqlResult DataQuery::Execute()
            {
                if (cursor.get())
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query cursor is in open state already.");

                    return SQL_RESULT_ERROR;
                }

                return MakeRequestExecute();
            }

            const meta::ColumnMetaVector & DataQuery::GetMeta() const
            {
                return resultMeta;
            }

            SqlResult DataQuery::FetchNextRow(app::ColumnBindingMap& columnBindings)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (!cursor->HasData())
                    return SQL_RESULT_NO_DATA;

                cursor->Increment();

                if (cursor->NeedDataUpdate())
                {
                    SqlResult result = MakeRequestFetch();

                    if (result != SQL_RESULT_SUCCESS)
                        return result;
                }

                if (!cursor->HasData())
                    return SQL_RESULT_NO_DATA;

                Row* row = cursor->GetRow();

                if (!row)
                {
                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, "Unknown error.");

                    return SQL_RESULT_ERROR;
                }

                for (int32_t i = 1; i < row->GetSize() + 1; ++i)
                {
                    app::ColumnBindingMap::iterator it = columnBindings.find(i);

                    if (it == columnBindings.end())
                        continue;

                    SqlResult result = row->ReadColumnToBuffer(i, it->second);

                    if (result == SQL_RESULT_ERROR)
                    {
                        diag.AddStatusRecord(SQL_STATE_01S01_ERROR_IN_ROW, "Can not retrieve row column.", 0, i);

                        return SQL_RESULT_ERROR;
                    }
                }

                return SQL_RESULT_SUCCESS;
            }

            SqlResult DataQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                Row* row = cursor->GetRow();

                if (!row)
                    return SQL_RESULT_NO_DATA;

                SqlResult result = row->ReadColumnToBuffer(columnIdx, buffer);

                if (result == SQL_RESULT_ERROR)
                {
                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, "Unknown column type.");

                    return SQL_RESULT_ERROR;
                }

                return result;
            }

            SqlResult DataQuery::Close()
            {
                if (!cursor.get())
                    return SQL_RESULT_SUCCESS;

                SqlResult result = MakeRequestClose();

                if (result == SQL_RESULT_SUCCESS)
                {
                    cursor.reset();

                    resultMeta.clear();
                }

                return result;
            }

            bool DataQuery::DataAvailable() const
            {
                return cursor.get() && cursor->HasData();
            }

            int64_t DataQuery::AffectedRows() const
            {
                // We are only support SELECT statements so we can not affect any row.
                return 0;
            }

            SqlResult DataQuery::MakeRequestExecute()
            {
                const std::string& cacheName = connection.GetCache();

                QueryExecuteRequest req(cacheName, sql, params);
                QueryExecuteResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SQL_RESULT_ERROR;
                }

                if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, rsp.GetError());

                    return SQL_RESULT_ERROR;
                }

                cursor.reset(new Cursor(rsp.GetQueryId()));

                resultMeta.assign(rsp.GetMeta().begin(), rsp.GetMeta().end());

                LOG_MSG("Query id: " << cursor->GetQueryId());
                for (size_t i = 0; i < rsp.GetMeta().size(); ++i)
                {
                    LOG_MSG("\n[" << i << "] SchemaName:     " << rsp.GetMeta()[i].GetSchemaName()
                        <<  "\n[" << i << "] TypeName:       " << rsp.GetMeta()[i].GetTableName()
                        <<  "\n[" << i << "] ColumnName:     " << rsp.GetMeta()[i].GetColumnName()
                        <<  "\n[" << i << "] ColumnType:     " << rsp.GetMeta()[i].GetDataType());
                }

                return SQL_RESULT_SUCCESS;
            }

            SqlResult DataQuery::MakeRequestClose()
            {
                QueryCloseRequest req(cursor->GetQueryId());
                QueryCloseResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SQL_RESULT_ERROR;
                }

                LOG_MSG("Query id: " << rsp.GetQueryId());

                if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, rsp.GetError());

                    return SQL_RESULT_ERROR;
                }

                return SQL_RESULT_SUCCESS;
            }

            SqlResult DataQuery::MakeRequestFetch()
            {
                std::auto_ptr<ResultPage> resultPage(new ResultPage());

                QueryFetchRequest req(cursor->GetQueryId(), connection.GetConfiguration().GetPageSize());
                QueryFetchResponse rsp(*resultPage);

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SQL_RESULT_ERROR;
                }

                if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, rsp.GetError());

                    return SQL_RESULT_ERROR;
                }

                cursor->UpdateData(resultPage);

                return SQL_RESULT_SUCCESS;
            }
        }
    }
}

