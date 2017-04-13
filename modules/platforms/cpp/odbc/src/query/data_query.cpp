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
                Query(diag, QueryType::DATA),
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

            SqlResult::Type DataQuery::Execute()
            {
                if (cursor.get())
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query cursor is in open state already.");

                    return SqlResult::AI_ERROR;
                }

                return MakeRequestExecute();
            }

            const meta::ColumnMetaVector & DataQuery::GetMeta() const
            {
                return resultMeta;
            }

            SqlResult::Type DataQuery::FetchNextRow(app::ColumnBindingMap& columnBindings)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                if (!cursor->HasData())
                    return SqlResult::AI_NO_DATA;

                cursor->Increment();

                if (cursor->NeedDataUpdate())
                {
                    SqlResult::Type result = MakeRequestFetch();

                    if (result != SqlResult::AI_SUCCESS)
                        return result;
                }

                if (!cursor->HasData())
                    return SqlResult::AI_NO_DATA;

                Row* row = cursor->GetRow();

                if (!row)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Unknown error.");

                    return SqlResult::AI_ERROR;
                }

                for (int32_t i = 1; i < row->GetSize() + 1; ++i)
                {
                    app::ColumnBindingMap::iterator it = columnBindings.find(i);

                    if (it == columnBindings.end())
                        continue;

                    SqlResult::Type result = row->ReadColumnToBuffer(i, it->second);

                    if (result == SqlResult::AI_ERROR)
                    {
                        diag.AddStatusRecord(SqlState::S01S01_ERROR_IN_ROW, "Can not retrieve row column.", 0, i);

                        return SqlResult::AI_ERROR;
                    }
                }

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type DataQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                if (!cursor.get())
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                Row* row = cursor->GetRow();

                if (!row)
                    return SqlResult::AI_NO_DATA;

                SqlResult::Type result = row->ReadColumnToBuffer(columnIdx, buffer);

                if (result == SqlResult::AI_ERROR)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Unknown column type.");

                    return SqlResult::AI_ERROR;
                }

                return result;
            }

            SqlResult::Type DataQuery::Close()
            {
                if (!cursor.get())
                    return SqlResult::AI_SUCCESS;

                SqlResult::Type result = MakeRequestClose();

                if (result == SqlResult::AI_SUCCESS)
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

            SqlResult::Type DataQuery::MakeRequestExecute()
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
                    diag.AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, rsp.GetError());

                    return SqlResult::AI_ERROR;
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

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type DataQuery::MakeRequestClose()
            {
                QueryCloseRequest req(cursor->GetQueryId());
                QueryCloseResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                LOG_MSG("Query id: " << rsp.GetQueryId());

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type DataQuery::MakeRequestFetch()
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
                    diag.AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                cursor->UpdateData(resultPage);

                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

