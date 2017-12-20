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
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/data_query.h"
#include "ignite/odbc/query/batch_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            DataQuery::DataQuery(diagnostic::Diagnosable& diag, Connection& connection, const std::string& sql,
                const app::ParameterSet& params, int32_t& timeout) :
                Query(diag, QueryType::DATA),
                connection(connection),
                sql(sql),
                params(params),
                resultMeta(),
                cursor(),
                rowsAffected(),
                rowsAffectedIdx(0),
                cachedNextPage(),
                timeout(timeout)
            {
                // No-op.
            }

            DataQuery::~DataQuery()
            {
                InternalClose();
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
                    if (cachedNextPage.get())
                        cursor->UpdateData(cachedNextPage);
                    else
                    {
                        SqlResult::Type result = MakeRequestFetch();

                        if (result != SqlResult::AI_SUCCESS)
                            return result;
                    }
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
                {
                    diag.AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return SqlResult::AI_ERROR;
                }

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
                return InternalClose();
            }

            SqlResult::Type DataQuery::InternalClose()
            {
                if (!cursor.get())
                    return SqlResult::AI_SUCCESS;

                SqlResult::Type result = SqlResult::AI_SUCCESS;

                if (!cursor->IsClosedRemotely())
                    result = MakeRequestClose();

                if (result == SqlResult::AI_SUCCESS)
                {
                    cursor.reset();

                    resultMeta.clear();

                    rowsAffectedIdx = 0;

                    rowsAffected.clear();
                }

                return result;
            }

            bool DataQuery::DataAvailable() const
            {
                return cursor.get() && cursor->HasData();
            }

            int64_t DataQuery::AffectedRows() const
            {
                int64_t affected = rowsAffectedIdx < rowsAffected.size() ? rowsAffected[rowsAffectedIdx] : 0;
                return affected < 0 ? 0 : affected;
            }

            SqlResult::Type DataQuery::NextResultSet()
            {
                if (rowsAffectedIdx + 1 >= rowsAffected.size())
                {
                    InternalClose();

                    return SqlResult::AI_NO_DATA;
                }

                SqlResult::Type res = MakeRequestMoreResults();

                if (res == SqlResult::AI_SUCCESS)
                    ++rowsAffectedIdx;

                return res;
            }

            SqlResult::Type DataQuery::MakeRequestExecute()
            {
                const std::string& schema = connection.GetSchema();

                QueryExecuteRequest req(schema, sql, params, timeout);
                QueryExecuteResponse rsp;

                try
                {
                    // Setting connection timeout to 1 second more than query timeout itself.
                    int32_t connectionTimeout = timeout ? timeout + 1 : 0;

                    bool success = connection.SyncMessage(req, rsp, connectionTimeout);

                    if (!success)
                    {
                        diag.AddStatusRecord(SqlState::SHYT00_TIMEOUT_EXPIRED, "Query timeout expired");

                        return SqlResult::AI_ERROR;
                    }
                }
                catch (const OdbcError& err)
                {
                    diag.AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                resultMeta.assign(rsp.GetMeta().begin(), rsp.GetMeta().end());

                rowsAffected = rsp.GetAffectedRows();

                LOG_MSG("Query id: " << rsp.GetQueryId());
                LOG_MSG("Affected Rows list size: " << rowsAffected.size());

                for (size_t i = 0; i < resultMeta.size(); ++i)
                {
                    LOG_MSG("\n[" << i << "] SchemaName:     " << resultMeta[i].GetSchemaName()
                        <<  "\n[" << i << "] TypeName:       " << resultMeta[i].GetTableName()
                        <<  "\n[" << i << "] ColumnName:     " << resultMeta[i].GetColumnName()
                        <<  "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(resultMeta[i].GetDataType()));
                }

                cursor.reset(new Cursor(rsp.GetQueryId()));

                rowsAffectedIdx = 0;

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
                catch (const OdbcError& err)
                {
                    diag.AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                LOG_MSG("Query id: " << rsp.GetQueryId());

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

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
                catch (const OdbcError& err)
                {
                    diag.AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                LOG_MSG("Page size:    " << resultPage->GetSize());
                LOG_MSG("Page is last: " << resultPage->IsLast());

                cursor->UpdateData(resultPage);

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type DataQuery::MakeRequestMoreResults()
            {
                std::auto_ptr<ResultPage> resultPage(new ResultPage());

                QueryMoreResultsRequest req(cursor->GetQueryId(), connection.GetConfiguration().GetPageSize());
                QueryMoreResultsResponse rsp(*resultPage);

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const OdbcError& err)
                {
                    diag.AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                LOG_MSG("Page size:    " << resultPage->GetSize());
                LOG_MSG("Page is last: " << resultPage->IsLast());

                cachedNextPage = resultPage;
                cursor.reset(new Cursor(rsp.GetQueryId()));

                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

