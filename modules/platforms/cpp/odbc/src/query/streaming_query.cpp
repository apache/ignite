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
#include "ignite/odbc/query/streaming_query.h"
#include "ignite/odbc/sql/sql_set_streaming_command.h"


namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            StreamingQuery::StreamingQuery(
                diagnostic::Diagnosable& diag,
                Connection& connection,
                const app::ParameterSet& params,
                const SqlSetStreamingCommand& cmd) :
                Query(diag, QueryType::STREAMING),
                connection(connection),
                params(params),
                cmd(cmd),
                order(0),
                executed(false),
                currentBatch(),
                responseBuffer(1024)
            {
                // No-op.
            }

            StreamingQuery::~StreamingQuery()
            {
                // No-op.
            }

            SqlResult::Type StreamingQuery::Execute()
            {
                currentBatch.AddRow(sql, params);

                if (currentBatch.GetSize() < cmd.GetBatchSize())
                    return SqlResult::AI_SUCCESS;

                return Flush(false);
            }

            const meta::ColumnMetaVector& StreamingQuery::GetMeta() const
            {
                static meta::ColumnMetaVector empty;

                return empty;
            }

            SqlResult::Type StreamingQuery::FetchNextRow(app::ColumnBindingMap&)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type StreamingQuery::GetColumn(uint16_t, app::ApplicationDataBuffer&)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                diag.AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                    "Cursor has reached end of the result set.");

                return SqlResult::AI_ERROR;
            }

            SqlResult::Type StreamingQuery::Close()
            {
                Flush(true);

                executed = false;

                return SqlResult::AI_SUCCESS;
            }

            bool StreamingQuery::DataAvailable() const
            {
                return false;
            }

            int64_t StreamingQuery::AffectedRows() const
            {
                return 0;
            }

            SqlResult::Type StreamingQuery::NextResultSet()
            {
                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type StreamingQuery::Flush(bool last)
            {
                if (currentBatch.GetSize() == 0 && !last)
                    return SqlResult::AI_SUCCESS;

                SqlResult::Type res = MakeRequestStreamingBatch(last);

                currentBatch.Clear();

                return res;
            }

            void StreamingQuery::PrepareQuery(const std::string& query)
            {
                sql = query;
            }

            SqlResult::Type StreamingQuery::MakeRequestStreamingBatch(bool last)
            {
                const std::string& schema = connection.GetSchema();

                StreamingBatchRequest req(schema, currentBatch, last);
                Response rsp;

                currentBatch.Synchronize();

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

                currentBatch.Clear();
                
                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type StreamingQuery::MakeRequestStreamingBatchOrdered(bool last)
            {
                const std::string& schema = connection.GetSchema();

                StreamingBatchOrderedRequest req(schema, currentBatch, last, order);
                StreamingBatchOrderedResponse rsp;

                currentBatch.Synchronize();

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

                currentBatch.Clear();

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetErrorCode() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetErrorMessage());

                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetErrorCode()), rsp.GetErrorMessage());

                    return SqlResult::AI_ERROR;
                }

                assert(order == rsp.GetOrder());

                ++order;

                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

