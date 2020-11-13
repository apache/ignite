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
                diagnostic::DiagnosableAdapter& diag,
                Connection& connection,
                const app::ParameterSet& params) :
                Query(diag, QueryType::STREAMING),
                connection(connection),
                params(params)
            {
                // No-op.
            }

            StreamingQuery::~StreamingQuery()
            {
                // No-op.
            }

            SqlResult::Type StreamingQuery::Execute()
            {
                return connection.GetStreamingContext().Execute(sql, params);
            }

            const meta::ColumnMetaVector* StreamingQuery::GetMeta()
            {
                return 0;
            }

            SqlResult::Type StreamingQuery::FetchNextRow(app::ColumnBindingMap&)
            {
                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type StreamingQuery::GetColumn(uint16_t, app::ApplicationDataBuffer&)
            {
                diag.AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE, "Column is not available.");

                return SqlResult::AI_ERROR;
            }

            SqlResult::Type StreamingQuery::Close()
            {
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
        }
    }
}

