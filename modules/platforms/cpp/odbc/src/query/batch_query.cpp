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
#include "ignite/odbc/query/batch_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            BatchQuery::BatchQuery(diagnostic::Diagnosable& diag, Connection& connection,
                const std::string& sql, const app::ParameterSet& params) :
                Query(diag, QueryType::DATA),
                connection(connection),
                sql(sql),
                params(params),
                rowsAffected(-1),
                executed(false),
                dataRetrieved(false)
            {
                // No-op.
            }

            BatchQuery::~BatchQuery()
            {
                // No-op.
            }

            SqlResult::Type BatchQuery::Execute()
            {
                if (executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query cursor is in open state already.");

                    return SqlResult::AI_ERROR;
                }

                int32_t maxPageSize = connection.GetConfiguration().GetPageSize();
                int32_t rowNum = params.GetRowNumber();

                int32_t currentPageSize = std::min(maxPageSize, rowNum);

                SqlResult::Type res = MakeRequestExecuteStart(0, currentPageSize, currentPageSize == rowNum);

                int32_t processed = currentPageSize;

                while (res == SqlResult::AI_SUCCESS && processed < rowNum)
                {
                    currentPageSize = std::min(maxPageSize, rowNum - processed);

                    res = MakeRequestExecuteContinue(processed, processed + currentPageSize, currentPageSize == rowNum - processed);

                    processed += currentPageSize;
                }

                return res;
            }

            const meta::ColumnMetaVector& BatchQuery::GetMeta() const
            {
                return resultMeta;
            }

            SqlResult::Type BatchQuery::FetchNextRow(app::ColumnBindingMap& columnBindings)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                if (dataRetrieved)
                    return SqlResult::AI_NO_DATA;

                app::ColumnBindingMap::iterator it = columnBindings.find(1);

                if (it != columnBindings.end())
                    it->second.PutInt64(rowsAffected);

                dataRetrieved = true;

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type BatchQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                //if (!cursor.get())
                //{
                //    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                //    return SqlResult::AI_ERROR;
                //}

                //Row* row = cursor->GetRow();

                //if (!row)
                //    return SqlResult::AI_NO_DATA;

                //SqlResult::Type result = row->ReadColumnToBuffer(columnIdx, buffer);

                //if (result == SqlResult::AI_ERROR)
                //{
                //    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Unknown column type.");

                //    return SqlResult::AI_ERROR;
                //}

                //return result;
                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type BatchQuery::Close()
            {
                return SqlResult::AI_SUCCESS;
            }

            bool BatchQuery::DataAvailable() const
            {
                return false;
            }

            int64_t BatchQuery::AffectedRows() const
            {
                return rowsAffected;
            }

            SqlResult::Type BatchQuery::MakeRequestExecuteStart(SqlUlen begin, SqlUlen end, bool last)
            {
                //const std::string& schema = connection.GetSchema();

                //QueryExecuteBatchRequestStart req(schema, sql, params, begin, end, last);
                //QueryExecuteResponse rsp;

                //try
                //{
                //    connection.SyncMessage(req, rsp);
                //}
                //catch (const IgniteError& err)
                //{
                //    diag.AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                //    return SqlResult::AI_ERROR;
                //}

                //if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                //{
                //    LOG_MSG("Error: " << rsp.GetError());

                //    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, rsp.GetError());

                //    return SqlResult::AI_ERROR;
                //}

                //cursor.reset(new Cursor(rsp.GetQueryId()));

                //resultMeta.assign(rsp.GetMeta().begin(), rsp.GetMeta().end());

                //LOG_MSG("Query id: " << cursor->GetQueryId());
                //for (size_t i = 0; i < resultMeta.size(); ++i)
                //{
                //    LOG_MSG("\n[" << i << "] SchemaName:     " << resultMeta[i].GetSchemaName()
                //        <<  "\n[" << i << "] TypeName:       " << resultMeta[i].GetTableName()
                //        <<  "\n[" << i << "] ColumnName:     " << resultMeta[i].GetColumnName()
                //        <<  "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(resultMeta[i].GetDataType()));
                //}

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type BatchQuery::MakeRequestExecuteContinue(SqlUlen begin, SqlUlen end, bool last)
            {
                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

