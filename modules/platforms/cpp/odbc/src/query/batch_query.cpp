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
#include "ignite/odbc/query/batch_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            BatchQuery::BatchQuery(diagnostic::Diagnosable& diag, Connection& connection,
                const std::string& sql, const app::ParameterSet& params) :
                Query(diag, BATCH),
                connection(connection),
                sql(sql),
                params(params),
                resultMeta(),
                rowsAffected(0),
                setsProcessed(0),
                executed(false),
                dataRetrieved(false)
            {
                // No-op.
            }

            BatchQuery::~BatchQuery()
            {
                // No-op.
            }

            SqlResult BatchQuery::Execute()
            {
                if (executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query cursor is in open state already.");

                    return SQL_RESULT_ERROR;
                }

                int32_t maxPageSize = connection.GetConfiguration().GetPageSize();
                int32_t rowNum = params.GetParamSetSize();
                SqlResult res;

                int32_t processed = 0;

                do {
                    int32_t currentPageSize = std::min(maxPageSize, rowNum - processed);
                    bool lastPage = currentPageSize == rowNum - processed;

                    res = MakeRequestExecuteBatch(processed, processed + currentPageSize, lastPage);

                    processed += currentPageSize;
                } while (res == SQL_RESULT_SUCCESS && processed < rowNum);

                params.SetParamsProcessed(static_cast<SqlUlen>(setsProcessed));

                return res;
            }

            const meta::ColumnMetaVector& BatchQuery::GetMeta() const
            {
                return resultMeta;
            }

            SqlResult BatchQuery::FetchNextRow(app::ColumnBindingMap& columnBindings)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (dataRetrieved)
                    return SQL_RESULT_NO_DATA;

                app::ColumnBindingMap::iterator it = columnBindings.find(1);

                if (it != columnBindings.end())
                    it->second.PutInt64(rowsAffected);

                dataRetrieved = true;

                return SQL_RESULT_SUCCESS;
            }

            SqlResult BatchQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (dataRetrieved)
                    return SQL_RESULT_NO_DATA;

                if (columnIdx != 1)
                {
                    std::stringstream builder;
                    builder << "Column with id " << columnIdx << " is not available in result set.";

                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, builder.str());

                    return SQL_RESULT_ERROR;
                }

                buffer.PutInt64(rowsAffected);

                return SQL_RESULT_SUCCESS;
            }

            SqlResult BatchQuery::Close()
            {
                return SQL_RESULT_SUCCESS;
            }

            bool BatchQuery::DataAvailable() const
            {
                return false;
            }

            int64_t BatchQuery::AffectedRows() const
            {
                return rowsAffected;
            }

            SqlResult BatchQuery::MakeRequestExecuteBatch(SqlUlen begin, SqlUlen end, bool last)
            {
                const std::string& schema = connection.GetCache();

                QueryExecuteBatchtRequest req(schema, sql, params, begin, end, last);
                QueryExecuteBatchResponse rsp;

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

                rowsAffected += rsp.GetAffectedRows();
                LOG_MSG("rowsAffected: " << rowsAffected);

                if (!rsp.GetErrorMessage().empty())
                {
                    LOG_MSG("Error: " << rsp.GetErrorMessage());

                    setsProcessed += rsp.GetErrorSetIdx();
                    LOG_MSG("setsProcessed: " << setsProcessed);

                    diag.AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, rsp.GetErrorMessage(),
                        static_cast<int32_t>(setsProcessed), 0);

                    return SQL_RESULT_SUCCESS_WITH_INFO;
                }

                setsProcessed += end - begin;
                LOG_MSG("setsProcessed: " << setsProcessed);

                return SQL_RESULT_SUCCESS;
            }
        }
    }
}

