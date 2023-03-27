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
#include "ignite/odbc/sql/sql_set_streaming_command.h"

#include "ignite/odbc/streaming/streaming_context.h"

namespace ignite
{
    namespace odbc
    {
        namespace streaming
        {
            StreamingContext::StreamingContext() :
                connection(0),
                batchSize(0),
                order(0),
                enabled(false),
                currentBatch()
            {
                // No-op.
            }

            StreamingContext::~StreamingContext()
            {
                // No-op.
            }

            SqlResult::Type StreamingContext::Enable(const SqlSetStreamingCommand& cmd)
            {
                SqlResult::Type res = SqlResult::AI_SUCCESS;

                if (enabled)
                    res = Disable();

                if (res != SqlResult::AI_SUCCESS)
                    return res;

                batchSize = cmd.GetBatchSize();

                enabled = true;

                order = 0;

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type StreamingContext::Disable()
            {
                LOG_MSG("Disabling streaming context.");

                SqlResult::Type res = SqlResult::AI_SUCCESS;

                if (enabled)
                    res = Flush(true);

                enabled = false;

                return res;
            }

            SqlResult::Type StreamingContext::Execute(const std::string& sql, const app::ParameterSet& params)
            {
                assert(enabled);

                currentBatch.AddRow(sql, params);

                if (currentBatch.GetSize() < batchSize)
                    return SqlResult::AI_SUCCESS;

                return Flush(false);
            }

            SqlResult::Type StreamingContext::Flush(bool last)
            {
                LOG_MSG("Flushing data");

                if (currentBatch.GetSize() == 0 && !last)
                    return SqlResult::AI_SUCCESS;

                SqlResult::Type res = MakeRequestStreamingBatch(last);

                currentBatch.Clear();

                return res;
            }

            SqlResult::Type StreamingContext::MakeRequestStreamingBatch(bool last)
            {
                assert(connection != 0);

                const std::string& schema = connection->GetSchema();

                StreamingBatchRequest req(schema, currentBatch, last, order);
                StreamingBatchResponse rsp;

                try
                {
                    connection->SyncMessage(req, rsp);
                }
                catch (const OdbcError& err)
                {
                    connection->AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    connection->AddStatusRecord(err.GetText());

                    return SqlResult::AI_ERROR;
                }

                currentBatch.Clear();

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());

                    connection->AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetErrorCode() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetErrorMessage());

                    connection->AddStatusRecord(ResponseStatusToSqlState(rsp.GetErrorCode()), rsp.GetErrorMessage());

                    return SqlResult::AI_ERROR;
                }

                assert(order == rsp.GetOrder());

                ++order;

                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

