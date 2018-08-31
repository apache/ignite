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

#ifndef _IGNITE_ODBC_QUERY_STREAMING_QUERY
#define _IGNITE_ODBC_QUERY_STREAMING_QUERY

#include "ignite/odbc/query/query.h"
#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/cursor.h"

#include "ignite/odbc/query/streaming/streaming_batch.h"

namespace ignite
{
    namespace odbc
    {
        /** Set streaming forward-declaration. */
        class SqlSetStreamingCommand;

        /** Connection forward-declaration. */
        class Connection;

        namespace query
        {
            /**
             * Streaming Query.
             */
            class StreamingQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param connection Associated connection.
                 * @param params SQL params.
                 * @param cmd Set streaming command.
                 */
                StreamingQuery(
                    diagnostic::Diagnosable& diag,
                    Connection& connection,
                    const app::ParameterSet& params,
                    const SqlSetStreamingCommand& cmd);

                /**
                 * Destructor.
                 */
                virtual ~StreamingQuery();

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual SqlResult::Type Execute();

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const meta::ColumnMetaVector& GetMeta() const;

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @param columnBindings Application buffers to put data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type FetchNextRow(app::ColumnBindingMap& columnBindings);

                /**
                 * Get data of the specified column in the result set.
                 *
                 * @param columnIdx Column index.
                 * @param buffer Buffer to put column data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer);

                /**
                 * Close query.
                 *
                 * @return Result.
                 */
                virtual SqlResult::Type Close();

                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const;

                /**
                 * Get number of rows affected by the statement.
                 *
                 * @return Number of rows affected by the statement.
                 */
                virtual int64_t AffectedRows() const;

                /**
                 * Move to the next result set.
                 * 
                 * @return Operaion result.
                 */
                virtual SqlResult::Type NextResultSet();

                /**
                 * Get SQL query string.
                 *
                 * @return SQL query string.
                 */
                const std::string& GetSql() const
                {
                    return sql;
                }

                /**
                 * Flush collected streaming data to remote server.
                 *
                 * @param last Last page indicator.
                 * @return Operation result.
                 */
                SqlResult::Type Flush(bool last);

                /**
                 * Prepare query for execution in a streaming mode.
                 *
                 * @param query Query.
                 */
                void PrepareQuery(const std::string& query);

            private:
                IGNITE_NO_COPY_ASSIGNMENT(StreamingQuery);

                /**
                 * Send batch request.
                 *
                 * @param last Last page flag.
                 * @return Result.
                 */
                SqlResult::Type MakeRequestStreamingBatch(bool last);

                /**
                 * Send batch ordered request.
                 *
                 * @param last Last page flag.
                 * @return Result.
                 */
                SqlResult::Type MakeRequestStreamingBatchOrdered(bool last);

                /** Connection associated with the statement. */
                Connection& connection;

                /** SQL Query. */
                std::string sql;

                /** Parameter bindings. */
                const app::ParameterSet& params;

                /** Ordered flag. */
                bool ordered;

                /** Batch size. */
                int32_t batchSize;

                /** Order. */
                int64_t order;

                /** Query executed. */
                bool executed;

                /** Current batch. */
                streaming::StreamingBatch currentBatch;

                /** Current response. */
                impl::interop::InteropUnpooledMemory responseBuffer;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_STREAMING_QUERY
