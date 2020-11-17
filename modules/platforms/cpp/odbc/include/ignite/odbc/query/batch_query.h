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

#ifndef _IGNITE_ODBC_QUERY_BATCH_QUERY
#define _IGNITE_ODBC_QUERY_BATCH_QUERY

#include "ignite/odbc/query/query.h"
#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/cursor.h"

namespace ignite
{
    namespace odbc
    {
        /** Connection forward-declaration. */
        class Connection;

        namespace query
        {
            /**
             * Query.
             */
            class BatchQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnostics collector.
                 * @param connection Associated connection.
                 * @param sql SQL query string.
                 * @param params SQL params.
                 * @param timeout Timeout in seconds.
                 */
                BatchQuery(diagnostic::DiagnosableAdapter& diag, Connection& connection, const std::string& sql,
                    const app::ParameterSet& params, int32_t& timeout);

                /**
                 * Destructor.
                 */
                virtual ~BatchQuery();

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
                virtual const meta::ColumnMetaVector* GetMeta();

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

            private:
                IGNITE_NO_COPY_ASSIGNMENT(BatchQuery);

                /**
                 * Make query execute request and use response to set internal
                 * state.
                 *
                 * @param begin Paramset interval beginning.
                 * @param end Paramset interval end.
                 * @param last Last page flag.
                 * @return Result.
                 */
                SqlResult::Type MakeRequestExecuteBatch(SqlUlen begin, SqlUlen end, bool last);

                /** Connection associated with the statement. */
                Connection& connection;

                /** SQL Query. */
                std::string sql;

                /** Parameter bindings. */
                const app::ParameterSet& params;

                /** Columns metadata. */
                meta::ColumnMetaVector resultMeta;

                /** Number of rows affected. */
                std::vector<int64_t> rowsAffected;

                /** Rows affected index. */
                size_t rowsAffectedIdx;

                /** Query executed. */
                bool executed;

                /** Timeout. */
                int32_t& timeout;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_BATCH_QUERY
