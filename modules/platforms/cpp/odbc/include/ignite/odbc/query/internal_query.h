/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_ODBC_QUERY_INTERNAL_QUERY
#define _IGNITE_ODBC_QUERY_INTERNAL_QUERY

#include <stdint.h>

#include <map>

#include "ignite/odbc/diagnostic/diagnosable.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/query/query.h"
#include "ignite/odbc/sql/sql_command.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            /**
             * Query.
             */
            class InternalQuery : public Query
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param diag Diagnosable.
                 * @param sql SQL query.
                 * @param cmd Parsed command.
                 */
                InternalQuery(diagnostic::Diagnosable& diag, const std::string& sql, std::auto_ptr<SqlCommand> cmd) :
                    Query(diag, QueryType::INTERNAL),
                    sql(sql),
                    cmd(cmd)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~InternalQuery()
                {
                    // No-op.
                }

                /**
                 * Execute query.
                 *
                 * @return True on success.
                 */
                virtual SqlResult::Type Execute()
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Internal error.");

                    return SqlResult::AI_ERROR;
                }

                /**
                 * Fetch next result row to application buffers.
                 *
                 * @param columnBindings Application buffers to put data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type FetchNextRow(app::ColumnBindingMap& columnBindings)
                {
                    (void) columnBindings;

                    return SqlResult::AI_NO_DATA;
                }

                /**
                 * Get data of the specified column in the result set.
                 *
                 * @param columnIdx Column index.
                 * @param buffer Buffer to put column data to.
                 * @return Operation result.
                 */
                virtual SqlResult::Type GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
                {
                    (void) columnIdx;
                    (void) buffer;

                    return SqlResult::AI_NO_DATA;
                }

                /**
                 * Close query.
                 *
                 * @return Operation result.
                 */
                virtual SqlResult::Type Close()
                {
                    return SqlResult::AI_SUCCESS;
                }

                /**
                 * Get column metadata.
                 *
                 * @return Column metadata.
                 */
                virtual const meta::ColumnMetaVector& GetMeta() const
                {
                    static const meta::ColumnMetaVector empty;

                    return empty;
                }

                /**
                 * Check if data is available.
                 *
                 * @return True if data is available.
                 */
                virtual bool DataAvailable() const
                {
                    return false;
                }

                /**
                 * Get number of rows affected by the statement.
                 *
                 * @return Number of rows affected by the statement.
                 */
                virtual int64_t AffectedRows() const
                {
                    return 0;
                }

                /**
                 * Move to the next result set.
                 *
                 * @return Operation result.
                 */
                virtual SqlResult::Type NextResultSet()
                {
                    return SqlResult::AI_NO_DATA;
                }

                /**
                 * Get SQL query.
                 *
                 * @return SQL query.
                 */
                SqlCommand& GetCommand() const
                {
                    return *cmd;
                }

                /**
                 * Get SQL query.
                 *
                 * @return SQL Query.
                 */
                const std::string& GetQuery() const
                {
                    return sql;
                }

            protected:
                /** SQL string*/
                std::string sql;

                /** SQL command. */
                std::auto_ptr<SqlCommand> cmd;
            };
        }
    }
}

#endif //_IGNITE_ODBC_QUERY_INTERNAL_QUERY
