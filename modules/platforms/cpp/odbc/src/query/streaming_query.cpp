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

            const meta::ColumnMetaVector& StreamingQuery::GetMeta() const
            {
                static meta::ColumnMetaVector empty;

                return empty;
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

