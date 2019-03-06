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

#include <ignite/impl/binary/binary_common.h>

#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/query/special_columns_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            SpecialColumnsQuery::SpecialColumnsQuery(diagnostic::Diagnosable& diag,
                int16_t type, const std::string& catalog, const std::string& schema,
                const std::string& table, int16_t scope, int16_t nullable) :
                Query(diag, QueryType::SPECIAL_COLUMNS),
                type(type),
                catalog(catalog),
                schema(schema),
                table(table),
                scope(scope),
                nullable(nullable),
                executed(false),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::odbc::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(8);

                const std::string sch("");
                const std::string tbl("");

                columnsMeta.push_back(ColumnMeta(sch, tbl, "SCOPE",          IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TYPE_NAME",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_SIZE",    IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "BUFFER_LENGTH",  IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DECIMAL_DIGITS", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "PSEUDO_COLUMN",  IGNITE_TYPE_SHORT));
            }

            SpecialColumnsQuery::~SpecialColumnsQuery()
            {
                // No-op.
            }

            SqlResult::Type SpecialColumnsQuery::Execute()
            {
                executed = true;

                return SqlResult::AI_SUCCESS;
            }

            const meta::ColumnMetaVector& SpecialColumnsQuery::GetMeta() const
            {
                return columnsMeta;
            }

            SqlResult::Type SpecialColumnsQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type SpecialColumnsQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type SpecialColumnsQuery::Close()
            {
                executed = false;

                return SqlResult::AI_SUCCESS;
            }

            bool SpecialColumnsQuery::DataAvailable() const
            {
                return false;
            }

            int64_t SpecialColumnsQuery::AffectedRows() const
            {
                return 0;
            }

            SqlResult::Type SpecialColumnsQuery::NextResultSet()
            {
                return SqlResult::AI_NO_DATA;
            }
        }
    }
}
