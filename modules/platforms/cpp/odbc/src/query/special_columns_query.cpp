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

#include <ignite/impl/binary/binary_common.h>

#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/query/special_columns_query.h"

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            SpecialColumnsQuery::SpecialColumnsQuery(diagnostic::DiagnosableAdapter& diag,
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

            const meta::ColumnMetaVector* SpecialColumnsQuery::GetMeta()
            {
                return &columnsMeta;
            }

            SqlResult::Type SpecialColumnsQuery::FetchNextRow(app::ColumnBindingMap&)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type SpecialColumnsQuery::GetColumn(uint16_t, app::ApplicationDataBuffer&)
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
