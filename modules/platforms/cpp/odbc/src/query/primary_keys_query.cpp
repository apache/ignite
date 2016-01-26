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
#include "ignite/odbc/connection.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/query/primary_keys_query.h"

namespace
{
    enum ResultColumn
    {
        /** Catalog name. NULL if not applicable to the data source. */
        TABLE_CAT = 1,

        /** Schema name. NULL if not applicable to the data source. */
        TABLE_SCHEM,

        /** Table name. */
        TABLE_NAME,

        /** Column name. */
        COLUMN_NAME,

        /** Column sequence number in key. */
        KEY_SEQ,

        /** Primary key name. */
        PK_NAME
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            PrimaryKeysQuery::PrimaryKeysQuery(diagnostic::Diagnosable& diag,
                Connection& connection, const std::string& catalog,
                const std::string& schema, const std::string& table) :
                Query(diag),
                connection(connection),
                catalog(catalog),
                schema(schema),
                table(table),
                executed(false),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::odbc::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(6);

                const std::string sch("");
                const std::string tbl("");

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_CAT",   SqlTypeName::VARCHAR,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_SCHEM", SqlTypeName::VARCHAR,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_NAME",  SqlTypeName::VARCHAR,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_NAME", SqlTypeName::VARCHAR,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "KEY_SEQ",     SqlTypeName::SMALLINT, IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "PK_NAME",     SqlTypeName::VARCHAR,  IGNITE_TYPE_STRING));
            }

            PrimaryKeysQuery::~PrimaryKeysQuery()
            {
                // No-op.
            }

            SqlResult PrimaryKeysQuery::Execute()
            {
                if (executed)
                    Close();

                meta.push_back(meta::PrimaryKeyMeta(catalog, schema, table, "_KEY", 1, "_KEY"));

                executed = true;

                cursor = meta.begin();

                return SQL_RESULT_SUCCESS;
            }

            const meta::ColumnMetaVector & PrimaryKeysQuery::GetMeta() const
            {
                return columnsMeta;
            }

            SqlResult PrimaryKeysQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (cursor == meta.end())
                    return SQL_RESULT_NO_DATA;

                app::ColumnBindingMap::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    GetColumn(it->first, it->second);

                ++cursor;

                return SQL_RESULT_SUCCESS;
            }

            SqlResult PrimaryKeysQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer& buffer)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (cursor == meta.end())
                    return SQL_RESULT_NO_DATA;

                const meta::PrimaryKeyMeta& currentColumn = *cursor;

                switch (columnIdx)
                {
                    case TABLE_CAT:
                    {
                        buffer.PutString(currentColumn.GetCatalogName());
                        break;
                    }

                    case TABLE_SCHEM:
                    {
                        buffer.PutString(currentColumn.GetSchemaName());
                        break;
                    }

                    case TABLE_NAME:
                    {
                        buffer.PutString(currentColumn.GetTableName());
                        break;
                    }

                    case COLUMN_NAME:
                    {
                        buffer.PutString(currentColumn.GetColumnName());
                        break;
                    }

                    case KEY_SEQ:
                    {
                        buffer.PutInt16(currentColumn.GetKeySeq());
                        break;
                    }

                    case PK_NAME:
                    {
                        buffer.PutString(currentColumn.GetKeyName());
                        break;
                    }

                    default:
                        break;
                }

                return SQL_RESULT_SUCCESS;
            }

            SqlResult PrimaryKeysQuery::Close()
            {
                meta.clear();

                executed = false;

                return SQL_RESULT_SUCCESS;
            }

            bool PrimaryKeysQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            int64_t PrimaryKeysQuery::AffectedRows() const
            {
                return 0;
            }
        }
    }
}

