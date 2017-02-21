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
#include "ignite/odbc/log.h"
#include "ignite/odbc/query/table_metadata_query.h"

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

        /** Table type. */
        TABLE_TYPE,

        /** A description of the column. */
        REMARKS
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            TableMetadataQuery::TableMetadataQuery(diagnostic::Diagnosable& diag,
                Connection& connection, const std::string& catalog,const std::string& schema,
                const std::string& table, const std::string& tableType) :
                Query(diag, TABLE_METADATA),
                connection(connection),
                catalog(catalog),
                schema(schema),
                table(table),
                tableType(tableType),
                executed(false),
                meta(),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::odbc::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(5);

                const std::string sch("");
                const std::string tbl("");

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_CAT",   IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_SCHEM", IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_NAME",  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_TYPE",  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "REMARKS",     IGNITE_TYPE_STRING));
            }

            TableMetadataQuery::~TableMetadataQuery()
            {
                // No-op.
            }

            SqlResult TableMetadataQuery::Execute()
            {
                if (executed)
                    Close();

                SqlResult result = MakeRequestGetTablesMeta();

                if (result == SQL_RESULT_SUCCESS)
                {
                    executed = true;

                    cursor = meta.begin();
                }

                return result;
            }

            const meta::ColumnMetaVector& TableMetadataQuery::GetMeta() const
            {
                return columnsMeta;
            }

            SqlResult TableMetadataQuery::FetchNextRow(app::ColumnBindingMap& columnBindings)
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

            SqlResult TableMetadataQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer & buffer)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SQL_STATE_HY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SQL_RESULT_ERROR;
                }

                if (cursor == meta.end())
                    return SQL_RESULT_NO_DATA;

                const meta::TableMeta& currentColumn = *cursor;

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

                    case TABLE_TYPE:
                    {
                        buffer.PutString(currentColumn.GetTableType());
                        break;
                    }

                    case REMARKS:
                    {
                        buffer.PutNull();
                        break;
                    }

                    default:
                        break;
                }

                return SQL_RESULT_SUCCESS;
            }

            SqlResult TableMetadataQuery::Close()
            {
                meta.clear();

                executed = false;

                return SQL_RESULT_SUCCESS;
            }

            bool TableMetadataQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            int64_t TableMetadataQuery::AffectedRows() const
            {
                return 0;
            }

            SqlResult TableMetadataQuery::MakeRequestGetTablesMeta()
            {
                QueryGetTablesMetaRequest req(catalog, schema, table, tableType);
                QueryGetTablesMetaResponse rsp;

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

                meta = rsp.GetMeta();

                for (size_t i = 0; i < meta.size(); ++i)
                {
                    LOG_MSG("\n[" << i << "] CatalogName: " << meta[i].GetCatalogName()
                         << "\n[" << i << "] SchemaName:  " << meta[i].GetSchemaName()
                         << "\n[" << i << "] TableName:   " << meta[i].GetTableName()
                         << "\n[" << i << "] TableType:   " << meta[i].GetTableType());
                }

                return SQL_RESULT_SUCCESS;
            }
        }
    }
}

