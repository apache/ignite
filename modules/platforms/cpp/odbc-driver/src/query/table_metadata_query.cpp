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
#include "ignite/odbc/query/table_metadata_query.h"

namespace
{
    enum ResultColumnName
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
            TableMetadataQuery::TableMetadataQuery(Connection& connection, const std::string& catalog,
                const std::string& schema, const std::string& table, const std::string& tableType) :
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
                using meta::ColumnMeta;

                columnsMeta.reserve(5);

                const std::string sch("");
                const std::string tbl("");

                const std::string varcharType("java.lang.String");

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_CAT",   varcharType, IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_SCHEM", varcharType, IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_NAME",  varcharType, IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_TYPE",  varcharType, IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "REMARKS",     varcharType, IGNITE_TYPE_STRING));
            }

            TableMetadataQuery::~TableMetadataQuery()
            {
                // No-op.
            }
            
            bool TableMetadataQuery::Execute()
            {
                bool success = MakeRequestGetTablesMeta();

                executed = success;

                if (success)
                    cursor = meta.begin();

                return success;
            }

            const meta::ColumnMetaVector& TableMetadataQuery::GetMeta() const
            {
                return columnsMeta;
            }

            SqlResult TableMetadataQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
            {
                if (!executed)
                    return SQL_RESULT_ERROR;

                if (cursor == meta.end())
                    return SQL_RESULT_NO_DATA;

                app::ColumnBindingMap::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                {
                    uint16_t columnIdx = it->first;
                    app::ApplicationDataBuffer& buffer = it->second;
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
                }

                ++cursor;

                return SQL_RESULT_SUCCESS;
            }

            bool TableMetadataQuery::Close()
            {
                meta.clear();

                executed = false;

                return true;
            }

            bool TableMetadataQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            bool TableMetadataQuery::MakeRequestGetTablesMeta()
            {
                QueryGetTablesMetaRequest req(catalog, schema, table, tableType);
                QueryGetTablesMetaResponse rsp;

                bool success = connection.SyncMessage(req, rsp);

                if (!success)
                    return false;

                if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
                {
                    LOG_MSG("Error: %s\n", rsp.GetError().c_str());

                    return false;
                }

                meta = rsp.GetMeta();

                for (int i = 0; i < meta.size(); ++i)
                {
                    LOG_MSG("[%d] CatalogName: %s\n", i, meta[i].GetCatalogName().c_str());
                    LOG_MSG("[%d] SchemaName:  %s\n", i, meta[i].GetSchemaName().c_str());
                    LOG_MSG("[%d] TableName:   %s\n", i, meta[i].GetTableName().c_str());
                    LOG_MSG("[%d] TableType:   %s\n", i, meta[i].GetTableType().c_str());
                    LOG_MSG("\n");
                }

                return true;
            }

            bool TableMetadataQuery::SpecialSemanticsAllCatalogs()
            {
                return catalog == "%" &&
                       schema.empty() &&
                       table.empty();
            }

            bool TableMetadataQuery::SpecialSemanticsAllSchemas()
            {
                return schema == "%" &&
                       catalog.empty() &&
                       table.empty();
            }

            bool TableMetadataQuery::SpecialSemanticsAllTableTypes()
            {
                return tableType == "%" &&
                       catalog.empty() &&
                       schema.empty() &&
                       table.empty();
            }
        }
    }
}

