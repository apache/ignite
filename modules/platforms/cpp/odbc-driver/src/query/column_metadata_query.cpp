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
#include "ignite/odbc/query/column_metadata_query.h"

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

        /** Column name. */
        COLUMN_NAME,

        /** SQL data type. */
        DATA_TYPE,

        /** Data source–dependent data type name. */
        TYPE_NAME,

        /** Column size. */
        COLUMN_SIZE,

        /** The length in bytes of data transferred on fetch. */
        BUFFER_LENGTH,

        /** The total number of significant digits to the right of the decimal point. */
        DECIMAL_DIGITS,

        /** Precision. */
        NUM_PREC_RADIX,

        /** Nullability of the data in column. */
        NULLABLE,

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
            ColumnMetadataQuery::ColumnMetadataQuery(Connection& connection, const std::string& schema,
                                         const std::string& table, const std::string& column) :
                connection(connection),
                schema(schema),
                table(table),
                column(column),
                executed(false),
                meta(),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using meta::ColumnMeta;

                columnsMeta.reserve(12);

                const std::string sch("");
                const std::string tbl("");

                const std::string varcharType("java.lang.String");
                const std::string smallintType("java.lang.Short");
                const std::string integerType("java.lang.Integer");

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_CAT",      varcharType,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_SCHEM",    varcharType,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_NAME",     varcharType,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_NAME",    varcharType,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DATA_TYPE",      smallintType, IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TYPE_NAME",      varcharType,  IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_SIZE",    integerType,  IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "BUFFER_LENGTH",  integerType,  IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DECIMAL_DIGITS", smallintType, IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NUM_PREC_RADIX", smallintType, IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NULLABLE",       smallintType, IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "REMARKS",        varcharType,  IGNITE_TYPE_STRING));
            }

            ColumnMetadataQuery::~ColumnMetadataQuery()
            {
                // No-op.
            }
            
            bool ColumnMetadataQuery::Execute()
            {
                bool success = MakeRequestGetColumnsMeta();

                executed = success;

                if (success)
                    cursor = meta.begin();

                return success;
            }

            const meta::ColumnMetaVector& ColumnMetadataQuery::GetMeta() const
            {
                return columnsMeta;
            }

            SqlResult ColumnMetadataQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
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
                    const meta::ColumnMeta& currentColumn = *cursor;
                    uint8_t columnType = currentColumn.GetDataType();

                    switch (columnIdx)
                    {
                        case TABLE_CAT:
                        {
                            buffer.PutNull();
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

                        case DATA_TYPE:
                        {
                            buffer.PutInt16(type_traits::BinaryToSqlType(columnType));
                            break;
                        }

                        case TYPE_NAME:
                        {
                            buffer.PutString(currentColumn.GetColumnTypeName());
                            break;
                        }

                        case COLUMN_SIZE:
                        {
                            buffer.PutInt16(type_traits::BinaryTypeColumnSize(columnType));
                            break;
                        }

                        case BUFFER_LENGTH:
                        {
                            buffer.PutInt16(type_traits::BinaryTypeTransferLength(columnType));
                            break;
                        }

                        case DECIMAL_DIGITS:
                        {
                            int32_t decDigits = type_traits::BinaryTypeDecimalDigits(columnType);
                            if (decDigits < 0)
                                buffer.PutNull();
                            else
                                buffer.PutInt16(static_cast<int16_t>(decDigits));
                            break;
                        }

                        case NUM_PREC_RADIX:
                        {
                            buffer.PutInt16(type_traits::BinaryTypeNumPrecRadix(columnType));
                            break;
                        }

                        case NULLABLE:
                        {
                            buffer.PutInt16(type_traits::BinaryTypeNullability(columnType));
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

            bool ColumnMetadataQuery::Close()
            {
                meta.clear();

                executed = false;

                return true;
            }

            bool ColumnMetadataQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            int64_t ColumnMetadataQuery::AffectedRows() const
            {
                return 0;
            }

            bool ColumnMetadataQuery::MakeRequestGetColumnsMeta()
            {
                QueryGetColumnsMetaRequest req(schema, table, column);
                QueryGetColumnsMetaResponse rsp;

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
                    LOG_MSG("[%d] SchemaName:     %s\n", i, meta[i].GetSchemaName().c_str());
                    LOG_MSG("[%d] TableName:      %s\n", i, meta[i].GetTableName().c_str());
                    LOG_MSG("[%d] ColumnName:     %s\n", i, meta[i].GetColumnName().c_str());
                    LOG_MSG("[%d] ColumnTypeName: %s\n", i, meta[i].GetColumnTypeName().c_str());
                    LOG_MSG("\n");
                }

                return true;
            }
        }
    }
}

