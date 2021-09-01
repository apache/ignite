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
#include "ignite/odbc/odbc_error.h"
#include "ignite/odbc/query/column_metadata_query.h"

namespace
{
    struct ResultColumn
    {
        enum Type
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

            /** Data source-dependent data type name. */
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
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            ColumnMetadataQuery::ColumnMetadataQuery(diagnostic::DiagnosableAdapter& diag,
                Connection& connection, const std::string& schema,
                const std::string& table, const std::string& column) :
                Query(diag, QueryType::COLUMN_METADATA),
                connection(connection),
                schema(schema),
                table(table),
                column(column),
                executed(false),
                fetched(false),
                meta(),
                columnsMeta()
            {
                using namespace ignite::impl::binary;
                using namespace ignite::odbc::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(12);

                const std::string sch;
                const std::string tbl;

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_CAT",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_SCHEM",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TABLE_NAME",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "TYPE_NAME",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_SIZE",    IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "BUFFER_LENGTH",  IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DECIMAL_DIGITS", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NUM_PREC_RADIX", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NULLABLE",       IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "REMARKS",        IGNITE_TYPE_STRING));
            }

            ColumnMetadataQuery::~ColumnMetadataQuery()
            {
                // No-op.
            }

            SqlResult::Type ColumnMetadataQuery::Execute()
            {
                if (executed)
                    Close();

                SqlResult::Type result = MakeRequestGetColumnsMeta();

                if (result == SqlResult::AI_SUCCESS)
                {
                    executed = true;
                    fetched = false;

                    cursor = meta.begin();
                }

                return result;
            }

            const meta::ColumnMetaVector* ColumnMetadataQuery::GetMeta()
            {
                return &columnsMeta;
            }

            SqlResult::Type ColumnMetadataQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                if (!fetched)
                    fetched = true;
                else
                    ++cursor;

                if (cursor == meta.end())
                    return SqlResult::AI_NO_DATA;

                app::ColumnBindingMap::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    GetColumn(it->first, it->second);

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type ColumnMetadataQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer & buffer)
            {
                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                if (cursor == meta.end())
                {
                    diag.AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return SqlResult::AI_ERROR;
                }

                const meta::ColumnMeta& currentColumn = *cursor;
                uint8_t columnType = currentColumn.GetDataType();

                switch (columnIdx)
                {
                    case ResultColumn::TABLE_CAT:
                    {
                        buffer.PutNull();
                        break;
                    }

                    case ResultColumn::TABLE_SCHEM:
                    {
                        buffer.PutString(currentColumn.GetSchemaName());
                        break;
                    }

                    case ResultColumn::TABLE_NAME:
                    {
                        buffer.PutString(currentColumn.GetTableName());
                        break;
                    }

                    case ResultColumn::COLUMN_NAME:
                    {
                        buffer.PutString(currentColumn.GetColumnName());
                        break;
                    }

                    case ResultColumn::DATA_TYPE:
                    {
                        buffer.PutInt16(type_traits::BinaryToSqlType(columnType));
                        break;
                    }

                    case ResultColumn::TYPE_NAME:
                    {
                        buffer.PutString(type_traits::BinaryTypeToSqlTypeName(currentColumn.GetDataType()));
                        break;
                    }

                    case ResultColumn::COLUMN_SIZE:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeColumnSize(columnType));
                        break;
                    }

                    case ResultColumn::BUFFER_LENGTH:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeTransferLength(columnType));
                        break;
                    }

                    case ResultColumn::DECIMAL_DIGITS:
                    {
                        int32_t decDigits = type_traits::BinaryTypeDecimalDigits(columnType);
                        if (decDigits < 0)
                            buffer.PutNull();
                        else
                            buffer.PutInt16(static_cast<int16_t>(decDigits));
                        break;
                    }

                    case ResultColumn::NUM_PREC_RADIX:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeNumPrecRadix(columnType));
                        break;
                    }

                    case ResultColumn::NULLABLE:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeNullability(columnType));
                        break;
                    }

                    case ResultColumn::REMARKS:
                    {
                        buffer.PutNull();
                        break;
                    }

                    default:
                        break;
                }

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type ColumnMetadataQuery::Close()
            {
                meta.clear();

                executed = false;

                return SqlResult::AI_SUCCESS;
            }

            bool ColumnMetadataQuery::DataAvailable() const
            {
                return cursor != meta.end();
            }

            int64_t ColumnMetadataQuery::AffectedRows() const
            {
                return 0;
            }

            SqlResult::Type ColumnMetadataQuery::NextResultSet()
            {
                return SqlResult::AI_NO_DATA;
            }

            SqlResult::Type ColumnMetadataQuery::MakeRequestGetColumnsMeta()
            {
                QueryGetColumnsMetaRequest req(schema, table, column);
                QueryGetColumnsMetaResponse rsp;

                try
                {
                    connection.SyncMessage(req, rsp);
                }
                catch (const OdbcError& err)
                {
                    diag.AddStatusRecord(err);

                    return SqlResult::AI_ERROR;
                }
                catch (const IgniteError& err)
                {
                    diag.AddStatusRecord(err.GetText());

                    return SqlResult::AI_ERROR;
                }

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                {
                    LOG_MSG("Error: " << rsp.GetError());
                    diag.AddStatusRecord(ResponseStatusToSqlState(rsp.GetStatus()), rsp.GetError());

                    return SqlResult::AI_ERROR;
                }

                meta = rsp.GetMeta();

                for (size_t i = 0; i < meta.size(); ++i)
                {
                    LOG_MSG("\n[" << i << "] SchemaName:     " << meta[i].GetSchemaName()
                         << "\n[" << i << "] TableName:      " << meta[i].GetTableName()
                         << "\n[" << i << "] ColumnName:     " << meta[i].GetColumnName()
                         << "\n[" << i << "] ColumnType:     " << static_cast<int32_t>(meta[i].GetDataType()));
                }

                return SqlResult::AI_SUCCESS;
            }
        }
    }
}

