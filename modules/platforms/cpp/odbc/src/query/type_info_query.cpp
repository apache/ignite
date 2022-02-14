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

#include <cassert>

#include <ignite/impl/binary/binary_common.h>

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/query/type_info_query.h"

namespace
{
    struct ResultColumn
    {
        enum Type
        {
            /** Data source-dependent data-type name. */
            TYPE_NAME = 1,

            /** SQL data type. */
            DATA_TYPE,

            /** The maximum column size that the server supports for this data type. */
            COLUMN_SIZE,

            /** Character or characters used to prefix a literal. */
            LITERAL_PREFIX,

            /** Character or characters used to terminate a literal. */
            LITERAL_SUFFIX,

            /**
             * A list of keywords, separated by commas, corresponding to each
             * parameter that the application may specify in parentheses when using
             * the name that is returned in the TYPE_NAME field.
             */
            CREATE_PARAMS,

            /** Whether the data type accepts a NULL value. */
            NULLABLE,

            /**
             * Whether a character data type is case-sensitive in collations and
             * comparisons.
             */
            CASE_SENSITIVE,

            /** How the data type is used in a WHERE clause. */
            SEARCHABLE,

            /** Whether the data type is unsigned. */
            UNSIGNED_ATTRIBUTE,

            /** Whether the data type has predefined fixed precision and scale. */
            FIXED_PREC_SCALE,

            /** Whether the data type is auto-incrementing. */
            AUTO_UNIQUE_VALUE,

            /**
             * Localized version of the data source–dependent name of the data
             * type.
             */
            LOCAL_TYPE_NAME,

            /** The minimum scale of the data type on the data source. */
            MINIMUM_SCALE,

            /** The maximum scale of the data type on the data source. */
            MAXIMUM_SCALE,

            /**
             * The value of the SQL data type as it appears in the SQL_DESC_TYPE
             * field of the descriptor.
             */
            SQL_DATA_TYPE,

            /**
             * When the value of SQL_DATA_TYPE is SQL_DATETIME or SQL_INTERVAL,
             * this column contains the datetime/interval sub-code.
             */
            SQL_DATETIME_SUB,

            /**
             * If the data type is an approximate numeric type, this column
             * contains the value 2 to indicate that COLUMN_SIZE specifies a number
             * of bits.
             */
            NUM_PREC_RADIX,

            /**
             * If the data type is an interval data type, then this column contains
             * the value of the interval leading precision.
             */
            INTERVAL_PRECISION
        };
    };
}

namespace ignite
{
    namespace odbc
    {
        namespace query
        {
            TypeInfoQuery::TypeInfoQuery(diagnostic::DiagnosableAdapter& diag, int16_t sqlType) :
                Query(diag, QueryType::TYPE_INFO),
                columnsMeta(),
                executed(false),
                fetched(false),
                types(),
                cursor(types.end())
            {
                using namespace ignite::impl::binary;
                using namespace ignite::odbc::type_traits;

                using meta::ColumnMeta;

                columnsMeta.reserve(19);

                const std::string sch;
                const std::string tbl;

                columnsMeta.push_back(ColumnMeta(sch, tbl, "TYPE_NAME",          IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "DATA_TYPE",          IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "COLUMN_SIZE",        IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LITERAL_PREFIX",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LITERAL_SUFFIX",     IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "CREATE_PARAMS",      IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NULLABLE",           IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "CASE_SENSITIVE",     IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SEARCHABLE",         IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "UNSIGNED_ATTRIBUTE", IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "FIXED_PREC_SCALE",   IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "AUTO_UNIQUE_VALUE",  IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "LOCAL_TYPE_NAME",    IGNITE_TYPE_STRING));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "MINIMUM_SCALE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "MAXIMUM_SCALE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SQL_DATA_TYPE",      IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "SQL_DATETIME_SUB",   IGNITE_TYPE_SHORT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "NUM_PREC_RADIX",     IGNITE_TYPE_INT));
                columnsMeta.push_back(ColumnMeta(sch, tbl, "INTERVAL_PRECISION", IGNITE_TYPE_SHORT));

                assert(IsSqlTypeSupported(sqlType) || sqlType == SQL_ALL_TYPES);

                if (sqlType == SQL_ALL_TYPES)
                {
                    types.push_back(IGNITE_TYPE_STRING);
                    types.push_back(IGNITE_TYPE_SHORT);
                    types.push_back(IGNITE_TYPE_INT);
                    types.push_back(IGNITE_TYPE_DECIMAL);
                    types.push_back(IGNITE_TYPE_FLOAT);
                    types.push_back(IGNITE_TYPE_DOUBLE);
                    types.push_back(IGNITE_TYPE_BOOL);
                    types.push_back(IGNITE_TYPE_BYTE);
                    types.push_back(IGNITE_TYPE_LONG);
                    types.push_back(IGNITE_TYPE_UUID);
                    types.push_back(IGNITE_TYPE_BINARY);
                }
                else
                    types.push_back(SqlTypeToBinary(sqlType));
            }

            TypeInfoQuery::~TypeInfoQuery()
            {
                // No-op.
            }

            SqlResult::Type TypeInfoQuery::Execute()
            {
                cursor = types.begin();

                executed = true;
                fetched = false;

                return SqlResult::AI_SUCCESS;
            }

            const meta::ColumnMetaVector* TypeInfoQuery::GetMeta()
            {
                return &columnsMeta;
            }

            SqlResult::Type TypeInfoQuery::FetchNextRow(app::ColumnBindingMap & columnBindings)
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

                if (cursor == types.end())
                    return SqlResult::AI_NO_DATA;

                app::ColumnBindingMap::iterator it;

                for (it = columnBindings.begin(); it != columnBindings.end(); ++it)
                    GetColumn(it->first, it->second);

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type TypeInfoQuery::GetColumn(uint16_t columnIdx, app::ApplicationDataBuffer & buffer)
            {
                using namespace ignite::impl::binary;

                if (!executed)
                {
                    diag.AddStatusRecord(SqlState::SHY010_SEQUENCE_ERROR, "Query was not executed.");

                    return SqlResult::AI_ERROR;
                }

                if (cursor == types.end())
                {
                    diag.AddStatusRecord(SqlState::S24000_INVALID_CURSOR_STATE,
                        "Cursor has reached end of the result set.");

                    return SqlResult::AI_ERROR;
                }

                int8_t currentType = *cursor;

                switch (columnIdx)
                {
                    case ResultColumn::TYPE_NAME:
                    {
                        buffer.PutString(type_traits::BinaryTypeToSqlTypeName(currentType));

                        break;
                    }

                    case ResultColumn::DATA_TYPE:
                    case ResultColumn::SQL_DATA_TYPE:
                    {
                        buffer.PutInt16(type_traits::BinaryToSqlType(currentType));

                        break;
                    }

                    case ResultColumn::COLUMN_SIZE:
                    {
                        buffer.PutInt32(type_traits::BinaryTypeColumnSize(currentType));

                        break;
                    }

                    case ResultColumn::LITERAL_PREFIX:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.PutString("'");
                        else if (currentType == IGNITE_TYPE_BINARY)
                            buffer.PutString("0x");
                        else
                            buffer.PutNull();

                        break;
                    }

                    case ResultColumn::LITERAL_SUFFIX:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.PutString("'");
                        else
                            buffer.PutNull();

                        break;
                    }

                    case ResultColumn::CREATE_PARAMS:
                    {
                        buffer.PutNull();

                        break;
                    }

                    case ResultColumn::NULLABLE:
                    {
                        buffer.PutInt32(type_traits::BinaryTypeNullability(currentType));

                        break;
                    }

                    case ResultColumn::CASE_SENSITIVE:
                    {
                        if (currentType == IGNITE_TYPE_STRING)
                            buffer.PutInt16(SQL_TRUE);
                        else
                            buffer.PutInt16(SQL_FALSE);

                        break;
                    }

                    case ResultColumn::SEARCHABLE:
                    {
                        buffer.PutInt16(SQL_SEARCHABLE);

                        break;
                    }

                    case ResultColumn::UNSIGNED_ATTRIBUTE:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeUnsigned(currentType));

                        break;
                    }

                    case ResultColumn::FIXED_PREC_SCALE:
                    case ResultColumn::AUTO_UNIQUE_VALUE:
                    {
                        buffer.PutInt16(SQL_FALSE);

                        break;
                    }

                    case ResultColumn::LOCAL_TYPE_NAME:
                    {
                        buffer.PutNull();

                        break;
                    }

                    case ResultColumn::MINIMUM_SCALE:
                    case ResultColumn::MAXIMUM_SCALE:
                    {
                        buffer.PutInt16(type_traits::BinaryTypeDecimalDigits(currentType));

                        break;
                    }

                    case ResultColumn::SQL_DATETIME_SUB:
                    {
                        buffer.PutNull();

                        break;
                    }

                    case ResultColumn::NUM_PREC_RADIX:
                    {
                        buffer.PutInt32(type_traits::BinaryTypeNumPrecRadix(currentType));

                        break;
                    }

                    case ResultColumn::INTERVAL_PRECISION:
                    {
                        buffer.PutNull();

                        break;
                    }

                    default:
                        break;
                }

                return SqlResult::AI_SUCCESS;
            }

            SqlResult::Type TypeInfoQuery::Close()
            {
                cursor = types.end();

                executed = false;

                return SqlResult::AI_SUCCESS;
            }

            bool TypeInfoQuery::DataAvailable() const
            {
                return cursor != types.end();
            }

            int64_t TypeInfoQuery::AffectedRows() const
            {
                return 0;
            }

            SqlResult::Type TypeInfoQuery::NextResultSet()
            {
                return SqlResult::AI_NO_DATA;
            }
        }
    }
}

