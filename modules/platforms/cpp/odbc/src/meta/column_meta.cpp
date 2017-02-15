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

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/type_traits.h"
#include "ignite/odbc/common_types.h"

namespace ignite
{
    namespace odbc
    {
        namespace meta
        {

#define DBG_STR_CASE(x) case x: return #x

            const char* ColumnMeta::AttrIdToString(uint16_t id)
            {
                switch (id)
                {
                    DBG_STR_CASE(SQL_DESC_LABEL);
                    DBG_STR_CASE(SQL_DESC_BASE_COLUMN_NAME);
                    DBG_STR_CASE(SQL_DESC_NAME);
                    DBG_STR_CASE(SQL_DESC_TABLE_NAME);
                    DBG_STR_CASE(SQL_DESC_BASE_TABLE_NAME);
                    DBG_STR_CASE(SQL_DESC_SCHEMA_NAME);
                    DBG_STR_CASE(SQL_DESC_CATALOG_NAME);
                    DBG_STR_CASE(SQL_DESC_LITERAL_PREFIX);
                    DBG_STR_CASE(SQL_DESC_LITERAL_SUFFIX);
                    DBG_STR_CASE(SQL_DESC_TYPE_NAME);
                    DBG_STR_CASE(SQL_DESC_LOCAL_TYPE_NAME);
                    DBG_STR_CASE(SQL_DESC_FIXED_PREC_SCALE);
                    DBG_STR_CASE(SQL_DESC_AUTO_UNIQUE_VALUE);
                    DBG_STR_CASE(SQL_DESC_CASE_SENSITIVE);
                    DBG_STR_CASE(SQL_DESC_CONCISE_TYPE);
                    DBG_STR_CASE(SQL_DESC_TYPE);
                    DBG_STR_CASE(SQL_DESC_DISPLAY_SIZE);
                    DBG_STR_CASE(SQL_DESC_LENGTH);
                    DBG_STR_CASE(SQL_DESC_OCTET_LENGTH);
                    DBG_STR_CASE(SQL_DESC_NULLABLE);
                    DBG_STR_CASE(SQL_DESC_NUM_PREC_RADIX);
                    DBG_STR_CASE(SQL_DESC_PRECISION);
                    DBG_STR_CASE(SQL_DESC_SCALE);
                    DBG_STR_CASE(SQL_DESC_SEARCHABLE);
                    DBG_STR_CASE(SQL_DESC_UNNAMED);
                    DBG_STR_CASE(SQL_DESC_UNSIGNED);
                    DBG_STR_CASE(SQL_DESC_UPDATABLE);
                default:
                    break;
                }
                return "<< UNKNOWN ID >>";
            }

#undef DBG_STR_CASE

            void ColumnMeta::Read(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                utility::ReadString(reader, schemaName);
                utility::ReadString(reader, tableName);
                utility::ReadString(reader, columnName);

                dataType = reader.ReadInt8();
            }

            bool ColumnMeta::GetAttribute(uint16_t fieldId, std::string& value) const 
            {
                using namespace ignite::impl::binary;

                switch (fieldId)
                {
                    case SQL_DESC_LABEL:
                    case SQL_DESC_BASE_COLUMN_NAME:
                    case SQL_DESC_NAME:
                    {
                        value = columnName;

                        return true;
                    }

                    case SQL_DESC_TABLE_NAME:
                    case SQL_DESC_BASE_TABLE_NAME:
                    {
                        value = tableName;

                        return true;
                    }

                    case SQL_DESC_SCHEMA_NAME:
                    {
                        value = schemaName;

                        return true;
                    }

                    case SQL_DESC_CATALOG_NAME:
                    {
                        value.clear();

                        return true;
                    }

                    case SQL_DESC_LITERAL_PREFIX:
                    case SQL_DESC_LITERAL_SUFFIX:
                    {
                        if (dataType == IGNITE_TYPE_STRING)
                            value = "'";
                        else
                            value.clear();

                        return true;
                    }

                    case SQL_DESC_TYPE_NAME:
                    case SQL_DESC_LOCAL_TYPE_NAME:
                    {
                        value = type_traits::BinaryTypeToSqlTypeName(dataType);

                        return true;
                    }

                    default:
                        return false;
                }
            }

            bool ColumnMeta::GetAttribute(uint16_t fieldId, SqlLen& value) const
            {
                using namespace ignite::impl::binary;

                switch (fieldId)
                {
                    case SQL_DESC_FIXED_PREC_SCALE:
                    case SQL_DESC_AUTO_UNIQUE_VALUE:
                    {
                        value = SQL_FALSE;

                        return true;
                    }

                    case SQL_DESC_CASE_SENSITIVE:
                    {
                        if (dataType == IGNITE_TYPE_STRING)
                            value = SQL_TRUE;
                        else
                            value = SQL_FALSE;

                        return true;
                    }

                    case SQL_DESC_CONCISE_TYPE:
                    case SQL_DESC_TYPE:
                    {
                        value = type_traits::BinaryToSqlType(dataType);

                        return true;
                    }

                    case SQL_DESC_DISPLAY_SIZE:
                    {
                        value = type_traits::BinaryTypeDisplaySize(dataType);

                        return true;
                    }

                    case SQL_DESC_LENGTH:
                    case SQL_DESC_OCTET_LENGTH:
                    {
                        value = type_traits::BinaryTypeTransferLength(dataType);

                        return true;
                    }

                    case SQL_DESC_NULLABLE:
                    {
                        value = type_traits::BinaryTypeNullability(dataType);

                        return true;
                    }

                    case SQL_DESC_NUM_PREC_RADIX:
                    {
                        value = type_traits::BinaryTypeNumPrecRadix(dataType);

                        return true;
                    }

                    case SQL_DESC_PRECISION:
                    {
                        value = type_traits::BinaryTypeColumnSize(dataType);

                        return true;
                    }

                    case SQL_DESC_SCALE:
                    {
                        value = type_traits::BinaryTypeDecimalDigits(dataType);

                        if (value < 0)
                            value = 0;

                        return true;
                    }

                    case SQL_DESC_SEARCHABLE:
                    {
                        value = SQL_PRED_BASIC;

                        return true;
                    }

                    case SQL_DESC_UNNAMED:
                    {
                        value = columnName.empty() ? SQL_UNNAMED : SQL_NAMED;

                        return true;
                    }

                    case SQL_DESC_UNSIGNED:
                    {
                        value = type_traits::BinaryTypeUnsigned(dataType) ? SQL_TRUE : SQL_FALSE;

                        return true;
                    }

                    case SQL_DESC_UPDATABLE:
                    {
                        // We do not support update for now so just set all
                        // columns to readonly.
                        value = SQL_ATTR_READONLY;

                        return true;
                    }

                    default:
                        return false;
                }
            }

            void ReadColumnMetaVector(ignite::impl::binary::BinaryReaderImpl& reader, ColumnMetaVector& meta)
            {
                int32_t metaNum = reader.ReadInt32();

                meta.clear();
                meta.reserve(static_cast<size_t>(metaNum));

                for (int32_t i = 0; i < metaNum; ++i)
                {
                    meta.push_back(ColumnMeta());

                    meta.back().Read(reader);
                }
            }
        }
    }
}