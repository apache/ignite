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

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/type_traits.h"

namespace
{
    /** Default display size. */
    enum { DEFAULT_DISPLAY_SIZE = 34 };

    /** Default variable size data display size. */
    enum { DEFAULT_VARDATA_DISPLAY_SIZE = 64 };
}

namespace ignite
{
    namespace odbc
    {
        namespace type_traits
        {
            const std::string SqlTypeName::VARCHAR("LONG VARCHAR");

            const std::string SqlTypeName::SMALLINT("SMALLINT");

            const std::string SqlTypeName::INTEGER("INTEGER");

            const std::string SqlTypeName::DECIMAL("DECIMAL");

            const std::string SqlTypeName::FLOAT("FLOAT");

            const std::string SqlTypeName::DOUBLE("DOUBLE");

            const std::string SqlTypeName::BIT("BIT");

            const std::string SqlTypeName::TINYINT("TINYINT");

            const std::string SqlTypeName::BIGINT("BIGINT");

            const std::string SqlTypeName::BINARY("LONG VARBINARY");

            const std::string SqlTypeName::DATE("DATE");

            const std::string SqlTypeName::TIMESTAMP("TIMESTAMP");

            const std::string SqlTypeName::GUID("GUID");

#ifdef ODBC_DEBUG

#define DBG_STR_CASE(x) case x: return #x

            const char* StatementAttrIdToString(long id)
            {
                switch (id)
                {
                    DBG_STR_CASE(SQL_ATTR_APP_PARAM_DESC);
                    DBG_STR_CASE(SQL_ATTR_APP_ROW_DESC);
                    DBG_STR_CASE(SQL_ATTR_ASYNC_ENABLE);
                    DBG_STR_CASE(SQL_ATTR_CONCURRENCY);
                    DBG_STR_CASE(SQL_ATTR_CURSOR_SCROLLABLE);
                    DBG_STR_CASE(SQL_ATTR_CURSOR_SENSITIVITY);
                    DBG_STR_CASE(SQL_ATTR_CURSOR_TYPE);
                    DBG_STR_CASE(SQL_ATTR_ENABLE_AUTO_IPD);
                    DBG_STR_CASE(SQL_ATTR_FETCH_BOOKMARK_PTR);
                    DBG_STR_CASE(SQL_ATTR_IMP_PARAM_DESC);
                    DBG_STR_CASE(SQL_ATTR_IMP_ROW_DESC);
                    DBG_STR_CASE(SQL_ATTR_KEYSET_SIZE);
                    DBG_STR_CASE(SQL_ATTR_MAX_LENGTH);
                    DBG_STR_CASE(SQL_ATTR_MAX_ROWS);
                    DBG_STR_CASE(SQL_ATTR_METADATA_ID);
                    DBG_STR_CASE(SQL_ATTR_NOSCAN);
                    DBG_STR_CASE(SQL_ATTR_PARAM_BIND_OFFSET_PTR);
                    DBG_STR_CASE(SQL_ATTR_PARAM_BIND_TYPE);
                    DBG_STR_CASE(SQL_ATTR_PARAM_OPERATION_PTR);
                    DBG_STR_CASE(SQL_ATTR_PARAM_STATUS_PTR);
                    DBG_STR_CASE(SQL_ATTR_PARAMS_PROCESSED_PTR);
                    DBG_STR_CASE(SQL_ATTR_PARAMSET_SIZE);
                    DBG_STR_CASE(SQL_ATTR_QUERY_TIMEOUT);
                    DBG_STR_CASE(SQL_ATTR_RETRIEVE_DATA);
                    DBG_STR_CASE(SQL_ATTR_ROW_ARRAY_SIZE);
                    DBG_STR_CASE(SQL_ATTR_ROW_BIND_OFFSET_PTR);
                    DBG_STR_CASE(SQL_ATTR_ROW_BIND_TYPE);
                    DBG_STR_CASE(SQL_ATTR_ROW_NUMBER);
                    DBG_STR_CASE(SQL_ATTR_ROW_OPERATION_PTR);
                    DBG_STR_CASE(SQL_ATTR_ROW_STATUS_PTR);
                    DBG_STR_CASE(SQL_ATTR_ROWS_FETCHED_PTR);
                    DBG_STR_CASE(SQL_ATTR_SIMULATE_CURSOR);
                    DBG_STR_CASE(SQL_ATTR_USE_BOOKMARKS);
                default:
                    break;
                }
                return "<< UNKNOWN ID >>";
            }

#undef DBG_STR_CASE
#endif

            const std::string& BinaryTypeToSqlTypeName(int8_t binaryType)
            {
                using namespace ignite::impl::binary;

                switch (binaryType)
                {
                case IGNITE_TYPE_STRING:
                    return SqlTypeName::VARCHAR;

                case IGNITE_TYPE_SHORT:
                    return SqlTypeName::SMALLINT;

                case IGNITE_TYPE_INT:
                    return SqlTypeName::INTEGER;

                case IGNITE_TYPE_DECIMAL:
                    return SqlTypeName::DECIMAL;

                case IGNITE_TYPE_FLOAT:
                    return SqlTypeName::FLOAT;

                case IGNITE_TYPE_DOUBLE:
                    return SqlTypeName::DOUBLE;

                case IGNITE_TYPE_BOOL:
                    return SqlTypeName::BIT;

                case IGNITE_TYPE_BYTE:
                case IGNITE_TYPE_CHAR:
                    return SqlTypeName::TINYINT;

                case IGNITE_TYPE_LONG:
                    return SqlTypeName::BIGINT;

                case IGNITE_TYPE_UUID:
                    return SqlTypeName::GUID;

                case IGNITE_TYPE_DATE:
                    return SqlTypeName::DATE;

                case IGNITE_TYPE_TIMESTAMP:
                    return SqlTypeName::TIMESTAMP;

                case IGNITE_TYPE_OBJECT:
                case IGNITE_TYPE_ARRAY_BYTE:
                case IGNITE_TYPE_ARRAY_SHORT:
                case IGNITE_TYPE_ARRAY_INT:
                case IGNITE_TYPE_ARRAY_LONG:
                case IGNITE_TYPE_ARRAY_FLOAT:
                case IGNITE_TYPE_ARRAY_DOUBLE:
                case IGNITE_TYPE_ARRAY_CHAR:
                case IGNITE_TYPE_ARRAY_BOOL:
                case IGNITE_TYPE_ARRAY_DECIMAL:
                case IGNITE_TYPE_ARRAY_STRING:
                case IGNITE_TYPE_ARRAY_UUID:
                case IGNITE_TYPE_ARRAY_DATE:
                case IGNITE_TYPE_ARRAY_TIMESTAMP:
                case IGNITE_TYPE_ARRAY:
                case IGNITE_TYPE_COLLECTION:
                case IGNITE_TYPE_MAP:
                case IGNITE_TYPE_MAP_ENTRY:
                case IGNITE_TYPE_BINARY:
                default:
                    return SqlTypeName::BINARY;
                }

                return SqlTypeName::BINARY;
            }

            bool IsApplicationTypeSupported(int16_t type)
            {
                return ToDriverType(type) != IGNITE_ODBC_C_TYPE_UNSUPPORTED;
            }

            bool IsSqlTypeSupported(int16_t type)
            {
                switch (type)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                    case SQL_SMALLINT:
                    case SQL_INTEGER:
                    case SQL_FLOAT:
                    case SQL_DOUBLE:
                    case SQL_BIT:
                    case SQL_TINYINT:
                    case SQL_BIGINT:
                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                    case SQL_GUID:
                    case SQL_DECIMAL:
                    case SQL_TYPE_DATE:
                    case SQL_TYPE_TIMESTAMP:
                        return true;

                    case SQL_WCHAR:
                    case SQL_WVARCHAR:
                    case SQL_WLONGVARCHAR:
                    case SQL_REAL:
                    case SQL_NUMERIC:
                    case SQL_TYPE_TIME:
                    case SQL_INTERVAL_MONTH:
                    case SQL_INTERVAL_YEAR:
                    case SQL_INTERVAL_YEAR_TO_MONTH:
                    case SQL_INTERVAL_DAY:
                    case SQL_INTERVAL_HOUR:
                    case SQL_INTERVAL_MINUTE:
                    case SQL_INTERVAL_SECOND:
                    case SQL_INTERVAL_DAY_TO_HOUR:
                    case SQL_INTERVAL_DAY_TO_MINUTE:
                    case SQL_INTERVAL_DAY_TO_SECOND:
                    case SQL_INTERVAL_HOUR_TO_MINUTE:
                    case SQL_INTERVAL_HOUR_TO_SECOND:
                    case SQL_INTERVAL_MINUTE_TO_SECOND:
                    default:
                        return false;
                }
            }

            int8_t SqlTypeToBinary(int16_t sqlType)
            {
                using namespace ignite::impl::binary;

                switch (sqlType)
                {
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR:
                        return IGNITE_TYPE_STRING;

                    case SQL_SMALLINT:
                        return IGNITE_TYPE_SHORT;

                    case SQL_TINYINT:
                        return IGNITE_TYPE_BYTE;

                    case SQL_INTEGER:
                        return IGNITE_TYPE_INT;

                    case SQL_BIGINT:
                        return IGNITE_TYPE_LONG;

                    case SQL_FLOAT:
                        return IGNITE_TYPE_FLOAT;

                    case SQL_DOUBLE:
                        return IGNITE_TYPE_DOUBLE;

                    case SQL_BIT:
                        return IGNITE_TYPE_BOOL;

                    case SQL_BINARY:
                    case SQL_VARBINARY:
                    case SQL_LONGVARBINARY:
                        return IGNITE_TYPE_BINARY;

                    case SQL_DECIMAL:
                        return IGNITE_TYPE_DECIMAL;

                    case SQL_GUID:
                        return IGNITE_TYPE_UUID;

                    case SQL_TYPE_DATE:
                        return IGNITE_TYPE_DATE;

                    case SQL_TYPE_TIMESTAMP:
                        return IGNITE_TYPE_TIMESTAMP;

                    default:
                        break;
                }

                return -1;
            }

            IgniteSqlType ToDriverType(int16_t type)
            {
                switch (type)
                {
                case SQL_C_CHAR:
                    return IGNITE_ODBC_C_TYPE_CHAR;

                case SQL_C_WCHAR:
                    return IGNITE_ODBC_C_TYPE_WCHAR;

                case SQL_C_SSHORT:
                case SQL_C_SHORT:
                    return IGNITE_ODBC_C_TYPE_SIGNED_SHORT;

                case SQL_C_USHORT:
                    return IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT;

                case SQL_C_SLONG:
                case SQL_C_LONG:
                    return IGNITE_ODBC_C_TYPE_SIGNED_LONG;

                case SQL_C_ULONG:
                    return IGNITE_ODBC_C_TYPE_UNSIGNED_LONG;

                case SQL_C_FLOAT:
                    return IGNITE_ODBC_C_TYPE_FLOAT;

                case SQL_C_DOUBLE:
                    return IGNITE_ODBC_C_TYPE_DOUBLE;

                case SQL_C_BIT:
                    return IGNITE_ODBC_C_TYPE_BIT;

                case SQL_C_STINYINT:
                case SQL_C_TINYINT:
                    return IGNITE_ODBC_C_TYPE_SIGNED_TINYINT;

                case SQL_C_UTINYINT:
                    return IGNITE_ODBC_C_TYPE_UNSIGNED_TINYINT;

                case SQL_C_SBIGINT:
                    return IGNITE_ODBC_C_TYPE_SIGNED_BIGINT;

                case SQL_C_UBIGINT:
                    return IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT;

                case SQL_C_BINARY:
                    return IGNITE_ODBC_C_TYPE_BINARY;

                case SQL_C_TYPE_DATE:
                    return IGNITE_ODBC_C_TYPE_TDATE;

                case SQL_C_TYPE_TIME:
                    return IGNITE_ODBC_C_TYPE_TTIME;

                case SQL_C_TYPE_TIMESTAMP:
                    return IGNITE_ODBC_C_TYPE_TTIMESTAMP;

                case SQL_C_NUMERIC:
                    return IGNITE_ODBC_C_TYPE_NUMERIC;

                case SQL_C_GUID:
                    return IGNITE_ODBC_C_TYPE_GUID;

                case SQL_C_DEFAULT:
                    return IGNITE_ODBC_C_TYPE_DEFAULT;

                default:
                    return IGNITE_ODBC_C_TYPE_UNSUPPORTED;
                }
            }

            int16_t BinaryToSqlType(int8_t binaryType)
            {
                using namespace ignite::impl::binary;
                switch (binaryType)
                {
                    case IGNITE_TYPE_BYTE:
                    case IGNITE_TYPE_CHAR:
                        return SQL_TINYINT;

                    case IGNITE_TYPE_SHORT:
                        return SQL_SMALLINT;

                    case IGNITE_TYPE_INT:
                        return SQL_INTEGER;

                    case IGNITE_TYPE_LONG:
                        return SQL_BIGINT;

                    case IGNITE_TYPE_FLOAT:
                        return SQL_FLOAT;

                    case IGNITE_TYPE_DOUBLE:
                        return SQL_DOUBLE;

                    case IGNITE_TYPE_BOOL:
                        return SQL_BIT;

                    case IGNITE_TYPE_DECIMAL:
                        return SQL_DECIMAL;

                    case IGNITE_TYPE_STRING:
                        return SQL_VARCHAR;

                    case IGNITE_TYPE_UUID:
                        return SQL_GUID;

                    case IGNITE_TYPE_DATE:
                        return SQL_TYPE_DATE;

                    case IGNITE_TYPE_TIMESTAMP:
                        return SQL_TYPE_TIMESTAMP;

                    case IGNITE_TYPE_ARRAY_BYTE:
                    case IGNITE_TYPE_ARRAY_SHORT:
                    case IGNITE_TYPE_ARRAY_INT:
                    case IGNITE_TYPE_ARRAY_LONG:
                    case IGNITE_TYPE_ARRAY_FLOAT:
                    case IGNITE_TYPE_ARRAY_DOUBLE:
                    case IGNITE_TYPE_ARRAY_CHAR:
                    case IGNITE_TYPE_ARRAY_BOOL:
                    case IGNITE_TYPE_ARRAY_DECIMAL:
                    case IGNITE_TYPE_ARRAY_STRING:
                    case IGNITE_TYPE_ARRAY_UUID:
                    case IGNITE_TYPE_ARRAY_DATE:
                    case IGNITE_TYPE_ARRAY:
                    case IGNITE_TYPE_COLLECTION:
                    case IGNITE_TYPE_MAP:
                    case IGNITE_TYPE_MAP_ENTRY:
                    case IGNITE_TYPE_BINARY:
                    case IGNITE_TYPE_OBJECT:
                    default:
                        return SQL_BINARY;
                }
            }

            int16_t BinaryTypeNullability(int8_t binaryType)
            {
                return SQL_NULLABLE_UNKNOWN;
            }

            int32_t SqlTypeDisplaySize(int16_t type)
            {
                switch (type)
                {
                    case SQL_VARCHAR:
                    case SQL_CHAR:
                    case SQL_WCHAR:
                    case SQL_BINARY:
                        return DEFAULT_VARDATA_DISPLAY_SIZE;

                    case SQL_BIT:
                        return 1;

                    case SQL_TINYINT:
                        return 4;

                    case SQL_SMALLINT:
                        return 6;

                    case SQL_INTEGER:
                        return 11;

                    case SQL_BIGINT:
                        return 20;

                    case SQL_REAL:
                        return 14;

                    case SQL_FLOAT:
                    case SQL_DOUBLE:
                        return 24;

                    case SQL_TYPE_DATE:
                        return 10;

                    case SQL_TYPE_TIME:
                        return 8;

                    case SQL_TYPE_TIMESTAMP:
                        return 19;

                    case SQL_GUID:
                        return 36;

                    case SQL_DECIMAL:
                    case SQL_NUMERIC:
                    default:
                        return DEFAULT_DISPLAY_SIZE;
                }
            }

            int32_t BinaryTypeDisplaySize(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeDisplaySize(sqlType);
            }

            int32_t SqlTypeColumnSize(int16_t type)
            {
                switch (type)
                {
                    case SQL_VARCHAR:
                    case SQL_CHAR:
                    case SQL_WCHAR:
                    case SQL_BINARY:
                        return DEFAULT_VARDATA_DISPLAY_SIZE;

                    case SQL_BIT:
                        return 1;

                    case SQL_TINYINT:
                        return 3;

                    case SQL_SMALLINT:
                        return 5;

                    case SQL_INTEGER:
                        return 10;

                    case SQL_BIGINT:
                        return 19;

                    case SQL_REAL:
                        return 7;

                    case SQL_FLOAT:
                    case SQL_DOUBLE:
                        return 15;

                    case SQL_TYPE_DATE:
                        return 10;

                    case SQL_TYPE_TIME:
                        return 8;

                    case SQL_TYPE_TIMESTAMP:
                        return 19;

                    case SQL_GUID:
                        return 36;

                    case SQL_DECIMAL:
                    case SQL_NUMERIC:
                    default:
                        return DEFAULT_DISPLAY_SIZE;
                }
            }

            int32_t BinaryTypeColumnSize(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeColumnSize(sqlType);
            }

            int32_t SqlTypeTransferLength(int16_t type)
            {
                switch (type)
                {
                    case SQL_VARCHAR:
                    case SQL_CHAR:
                    case SQL_WCHAR:
                    case SQL_BINARY:
                        return DEFAULT_VARDATA_DISPLAY_SIZE;

                    case SQL_BIT:
                    case SQL_TINYINT:
                        return 1;

                    case SQL_SMALLINT:
                        return 2;

                    case SQL_INTEGER:
                        return 4;

                    case SQL_BIGINT:
                        return 8;

                    case SQL_REAL:
                    case SQL_FLOAT:
                        return 4;

                    case SQL_DOUBLE:
                        return 8;

                    case SQL_TYPE_DATE:
                    case SQL_TYPE_TIME:
                        return 6;

                    case SQL_TYPE_TIMESTAMP:
                        return 16;

                    case SQL_GUID:
                        return 16;

                    case SQL_DECIMAL:
                    case SQL_NUMERIC:
                    default:
                        return DEFAULT_DISPLAY_SIZE;
                }
            }

            int32_t BinaryTypeTransferLength(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeTransferLength(sqlType);
            }

            int32_t SqlTypeNumPrecRadix(int16_t type)
            {
                switch (type)
                {
                    case SQL_REAL:
                    case SQL_FLOAT:
                    case SQL_DOUBLE:
                        return 2;

                    case SQL_BIT:
                    case SQL_TINYINT:
                    case SQL_SMALLINT:
                    case SQL_INTEGER:
                    case SQL_BIGINT:
                        return 10;

                    default:
                        return 0;
                }
            }

            int32_t BinaryTypeNumPrecRadix(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeNumPrecRadix(sqlType);
            }

            int32_t SqlTypeDecimalDigits(int16_t type)
            {
                // Not implemented for the NUMERIC and DECIMAL data types.
                return -1;
            }

            int32_t BinaryTypeDecimalDigits(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeDecimalDigits(sqlType);
            }

            bool SqlTypeUnsigned(int16_t type)
            {
                switch (type)
                {
                    case SQL_BIT:
                    case SQL_TINYINT:
                    case SQL_SMALLINT:
                    case SQL_INTEGER:
                    case SQL_BIGINT:
                    case SQL_REAL:
                    case SQL_FLOAT:
                    case SQL_DOUBLE:
                        return false;

                    default:
                        return true;
                }
            }

            bool BinaryTypeUnsigned(int8_t type)
            {
                int16_t sqlType = BinaryToSqlType(type);

                return SqlTypeUnsigned(sqlType);
            }
        }
    }
}

