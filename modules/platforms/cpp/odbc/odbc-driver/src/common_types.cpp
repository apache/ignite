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
#include "ignite/odbc/common_types.h"

namespace ignite
{
    namespace odbc
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

        const std::string SqlTypeName::GUID("GUID");

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

        int SqlResultToReturnCode(SqlResult result)
        {
            switch (result)
            {
                case SQL_RESULT_SUCCESS: 
                    return SQL_SUCCESS;

                case SQL_RESULT_SUCCESS_WITH_INFO:
                    return SQL_SUCCESS_WITH_INFO;

                case SQL_RESULT_NO_DATA:
                    return SQL_NO_DATA;

                case SQL_RESULT_ERROR:
                default:
                    return SQL_ERROR;
            }
        }

        DiagnosticField DiagnosticFieldToInternal(int16_t field)
        {
            switch (field)
            {
                case SQL_DIAG_CURSOR_ROW_COUNT:
                    return IGNITE_SQL_DIAG_HEADER_CURSOR_ROW_COUNT;

                case SQL_DIAG_DYNAMIC_FUNCTION:
                    return IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION;

                case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
                    return IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION_CODE;

                case SQL_DIAG_NUMBER:
                    return IGNITE_SQL_DIAG_HEADER_NUMBER;

                case SQL_DIAG_RETURNCODE:
                    return IGNITE_SQL_DIAG_HEADER_RETURNCODE;

                case SQL_DIAG_ROW_COUNT:
                    return IGNITE_SQL_DIAG_HEADER_ROW_COUNT;

                case SQL_DIAG_CLASS_ORIGIN:
                    return IGNITE_SQL_DIAG_STATUS_CLASS_ORIGIN;

                case SQL_DIAG_COLUMN_NUMBER:
                    return IGNITE_SQL_DIAG_STATUS_COLUMN_NUMBER;

                case SQL_DIAG_CONNECTION_NAME:
                    return IGNITE_SQL_DIAG_STATUS_CONNECTION_NAME;

                case SQL_DIAG_MESSAGE_TEXT:
                    return IGNITE_SQL_DIAG_STATUS_MESSAGE_TEXT;

                case SQL_DIAG_NATIVE:
                    return IGNITE_SQL_DIAG_STATUS_NATIVE;

                case SQL_DIAG_ROW_NUMBER:
                    return IGNITE_SQL_DIAG_STATUS_ROW_NUMBER;

                case SQL_DIAG_SERVER_NAME:
                    return IGNITE_SQL_DIAG_STATUS_SERVER_NAME;

                case SQL_DIAG_SQLSTATE:
                    return IGNITE_SQL_DIAG_STATUS_SQLSTATE;

                case SQL_DIAG_SUBCLASS_ORIGIN:
                    return IGNITE_SQL_DIAG_STATUS_SUBCLASS_ORIGIN;

                default:
                    break;
            }
            return IGNITE_SQL_DIAG_UNKNOWN;
        }
    }
}

