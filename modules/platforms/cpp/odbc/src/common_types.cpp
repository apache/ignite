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
        int SqlResultToReturnCode(SqlResult::Type result)
        {
            switch (result)
            {
                case SqlResult::AI_SUCCESS: 
                    return SQL_SUCCESS;

                case SqlResult::AI_SUCCESS_WITH_INFO:
                    return SQL_SUCCESS_WITH_INFO;

                case SqlResult::AI_NO_DATA:
                    return SQL_NO_DATA;

                case SqlResult::AI_NEED_DATA:
                    return SQL_NEED_DATA;

                case SqlResult::AI_ERROR:
                default:
                    return SQL_ERROR;
            }
        }

        DiagnosticField::Type DiagnosticFieldToInternal(int16_t field)
        {
            switch (field)
            {
                case SQL_DIAG_CURSOR_ROW_COUNT:
                    return DiagnosticField::HEADER_CURSOR_ROW_COUNT;

                case SQL_DIAG_DYNAMIC_FUNCTION:
                    return DiagnosticField::HEADER_DYNAMIC_FUNCTION;

                case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
                    return DiagnosticField::HEADER_DYNAMIC_FUNCTION_CODE;

                case SQL_DIAG_NUMBER:
                    return DiagnosticField::HEADER_NUMBER;

                case SQL_DIAG_RETURNCODE:
                    return DiagnosticField::HEADER_RETURNCODE;

                case SQL_DIAG_ROW_COUNT:
                    return DiagnosticField::HEADER_ROW_COUNT;

                case SQL_DIAG_CLASS_ORIGIN:
                    return DiagnosticField::STATUS_CLASS_ORIGIN;

                case SQL_DIAG_COLUMN_NUMBER:
                    return DiagnosticField::STATUS_COLUMN_NUMBER;

                case SQL_DIAG_CONNECTION_NAME:
                    return DiagnosticField::STATUS_CONNECTION_NAME;

                case SQL_DIAG_MESSAGE_TEXT:
                    return DiagnosticField::STATUS_MESSAGE_TEXT;

                case SQL_DIAG_NATIVE:
                    return DiagnosticField::STATUS_NATIVE;

                case SQL_DIAG_ROW_NUMBER:
                    return DiagnosticField::STATUS_ROW_NUMBER;

                case SQL_DIAG_SERVER_NAME:
                    return DiagnosticField::STATUS_SERVER_NAME;

                case SQL_DIAG_SQLSTATE:
                    return DiagnosticField::STATUS_SQLSTATE;

                case SQL_DIAG_SUBCLASS_ORIGIN:
                    return DiagnosticField::STATUS_SUBCLASS_ORIGIN;

                default:
                    break;
            }

            return DiagnosticField::UNKNOWN;
        }

        EnvironmentAttribute::Type EnvironmentAttributeToInternal(int32_t attr)
        {
            switch (attr)
            {
                case SQL_ATTR_ODBC_VERSION:
                    return EnvironmentAttribute::ODBC_VERSION;

                case SQL_ATTR_OUTPUT_NTS:
                    return EnvironmentAttribute::OUTPUT_NTS;

                default:
                    break;
            }

            return EnvironmentAttribute::UNKNOWN;
        }
    }
}

