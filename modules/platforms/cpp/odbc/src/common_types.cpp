/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

        SqlState::Type ResponseStatusToSqlState(int32_t status)
        {
            switch (status)
            {
                case ResponseStatus::PARSING_FAILURE:
                case ResponseStatus::KEY_UPDATE:
                case ResponseStatus::UNEXPECTED_OPERATION:
                    return SqlState::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION;

                case ResponseStatus::UNSUPPORTED_OPERATION:
                    return SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

                case ResponseStatus::UNEXPECTED_ELEMENT_TYPE:
                    return SqlState::SHY004_INVALID_SQL_DATA_TYPE;

                case ResponseStatus::DUPLICATE_KEY:
                case ResponseStatus::NULL_KEY:
                case ResponseStatus::NULL_VALUE:
                    return SqlState::S23000_INTEGRITY_CONSTRAINT_VIOLATION;

                case ResponseStatus::TABLE_NOT_FOUND:
                    return SqlState::S42S02_TABLE_OR_VIEW_NOT_FOUND;

                case ResponseStatus::INDEX_ALREADY_EXISTS:
                    return SqlState::S42S11_INDEX_ALREADY_EXISTS;

                case ResponseStatus::INDEX_NOT_FOUND:
                    return SqlState::S42S12_INDEX_NOT_FOUND;

                case ResponseStatus::TABLE_ALREADY_EXISTS:
                    return SqlState::S42S01_TABLE_OR_VIEW_ALREADY_EXISTS;

                case ResponseStatus::COLUMN_NOT_FOUND:
                    return SqlState::S42S22_COLUMN_NOT_FOUND;

                case ResponseStatus::COLUMN_ALREADY_EXISTS:
                    return SqlState::S42S21_COLUMN_ALREADY_EXISTS;

                case ResponseStatus::TRANSACTION_COMPLETED:
                    return SqlState::S25000_INVALID_TRANSACTION_STATE;

                case ResponseStatus::TRANSACTION_SERIALIZATION_ERROR:
                    return SqlState::S40001_SERIALIZATION_FAILURE;

                case ResponseStatus::CACHE_NOT_FOUND:
                case ResponseStatus::NULL_TABLE_DESCRIPTOR:
                case ResponseStatus::CONVERSION_FAILED:
                case ResponseStatus::CONCURRENT_UPDATE:
                case ResponseStatus::ENTRY_PROCESSING:
                case ResponseStatus::TABLE_DROP_FAILED:
                case ResponseStatus::STMT_TYPE_MISMATCH:
                case ResponseStatus::UNKNOWN_ERROR:
                default:
                    return SqlState::SHY000_GENERAL_ERROR;
            }
        }
    }
}

