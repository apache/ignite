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

#ifndef _IGNITE_ODBC_COMMON_TYPES
#define _IGNITE_ODBC_COMMON_TYPES

#include <stdint.h>
#include "system/odbc_constants.h"

namespace ignite
{
    namespace odbc
    {
        typedef SQLLEN SqlLen;
        typedef SQLULEN SqlUlen;

        /**
         * SQL result.
         */
        struct SqlResult
        {
            enum Type
            {
                /** Success. */
                AI_SUCCESS,

                /** Success with info. */
                AI_SUCCESS_WITH_INFO,

                /** Error. */
                AI_ERROR,

                /** No more data. */
                AI_NO_DATA,

                /** No more data. */
                AI_NEED_DATA
            };
        };

        /**
         * Provides detailed information about the cause of a warning or error.
         */
        struct SqlState
        {
            enum Type
            {
                /** Undefined state. Internal, should never be exposed to user. */
                UNKNOWN,

                /** Output data has been truncated. */
                S01004_DATA_TRUNCATED,

                /** Invalid connection string attribute. */
                S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,

                /** Error in row. */
                S01S01_ERROR_IN_ROW,

                /**
                 * The driver did not support the specified value and
                 * substituted a similar value.
                 */
                S01S02_OPTION_VALUE_CHANGED,

                /** The numeric or time data returned for a column was truncated. */
                S01S07_FRACTIONAL_TRUNCATION,

                /** Restricted data type attribute violation. */
                S07006_RESTRICTION_VIOLATION,

                /** Indicator needed but not suplied. */
                S22002_INDICATOR_NEEDED,

                /** String data, length mismatch. */
                S22026_DATA_LENGTH_MISMATCH,

                /** Integrity constraint violation. */
                S23000_INTEGRITY_CONSTRAINT_VIOLATION,

                /** Invalid cursor state. */
                S24000_INVALID_CURSOR_STATE,

                /** Invalid transaction state. */
                S25000_INVALID_TRANSACTION_STATE,

                /** Invalid schema name. */
                S3F000_INVALID_SCHEMA_NAME,

                /** Serialization failure. */
                S40001_SERIALIZATION_FAILURE,

                /** Syntax error or access violation. */
                S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION,

                /** Base table or view already exists. */
                S42S01_TABLE_OR_VIEW_ALREADY_EXISTS,

                /** Base table or view not found. */
                S42S02_TABLE_OR_VIEW_NOT_FOUND,

                /** Index already exists. */
                S42S11_INDEX_ALREADY_EXISTS,

                /** Index not found. */
                S42S12_INDEX_NOT_FOUND,

                /** Column already exists. */
                S42S21_COLUMN_ALREADY_EXISTS,

                /** Column not found. */
                S42S22_COLUMN_NOT_FOUND,

                /** Invalid descriptor index. */
                S07009_INVALID_DESCRIPTOR_INDEX,

                /**
                 * The driver was unable to establish a connection with the data
                 * source.
                 */
                S08001_CANNOT_CONNECT,

                /**
                 * The specified ConnectionHandle had already been used
                 * to establish a connection with a data source, and the connection
                 * was still open.
                 */
                S08002_ALREADY_CONNECTED,

                /** The connection specified was not open. */
                S08003_NOT_CONNECTED,

                /** Server rejected the connection. */
                S08004_CONNECTION_REJECTED,

                /** Communication link failure. */
                S08S01_LINK_FAILURE,

                /**
                 * An error occurred for which there was no specific SQLSTATE
                 * and for which no implementation-specific SQLSTATE was defined.
                 */
                SHY000_GENERAL_ERROR,

                /**
                 * The driver was unable to allocate memory for the specified
                 * handle.
                 */
                SHY001_MEMORY_ALLOCATION,

                /**
                 * The argument TargetType was neither a valid data type
                 * nor SQL_C_DEFAULT
                 */
                SHY003_INVALID_APPLICATION_BUFFER_TYPE,

                /** Invalid SQL data type. */
                SHY004_INVALID_SQL_DATA_TYPE,

                /** Operation canceled. */
                SHY008_OPERATION_CANCELED,

                /** Invalid use of null pointer. */
                SHY009_INVALID_USE_OF_NULL_POINTER,

                /** Function sequence error. */
                SHY010_SEQUENCE_ERROR,

                /**
                 * Invalid string or buffer length
                 */
                SHY090_INVALID_STRING_OR_BUFFER_LENGTH,

                /**
                 * Option type was out of range.
                 */
                SHY092_OPTION_TYPE_OUT_OF_RANGE,

                /** Column type out of range. */
                SHY097_COLUMN_TYPE_OUT_OF_RANGE,

                /** The value specified for the argument InputOutputType was invalid. */
                SHY105_INVALID_PARAMETER_TYPE,

                /** The value specified for the argument FetchOrientation was invalid. */
                SHY106_FETCH_TYPE_OUT_OF_RANGE,

                /**
                 * The driver does not support the feature of ODBC behavior that
                 * the application requested.
                 */
                SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,

                /** The timeout period expired before the data source responded to the request. */
                SHYT00_TIMEOUT_EXPIRED,

                /**
                 * The connection timeout period expired before the data source
                 * responded to the request.
                 */
                SHYT01_CONNECTION_TIMEOUT,

                /**
                 * Driver does not support this function.
                 */
                SIM001_FUNCTION_NOT_SUPPORTED
            };
        };

        /**
         * Diagnostic field type.
         */
        struct DiagnosticField
        {
            enum Type
            {
                /** Field type is unknown to the driver. */
                UNKNOWN,

                /** Header record field: Count of rows in the cursor. */
                HEADER_CURSOR_ROW_COUNT,

                /**
                 * Header record field: String that describes the SQL statement
                 * that the underlying function executed.
                 */
                HEADER_DYNAMIC_FUNCTION,

                /**
                 * Header record field: Numeric code that describes the SQL
                 * statement that was executed by the underlying function.
                 */
                HEADER_DYNAMIC_FUNCTION_CODE,

                /** Header record field: Number of status records. */
                HEADER_NUMBER,

                /** Header record field: Last operation return code. */
                HEADER_RETURNCODE,

                /** Header record field: Row count. */
                HEADER_ROW_COUNT,

                /** Status record field: Class origin. */
                STATUS_CLASS_ORIGIN,

                /** Status record field: Column number. */
                STATUS_COLUMN_NUMBER,

                /** Status record field: Connection name. */
                STATUS_CONNECTION_NAME,

                /** Status record field: Message text. */
                STATUS_MESSAGE_TEXT,

                /** Status record field: Native result code. */
                STATUS_NATIVE,

                /** Status record field: Row number. */
                STATUS_ROW_NUMBER,

                /** Status record field: Server name. */
                STATUS_SERVER_NAME,

                /** Status record field: SQLSTATE. */
                STATUS_SQLSTATE,

                /** Status record field: Subclass origin. */
                STATUS_SUBCLASS_ORIGIN
            };
        };

        /**
         * Environment attribute.
         */
        struct EnvironmentAttribute
        {
            enum Type
            {
                /** ODBC attribute is unknown to the driver. */
                UNKNOWN,

                /** ODBC version. */
                ODBC_VERSION,

                /** Null-termination of strings. */
                OUTPUT_NTS
            };
        };

        struct ResponseStatus
        {
            enum Type
            {
                /** Operation completed successfully. */
                SUCCESS = 0,

                /* 1xxx - parsing errors */

                /** Unknown error, or the one without specific code. */
                UNKNOWN_ERROR = 1,

                /** General parsing error - for the cases when there's no more specific code available. */
                PARSING_FAILURE = 1001,

                /** Requested operation is not supported. */
                UNSUPPORTED_OPERATION = 1002,

                /* 2xxx - analysis errors */

                /** Code encountered SQL statement of some type that it did not expect in current analysis context. */
                UNEXPECTED_OPERATION = 2001,

                /** Code encountered SQL expression of some type that it did not expect in current analysis context. */
                UNEXPECTED_ELEMENT_TYPE = 2002,

                /** Analysis detected that the statement is trying to directly UPDATE key or its fields. */
                KEY_UPDATE = 2003,

                /* 3xxx - database API related runtime errors */
                /** Required table not found. */
                TABLE_NOT_FOUND = 3001,

                /** Required table does not have a descriptor set. */
                NULL_TABLE_DESCRIPTOR = 3002,

                /** Statement type does not match that declared by JDBC driver. */
                STMT_TYPE_MISMATCH = 3003,

                /** DROP TABLE failed. */
                TABLE_DROP_FAILED = 3004,

                /** Index already exists. */
                INDEX_ALREADY_EXISTS = 3005,

                /** Index does not exist. */
                INDEX_NOT_FOUND = 3006,

                /** Required table already exists. */
                TABLE_ALREADY_EXISTS = 3007,

                /** Required column not found. */
                COLUMN_NOT_FOUND = 3008,

                /** Required column already exists. */
                COLUMN_ALREADY_EXISTS = 3009,

                /** Conversion failure. */
                CONVERSION_FAILED = 3013,

                /* 4xxx - cache related runtime errors */

                /** Attempt to INSERT a key that is already in cache. */
                DUPLICATE_KEY = 4001,

                /** Attempt to UPDATE or DELETE a key whose value has been updated concurrently by someone else. */
                CONCURRENT_UPDATE = 4002,

                /** Attempt to INSERT or MERGE {@code null} key. */
                NULL_KEY = 4003,

                /** Attempt to INSERT or MERGE {@code null} value. */
                NULL_VALUE = 4004,

                /** EntryProcessor has thrown an exception during IgniteCache::invokeAll. */
                ENTRY_PROCESSING = 4005,

                /** Cache not found. */
                CACHE_NOT_FOUND = 4006,

                /** Transaction is already completed. */
                TRANSACTION_COMPLETED = 5004,

                /** Transaction serialization error. */
                TRANSACTION_SERIALIZATION_ERROR = 5005
            };
        };

        /**
         * Convert internal Ignite type into ODBC SQL return code.
         *
         * @param result Internal result type.
         * @return ODBC result type.
         */
        int SqlResultToReturnCode(SqlResult::Type result);

        /**
         * Convert ODBC field type to internal DiagnosticField::Type type value.
         *
         * @param field ODBC field type.
         * @return Internal DiagnosticField::Type type value.
         */
        DiagnosticField::Type DiagnosticFieldToInternal(int16_t field);

        /**
         * Convert environment attribute to internal EnvironmentAttribute::Type type value.
         *
         * @param attr Environment attribute.
         * @return Internal EnvironmentAttribute::Type type value.
         */
        EnvironmentAttribute::Type EnvironmentAttributeToInternal(int32_t attr);

        /**
         * Convert request response status to SQL state.
         *
         * @param status Response status.
         * @return SQL state.
         */
        SqlState::Type ResponseStatusToSqlState(int32_t status);
    }
}

#endif //_IGNITE_ODBC_COMMON_TYPES
