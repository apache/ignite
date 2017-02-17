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

#include <ignite/odbc/system/odbc_constants.h>

#include <ignite/odbc/system/odbc_constants.h>

namespace ignite
{
    namespace odbc
    {
        typedef SQLLEN SqlLen;
        typedef SQLULEN SqlUlen;

        /**
         * SQL result.
         */
        enum SqlResult
        {
            /** Success. */
            SQL_RESULT_SUCCESS,

            /** Success with info. */
            SQL_RESULT_SUCCESS_WITH_INFO,

            /** Error. */
            SQL_RESULT_ERROR,

            /** No more data. */
            SQL_RESULT_NO_DATA,

            /** No more data. */
            SQL_RESULT_NEED_DATA
        };

        /**
         * Provides detailed information about the cause of a warning or error.
         */
        enum SqlState
        {
            /** Undefined state. Internal, should never be exposed to user. */
            SQL_STATE_UNKNOWN,

            /** Output data has been truncated. */
            SQL_STATE_01004_DATA_TRUNCATED,

            /** Invalid connection string attribute. */
            SQL_STATE_01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,

            /** Error in row. */
            SQL_STATE_01S01_ERROR_IN_ROW,

            /**
             * The driver did not support the specified value and
             * substituted a similar value.
             */
            SQL_STATE_01S02_OPTION_VALUE_CHANGED,

            /** String data, length mismatch. */
            SQL_STATE_22026_DATA_LENGTH_MISMATCH,

            /** Invalid cursor state. */
            SQL_STATE_24000_INVALID_CURSOR_STATE,

            /** Invalid descriptor index. */
            SQL_STATE_07009_INVALID_DESCRIPTOR_INDEX,

            /**
             * The driver was unable to establish a connection with the data
             * source.
             */
            SQL_STATE_08001_CANNOT_CONNECT,

            /**
             * The specified ConnectionHandle had already been used
             * to establish a connection with a data source, and the connection
             * was still open.
             */
            SQL_STATE_08002_ALREADY_CONNECTED,

            /** The connection specified was not open. */
            SQL_STATE_08003_NOT_CONNECTED,

            /**
             * An error occurred for which there was no specific SQLSTATE
             * and for which no implementation-specific SQLSTATE was defined.
             */
            SQL_STATE_HY000_GENERAL_ERROR,

            /**
             * The driver was unable to allocate memory for the specified
             * handle.
             */
            SQL_STATE_HY001_MEMORY_ALLOCATION,

            /**
             * The argument TargetType was neither a valid data type
             * nor SQL_C_DEFAULT
             */
            SQL_STATE_HY003_INVALID_APPLICATION_BUFFER_TYPE,

            /** Invalid use of null pointer. */
            SQL_STATE_HY009_INVALID_USE_OF_NULL_POINTER,

            /** Function sequence error. */
            SQL_STATE_HY010_SEQUENCE_ERROR,

            /**
             * Invalid string or buffer length
             */
            SQL_STATE_HY090_INVALID_STRING_OR_BUFFER_LENGTH,

            /**
             * Option type was out of range.
             */
            SQL_STATE_HY092_OPTION_TYPE_OUT_OF_RANGE,

            /** Column type out of range. */
            SQL_STATE_HY097_COLUMN_TYPE_OUT_OF_RANGE,

            /** The value specified for the argument InputOutputType was invalid. */
            SQL_STATE_HY105_INVALID_PARAMETER_TYPE,

            /** The value specified for the argument FetchOrientation was invalid. */
            SQL_STATE_HY106_FETCH_TYPE_OUT_OF_RANGE,

            /**
             * The driver does not support the feature of ODBC behavior that
             * the application requested.
             */
            SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,

            /**
             * The connection timeout period expired before the data source
             * responded to the request.
             */
            SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT,

            /**
             * Driver does not support this function.
             */
            SQL_STATE_IM001_FUNCTION_NOT_SUPPORTED
        };

        /**
         * Diagnostic field type.
         */
        enum DiagnosticField
        {
            /** Field type is unknown to the driver. */
            IGNITE_SQL_DIAG_UNKNOWN,

            /** Header record field: Count of rows in the cursor. */
            IGNITE_SQL_DIAG_HEADER_CURSOR_ROW_COUNT,

            /**
            * Header record field: String that describes the SQL statement
            * that the underlying function executed.
            */
            IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION,

            /**
            * Header record field: Numeric code that describes the SQL
            * statement that was executed by the underlying function.
            */
            IGNITE_SQL_DIAG_HEADER_DYNAMIC_FUNCTION_CODE,

            /** Header record field: Number of status records. */
            IGNITE_SQL_DIAG_HEADER_NUMBER,

            /** Header record field: Last operation return code. */
            IGNITE_SQL_DIAG_HEADER_RETURNCODE,

            /** Header record field: Row count. */
            IGNITE_SQL_DIAG_HEADER_ROW_COUNT,

            /** Status record field: Class origin. */
            IGNITE_SQL_DIAG_STATUS_CLASS_ORIGIN,

            /** Status record field: Column number. */
            IGNITE_SQL_DIAG_STATUS_COLUMN_NUMBER,

            /** Status record field: Connection name. */
            IGNITE_SQL_DIAG_STATUS_CONNECTION_NAME,

            /** Status record field: Message text. */
            IGNITE_SQL_DIAG_STATUS_MESSAGE_TEXT,

            /** Status record field: Native result code. */
            IGNITE_SQL_DIAG_STATUS_NATIVE,

            /** Status record field: Row number. */
            IGNITE_SQL_DIAG_STATUS_ROW_NUMBER,

            /** Status record field: Server name. */
            IGNITE_SQL_DIAG_STATUS_SERVER_NAME,

            /** Status record field: SQLSTATE. */
            IGNITE_SQL_DIAG_STATUS_SQLSTATE,

            /** Status record field: Subclass origin. */
            IGNITE_SQL_DIAG_STATUS_SUBCLASS_ORIGIN
        };

        /**
         * Environment attribute.
         */
        enum EnvironmentAttribute
        {
            /** ODBC attribute is unknown to the driver. */
            IGNITE_SQL_ENV_ATTR_UNKNOWN,

            /** ODBC version. */
            IGNITE_SQL_ENV_ATTR_ODBC_VERSION,

            /** Null-termination of strings. */
            IGNITE_SQL_ENV_ATTR_OUTPUT_NTS
        };

        /**
         * Convert internal Ignite type into ODBC SQL return code.
         *
         * @param result Internal result type.
         * @return ODBC result type.
         */
        int SqlResultToReturnCode(SqlResult result);

        /**
         * Convert ODBC field type to internal DiagnosticField type value.
         *
         * @param field ODBC field type.
         * @return Internal DiagnosticField type value.
         */
        DiagnosticField DiagnosticFieldToInternal(int16_t field);

        /**
         * Convert environment attribute to internal EnvironmentAttribute type value.
         *
         * @param attr Environment attribute.
         * @return Internal EnvironmentAttribute type value.
         */
        EnvironmentAttribute EnvironmentAttributeToInternal(int32_t attr);


    }
}

#endif //_IGNITE_ODBC_COMMON_TYPES
