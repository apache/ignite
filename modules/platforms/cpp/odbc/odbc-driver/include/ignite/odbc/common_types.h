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

#ifndef _IGNITE_ODBC_DRIVER_COMMON_TYPES
#define _IGNITE_ODBC_DRIVER_COMMON_TYPES

namespace ignite
{
    namespace odbc
    {
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
            SQL_RESULT_NO_DATA
        };

        /**
         * Convert internal Ignite type into ODBC SQL return code.
         */
        int SqlResultToReturnCode(SqlResult result);

        /**
         * Provides detailed information about the cause of a warning or error.
         */
        enum SqlState
        {
            /** Undefined state. Internal, should never be exposed to user. */
            SQL_STATE_UNKNOWN,

            /** Output data has been truncated. */
            SQL_STATE_01004_DATA_TRUNCATED,

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
             * The driver does not support the feature of ODBC behavior that
             * the application requested.
             */
            SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED
        };
    }
}

#endif