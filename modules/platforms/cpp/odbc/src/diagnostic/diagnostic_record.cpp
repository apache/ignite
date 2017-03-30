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

#include <set>
#include <string>

#include "ignite/odbc/diagnostic/diagnostic_record.h"

namespace
{
    /** SQLSTATEs defined by Open Group and ISO call-level interface. */
    const std::string ORIGIN_ISO_9075 = "ISO 9075";

    /** ODBC-specific SQLSTATEs (all those whose SQLSTATE class is "IM"). */
    const std::string ORIGIN_ODBC_3_0 = "ODBC 3.0";

    /** SQL state unknown constant. */
    const std::string STATE_UNKNOWN = "";

    /** SQL state 01004 constant. */
    const std::string STATE_01004 = "01004";

    /** SQL state 01S00 constant. */
    const std::string STATE_01S00 = "01S00";

    /** SQL state 01S01 constant. */
    const std::string STATE_01S01 = "01S01";

    /** SQL state 22026 constant. */
    const std::string STATE_22026 = "22026";

    /** SQL state 24000 constant. */
    const std::string STATE_24000 = "24000";

    /** SQL state 07009 constant. */
    const std::string STATE_07009 = "07009";

    /** SQL state 08001 constant. */
    const std::string STATE_08001 = "08001";

    /** SQL state 08002 constant. */
    const std::string STATE_08002 = "08002";

    /** SQL state 08003 constant. */
    const std::string STATE_08003 = "08003";

    /** SQL state HY000 constant. */
    const std::string STATE_HY000 = "HY000";

    /** SQL state HY001 constant. */
    const std::string STATE_HY001 = "HY001";

    /** SQL state HY003 constant. */
    const std::string STATE_HY003 = "HY003";

    /** SQL state HY009 constant. */
    const std::string STATE_HY009 = "HY009";

    /** SQL state HY010 constant. */
    const std::string STATE_HY010 = "HY010";

    /** SQL state HY092 constant. */
    const std::string STATE_HY092 = "HY092";

    /** SQL state HY105 constant. */
    const std::string STATE_HY105 = "HY105";

    /** SQL state HY106 constant. */
    const std::string STATE_HY106 = "HY106";

    /** SQL state HYC00 constant. */
    const std::string STATE_HYC00 = "HYC00";

    /** SQL state HYT01 constant. */
    const std::string STATE_HYT01 = "HYT01";

    /** SQL state HY090 constant. */
    const std::string STATE_HY090 = "HY090";

    /** SQL state IM001 constant. */
    const std::string STATE_IM001 = "IM001";
}

namespace ignite
{
    namespace odbc
    {
        namespace diagnostic
        {
            DiagnosticRecord::DiagnosticRecord() :
                sqlState(SQL_STATE_UNKNOWN),
                message(),
                connectionName(),
                serverName(),
                rowNum(0),
                columnNum(0),
                retrieved(false)
            {
                // No-op.
            }

            DiagnosticRecord::DiagnosticRecord(SqlState sqlState,
                const std::string& message, const std::string& connectionName,
                const std::string& serverName, int32_t rowNum, int32_t columnNum) :
                sqlState(sqlState),
                message(message),
                connectionName(connectionName),
                serverName(serverName),
                rowNum(rowNum),
                columnNum(columnNum),
                retrieved(false)
            {
                // No-op.
            }

            DiagnosticRecord::~DiagnosticRecord()
            {
                // No-op.
            }

            const std::string& DiagnosticRecord::GetClassOrigin() const
            {
                const std::string& state = GetSqlState();

                if (state[0] == 'I' && state[1] == 'M')
                    return ORIGIN_ODBC_3_0;

                return ORIGIN_ISO_9075;
            }

            const std::string& DiagnosticRecord::GetSubclassOrigin() const
            {
                static std::set<std::string> odbcSubclasses;

                if (odbcSubclasses.empty())
                {
                    // This is a fixed list taken from ODBC doc.
                    // Please do not add/remove values here.
                    odbcSubclasses.insert("01S00");
                    odbcSubclasses.insert("01S01");
                    odbcSubclasses.insert("01S02");
                    odbcSubclasses.insert("01S06");
                    odbcSubclasses.insert("01S07");
                    odbcSubclasses.insert("07S01");
                    odbcSubclasses.insert("08S01");
                    odbcSubclasses.insert("21S01");
                    odbcSubclasses.insert("21S02");
                    odbcSubclasses.insert("25S01");
                    odbcSubclasses.insert("25S02");
                    odbcSubclasses.insert("25S03");
                    odbcSubclasses.insert("42S01");
                    odbcSubclasses.insert("42S02");
                    odbcSubclasses.insert("42S11");
                    odbcSubclasses.insert("42S12");
                    odbcSubclasses.insert("42S21");
                    odbcSubclasses.insert("42S22");
                    odbcSubclasses.insert("HY095");
                    odbcSubclasses.insert("HY097");
                    odbcSubclasses.insert("HY098");
                    odbcSubclasses.insert("HY099");
                    odbcSubclasses.insert("HY100");
                    odbcSubclasses.insert("HY101");
                    odbcSubclasses.insert("HY105");
                    odbcSubclasses.insert("HY107");
                    odbcSubclasses.insert("HY109");
                    odbcSubclasses.insert("HY110");
                    odbcSubclasses.insert("HY111");
                    odbcSubclasses.insert("HYT00");
                    odbcSubclasses.insert("HYT01");
                    odbcSubclasses.insert("IM001");
                    odbcSubclasses.insert("IM002");
                    odbcSubclasses.insert("IM003");
                    odbcSubclasses.insert("IM004");
                    odbcSubclasses.insert("IM005");
                    odbcSubclasses.insert("IM006");
                    odbcSubclasses.insert("IM007");
                    odbcSubclasses.insert("IM008");
                    odbcSubclasses.insert("IM010");
                    odbcSubclasses.insert("IM011");
                    odbcSubclasses.insert("IM012");
                }

                const std::string& state = GetSqlState();

                if (odbcSubclasses.find(state) != odbcSubclasses.end())
                    return ORIGIN_ODBC_3_0;

                return ORIGIN_ISO_9075;
            }

            const std::string& DiagnosticRecord::GetMessageText() const
            {
                return message;
            }

            const std::string& DiagnosticRecord::GetConnectionName() const
            {
                return connectionName;
            }

            const std::string& DiagnosticRecord::GetServerName() const
            {
                return serverName;
            }

            const std::string& DiagnosticRecord::GetSqlState() const
            {
                switch (sqlState)
                {
                    case SQL_STATE_01004_DATA_TRUNCATED:
                        return STATE_01004;

                    case SQL_STATE_01S00_INVALID_CONNECTION_STRING_ATTRIBUTE:
                        return STATE_01S00;

                    case SQL_STATE_01S01_ERROR_IN_ROW:
                        return STATE_01S01;

                    case SQL_STATE_22026_DATA_LENGTH_MISMATCH:
                        return STATE_22026;

                    case SQL_STATE_24000_INVALID_CURSOR_STATE:
                        return STATE_24000;

                    case SQL_STATE_07009_INVALID_DESCRIPTOR_INDEX:
                        return STATE_07009;

                    case SQL_STATE_08001_CANNOT_CONNECT:
                        return STATE_08001;

                    case SQL_STATE_08002_ALREADY_CONNECTED:
                        return STATE_08002;

                    case SQL_STATE_08003_NOT_CONNECTED:
                        return STATE_08003;

                    case SQL_STATE_HY000_GENERAL_ERROR:
                        return STATE_HY000;

                    case SQL_STATE_HY001_MEMORY_ALLOCATION:
                        return STATE_HY001;

                    case SQL_STATE_HY003_INVALID_APPLICATION_BUFFER_TYPE:
                        return STATE_HY003;

                    case SQL_STATE_HY009_INVALID_USE_OF_NULL_POINTER:
                        return STATE_HY009;

                    case SQL_STATE_HY010_SEQUENCE_ERROR:
                        return STATE_HY010;

                    case SQL_STATE_HY090_INVALID_STRING_OR_BUFFER_LENGTH:
                        return STATE_HY090;

                    case SQL_STATE_HY092_OPTION_TYPE_OUT_OF_RANGE:
                        return STATE_HY092;

                    case SQL_STATE_HY105_INVALID_PARAMETER_TYPE:
                        return STATE_HY105;

                    case SQL_STATE_HY106_FETCH_TYPE_OUT_OF_RANGE:
                        return STATE_HY106;

                    case SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED:
                        return STATE_HYC00;

                    case SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT:
                        return STATE_HYT01;

                    case SQL_STATE_IM001_FUNCTION_NOT_SUPPORTED:
                        return STATE_IM001;

                    default:
                        break;
                }

                return STATE_UNKNOWN;
            }

            int32_t DiagnosticRecord::GetRowNumber() const
            {
                return rowNum;
            }

            int32_t DiagnosticRecord::GetColumnNumber() const
            {
                return columnNum;
            }

            bool DiagnosticRecord::IsRetrieved() const
            {
                return retrieved;
            }

            void DiagnosticRecord::MarkRetrieved()
            {
                retrieved = true;
            }
        }
    }
}
