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

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <boost/test/unit_test.hpp>

#include "ignite/ignition.h"

#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite_test;
using namespace boost::unit_test;

namespace ignite
{
    namespace odbc
    {
        void OdbcTestSuite::Prepare()
        {
            // Allocate an environment handle
            SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

            BOOST_REQUIRE(env != NULL);

            // We want ODBC 3 support
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

            // Allocate a connection handle
            SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

            BOOST_REQUIRE(dbc != NULL);
        }

        void OdbcTestSuite::Connect(SQLHDBC& conn, SQLHSTMT& statement, const std::string& connectStr)
        {
            // Allocate a connection handle
            SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

            BOOST_REQUIRE(conn != NULL);

            // Connect string
            std::vector<SQLCHAR> connectStr0(connectStr.begin(), connectStr.end());

            SQLCHAR outstr[ODBC_BUFFER_SIZE];
            SQLSMALLINT outstrlen;

            // Connecting to ODBC server.
            SQLRETURN ret = SQLDriverConnect(conn, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
                outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

            if (!SQL_SUCCEEDED(ret))
            {
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, conn));
            }

            // Allocate a statement handle
            SQLAllocHandle(SQL_HANDLE_STMT, conn, &statement);

            BOOST_REQUIRE(statement != NULL);
        }

        void OdbcTestSuite::Connect(const std::string& connectStr)
        {
            Prepare();

            // Connect string
            std::vector<SQLCHAR> connectStr0(connectStr.begin(), connectStr.end());

            SQLCHAR outstr[ODBC_BUFFER_SIZE];
            SQLSMALLINT outstrlen;

            // Connecting to ODBC server.
            SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
                outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));

            // Allocate a statement handle
            SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

            BOOST_REQUIRE(stmt != NULL);
        }

        std::string OdbcTestSuite::ExpectConnectionReject(const std::string& connectStr)
        {
            Prepare();

            // Connect string
            std::vector<SQLCHAR> connectStr0(connectStr.begin(), connectStr.end());

            SQLCHAR outstr[ODBC_BUFFER_SIZE];
            SQLSMALLINT outstrlen;

            // Connecting to ODBC server.
            SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
                outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

            BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

            return GetOdbcErrorState(SQL_HANDLE_DBC, dbc);
        }

        void OdbcTestSuite::Disconnect()
        {
            if (stmt)
            {
                // Releasing statement handle.
                SQLFreeHandle(SQL_HANDLE_STMT, stmt);
                stmt = NULL;
            }

            if (dbc)
            {
                // Disconneting from the server.
                SQLDisconnect(dbc);

                // Releasing allocated handles.
                SQLFreeHandle(SQL_HANDLE_DBC, dbc);
                dbc = NULL;
            }
        }

        void OdbcTestSuite::CleanUp()
        {
            Disconnect();

            if (env)
            {
                // Releasing allocated handles.
                SQLFreeHandle(SQL_HANDLE_ENV, env);
                env = NULL;
            }
        }

        Ignite OdbcTestSuite::StartTestNode(const char* cfg, const char* name)
        {
            std::string config(cfg);

#ifdef IGNITE_TESTS_32
            // Cutting off the ".xml" part.
            config.resize(config.size() - 4);
            config += "-32.xml";
#endif //IGNITE_TESTS_32

            return StartNode(config.c_str(), name);
        }

        OdbcTestSuite::OdbcTestSuite():
            env(NULL),
            dbc(NULL),
            stmt(NULL)
        {
            // No-op.
        }

        OdbcTestSuite::~OdbcTestSuite()
        {
            CleanUp();

            Ignition::StopAll(true);
        }

        int8_t OdbcTestSuite::GetTestI8Field(int64_t idx)
        {
            return static_cast<int8_t>(idx * 8);
        }

        void OdbcTestSuite::CheckTestI8Value(int idx, int8_t value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestI8Field(idx));
        }

        int16_t OdbcTestSuite::GetTestI16Field(int64_t idx)
        {
            return static_cast<int16_t>(idx * 16);
        }

        void OdbcTestSuite::CheckTestI16Value(int idx, int16_t value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestI16Field(idx));
        }

        int32_t OdbcTestSuite::GetTestI32Field(int64_t idx)
        {
            return static_cast<int32_t>(idx * 32);
        }

        void OdbcTestSuite::CheckTestI32Value(int idx, int32_t value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestI32Field(idx));
        }

        std::string OdbcTestSuite::GetTestString(int64_t idx)
        {
            std::stringstream builder;

            builder << "String#" << idx;

            return builder.str();
        }

        void OdbcTestSuite::CheckTestStringValue(int idx, const std::string &value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestString(idx));
        }

        float OdbcTestSuite::GetTestFloatField(int64_t idx)
        {
            return static_cast<float>(idx * 0.5f);
        }

        void OdbcTestSuite::CheckTestFloatValue(int idx, float value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestFloatField(idx));
        }

        double OdbcTestSuite::GetTestDoubleField(int64_t idx)
        {
            return static_cast<double>(idx * 0.25f);
        }

        void OdbcTestSuite::CheckTestDoubleValue(int idx, double value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestDoubleField(idx));
        }

        bool OdbcTestSuite::GetTestBoolField(int64_t idx)
        {
            return static_cast<bool>(idx % 2 == 0);
        }

        void OdbcTestSuite::CheckTestBoolValue(int idx, bool value)
        {
            BOOST_TEST_INFO("Test index: " << idx);
            BOOST_CHECK_EQUAL(value, GetTestBoolField(idx));
        }

        void OdbcTestSuite::GetTestDateField(int64_t idx, SQL_DATE_STRUCT& val)
        {
            val.year = static_cast<SQLSMALLINT>(2017 + idx / 365);
            val.month = static_cast<SQLUSMALLINT>(((idx / 28) % 12) + 1);
            val.day = static_cast<SQLUSMALLINT>((idx % 28) + 1);
        }

        void OdbcTestSuite::CheckTestDateValue(int idx, const SQL_DATE_STRUCT& val)
        {
            BOOST_TEST_CONTEXT("Test index: " << idx)
            {
                SQL_DATE_STRUCT expected;
                GetTestDateField(idx, expected);

                BOOST_CHECK_EQUAL(val.year, expected.year);
                BOOST_CHECK_EQUAL(val.month, expected.month);
                BOOST_CHECK_EQUAL(val.day, expected.day);
            }
        }

        void OdbcTestSuite::GetTestTimeField(int64_t idx, SQL_TIME_STRUCT& val)
        {
            val.hour = (idx / 3600) % 24;
            val.minute = (idx / 60) % 60;
            val.second = idx % 60;
        }

        void OdbcTestSuite::CheckTestTimeValue(int idx, const SQL_TIME_STRUCT& val)
        {
            BOOST_TEST_CONTEXT("Test index: " << idx)
            {
                SQL_TIME_STRUCT expected;
                GetTestTimeField(idx, expected);

                BOOST_CHECK_EQUAL(val.hour, expected.hour);
                BOOST_CHECK_EQUAL(val.minute, expected.minute);
                BOOST_CHECK_EQUAL(val.second, expected.second);
            }
        }

        void OdbcTestSuite::GetTestTimestampField(int64_t idx, SQL_TIMESTAMP_STRUCT& val)
        {
            SQL_DATE_STRUCT date;
            GetTestDateField(idx, date);

            SQL_TIME_STRUCT time;
            GetTestTimeField(idx, time);

            val.year = date.year;
            val.month = date.month;
            val.day = date.day;
            val.hour = time.hour;
            val.minute = time.minute;
            val.second = time.second;
            val.fraction = static_cast<uint64_t>(std::abs(idx * 914873)) % 1000000000;
        }

        void OdbcTestSuite::CheckTestTimestampValue(int idx, const SQL_TIMESTAMP_STRUCT& val)
        {
            BOOST_TEST_CONTEXT("Test index: " << idx)
            {
                SQL_TIMESTAMP_STRUCT expected;
                GetTestTimestampField(idx, expected);

                BOOST_CHECK_EQUAL(val.year, expected.year);
                BOOST_CHECK_EQUAL(val.month, expected.month);
                BOOST_CHECK_EQUAL(val.day, expected.day);
                BOOST_CHECK_EQUAL(val.hour, expected.hour);
                BOOST_CHECK_EQUAL(val.minute, expected.minute);
                BOOST_CHECK_EQUAL(val.second, expected.second);
                BOOST_CHECK_EQUAL(val.fraction, expected.fraction);
            }
        }

        void OdbcTestSuite::GetTestI8ArrayField(int64_t idx, int8_t* val, size_t valLen)
        {
            for (size_t j = 0; j < valLen; ++j)
                val[j] = static_cast<int8_t>(idx * valLen + j);
        }

        void OdbcTestSuite::CheckTestI8ArrayValue(int idx, const int8_t* val, size_t valLen)
        {
            BOOST_TEST_CONTEXT("Test index: " << idx)
            {
                common::FixedSizeArray<int8_t> expected(static_cast<int32_t>(valLen));
                GetTestI8ArrayField(idx, expected.GetData(), expected.GetSize());

                for (size_t j = 0; j < valLen; ++j)
                {
                    BOOST_TEST_INFO("Byte index: " << j);
                    BOOST_CHECK_EQUAL(val[j], expected[(int32_t)j]);
                }
            }
        }

        void OdbcTestSuite::CheckSQLDiagnosticError(int16_t handleType, SQLHANDLE handle, const std::string& expectSqlState)
        {
            SQLCHAR state[ODBC_BUFFER_SIZE];
            SQLINTEGER nativeError = 0;
            SQLCHAR message[ODBC_BUFFER_SIZE];
            SQLSMALLINT messageLen = 0;

            SQLRETURN ret = SQLGetDiagRec(handleType, handle, 1, state, &nativeError, message, sizeof(message), &messageLen);

            const std::string sqlState = reinterpret_cast<char*>(state);
            BOOST_REQUIRE_EQUAL(ret, SQL_SUCCESS);
            BOOST_REQUIRE_EQUAL(sqlState, expectSqlState);
            BOOST_REQUIRE(messageLen > 0);
        }

        void OdbcTestSuite::CheckSQLStatementDiagnosticError(const std::string& expectSqlState)
        {
            CheckSQLDiagnosticError(SQL_HANDLE_STMT, stmt, expectSqlState);
        }

        void OdbcTestSuite::CheckSQLConnectionDiagnosticError(const std::string& expectSqlState)
        {
            CheckSQLDiagnosticError(SQL_HANDLE_DBC, dbc, expectSqlState);
        }

        std::vector<SQLCHAR> OdbcTestSuite::MakeQuery(const std::string& qry)
        {
            return std::vector<SQLCHAR>(qry.begin(), qry.end());
        }

        SQLRETURN OdbcTestSuite::ExecQuery(const std::string& qry)
        {
            std::vector<SQLCHAR> sql = MakeQuery(qry);

            return SQLExecDirect(stmt, sql.data(), static_cast<SQLINTEGER>(sql.size()));
        }

        SQLRETURN OdbcTestSuite::PrepareQuery(const std::string& qry)
        {
            std::vector<SQLCHAR> sql = MakeQuery(qry);

            return SQLPrepare(stmt, sql.data(), static_cast<SQLINTEGER>(sql.size()));
        }

        void OdbcTestSuite::InsertTestStrings(int recordsNum, bool merge)
        {
            SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(?, ?)";
            SQLCHAR mergeReq[] = "MERGE INTO TestType(_key, strField) VALUES(?, ?)";

            SQLRETURN ret = SQLPrepare(stmt, merge ? mergeReq : insertReq, SQL_NTS);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            int64_t key = 0;
            char strField[1024] = {0};
            SQLLEN strFieldLen = 0;

            // Binding parameters.
            ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, sizeof(strField),
                                   sizeof(strField), &strField, sizeof(strField), &strFieldLen);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Inserting values.
            for (SQLSMALLINT i = 0; i < recordsNum; ++i)
            {
                key = i + 1;
                std::string val = GetTestString(i);

                CopyStringToBuffer(strField, val, sizeof(strField));
                strFieldLen = SQL_NTS;

                ret = SQLExecute(stmt);

                if (!SQL_SUCCEEDED(ret))
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

                SQLLEN affected = 0;
                ret = SQLRowCount(stmt, &affected);

                if (!SQL_SUCCEEDED(ret))
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

                BOOST_CHECK_EQUAL(affected, 1);

                ret = SQLMoreResults(stmt);

                if (ret != SQL_NO_DATA)
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
            }

            // Resetting parameters.
            ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        int OdbcTestSuite::InsertTestBatch(int from, int to, int expectedToAffect, bool merge)
        {
            using common::FixedSizeArray;

            SQLCHAR insertReq[] = "INSERT "
                "INTO TestType(_key, i8Field, i16Field, i32Field, strField, floatField, doubleField, boolField, dateField, "
                "timeField, timestampField, i8ArrayField) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            SQLCHAR mergeReq[] = "MERGE "
                "INTO TestType(_key, i8Field, i16Field, i32Field, strField, floatField, doubleField, boolField, dateField, "
                "timeField, timestampField, i8ArrayField) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            SQLRETURN ret;

            int32_t recordsNum = to - from;

            ret = SQLPrepare(stmt, merge ? mergeReq : insertReq, SQL_NTS);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            FixedSizeArray<int64_t> keys(recordsNum);
            FixedSizeArray<int8_t> i8Fields(recordsNum);
            FixedSizeArray<int16_t> i16Fields(recordsNum);
            FixedSizeArray<int32_t> i32Fields(recordsNum);
            FixedSizeArray<char> strFields(recordsNum * 1024);
            FixedSizeArray<float> floatFields(recordsNum);
            FixedSizeArray<double> doubleFields(recordsNum);
            FixedSizeArray<bool> boolFields(recordsNum);
            FixedSizeArray<SQL_DATE_STRUCT> dateFields(recordsNum);
            FixedSizeArray<SQL_TIME_STRUCT> timeFields(recordsNum);
            FixedSizeArray<SQL_TIMESTAMP_STRUCT> timestampFields(recordsNum);
            FixedSizeArray<int8_t> i8ArrayFields(recordsNum * 42);

            FixedSizeArray<SQLLEN> strFieldsLen(recordsNum);
            FixedSizeArray<SQLLEN> i8ArrayFieldsLen(recordsNum);

            BOOST_TEST_CHECKPOINT("Filling param data");

            for (int i = 0; i < recordsNum; ++i)
            {
                int seed = from + i;

                keys[i] = seed;
                i8Fields[i] = GetTestI8Field(seed);
                i16Fields[i] = GetTestI16Field(seed);
                i32Fields[i] = GetTestI32Field(seed);

                std::string val = GetTestString(seed);
                CopyStringToBuffer(strFields.GetData() + 1024 * i, val, 1023);
                strFieldsLen[i] = val.size();

                floatFields[i] = GetTestFloatField(seed);
                doubleFields[i] = GetTestDoubleField(seed);
                boolFields[i] = GetTestBoolField(seed);

                GetTestDateField(seed, dateFields[i]);
                GetTestTimeField(seed, timeFields[i]);
                GetTestTimestampField(seed, timestampFields[i]);

                GetTestI8ArrayField(seed, &i8ArrayFields[i*42], 42);
                i8ArrayFieldsLen[i] = 42;
            }

            SQLULEN setsProcessed = 0;

            BOOST_TEST_CHECKPOINT("Setting processed pointer");
            ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &setsProcessed, SQL_IS_POINTER);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding keys");
            ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, keys.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding i8Fields");
            ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 0, 0, i8Fields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding i16Fields");
            ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 0, 0, i16Fields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding i32Fields");
            ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, i32Fields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding strFields");
            ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1024, 0, strFields.GetData(), 1024, strFieldsLen.GetData());

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding floatFields");
            ret = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 0, 0, floatFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding doubleFields");
            ret = SQLBindParameter(stmt, 7, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, doubleFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding boolFields");
            ret = SQLBindParameter(stmt, 8, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, 0, 0, boolFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding dateFields");
            ret = SQLBindParameter(stmt, 9, SQL_PARAM_INPUT, SQL_C_TYPE_DATE, SQL_TYPE_DATE, 0, 0, dateFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding timeFields");
            ret = SQLBindParameter(stmt, 10, SQL_PARAM_INPUT, SQL_C_TYPE_TIME, SQL_TYPE_TIME, 0, 0, timeFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding timestampFields");
            ret = SQLBindParameter(stmt, 11, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, 0, 0, timestampFields.GetData(), 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Binding i8ArrayFields");
            ret = SQLBindParameter(stmt, 12, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 42, 0, i8ArrayFields.GetData(), 42, i8ArrayFieldsLen.GetData());

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Setting paramset size");
            ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE,
                 reinterpret_cast<SQLPOINTER>(static_cast<ptrdiff_t>(recordsNum)), 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Executing query");
            ret = SQLExecute(stmt);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            SQLLEN totallyAffected = 0;

            do
            {
                SQLLEN affected = 0;
                ret = SQLRowCount(stmt, &affected);

                if (!SQL_SUCCEEDED(ret))
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

                totallyAffected += affected;

                BOOST_TEST_CHECKPOINT("Getting next result set");

                ret = SQLMoreResults(stmt);

                if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
            }
            while (ret != SQL_NO_DATA);

            BOOST_CHECK_EQUAL(totallyAffected, expectedToAffect);

            BOOST_TEST_CHECKPOINT("Resetting parameters.");
            ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_TEST_CHECKPOINT("Setting paramset size");
            ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(1), 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            return static_cast<int>(setsProcessed);
        }

        void OdbcTestSuite::InsertBatchSelect(int recordsNum)
        {
            Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

            // Inserting values.
            int inserted = InsertTestBatch(0, recordsNum, recordsNum);

            BOOST_REQUIRE_EQUAL(inserted, recordsNum);

            int64_t key = 0;
            char strField[1024] = {0};
            SQLLEN strFieldLen = 0;

            // Binding columns.
            SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Binding columns.
            ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Just selecting everything to make sure everything is OK
            SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

            ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            int selectedRecordsNum = 0;

            ret = SQL_SUCCESS;

            while (ret == SQL_SUCCESS)
            {
                ret = SQLFetch(stmt);

                if (ret == SQL_NO_DATA)
                    break;

                if (!SQL_SUCCEEDED(ret))
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

                std::string expectedStr = GetTestString(selectedRecordsNum);
                int64_t expectedKey = selectedRecordsNum;

                BOOST_CHECK_EQUAL(key, expectedKey);

                BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

                ++selectedRecordsNum;
            }

            BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
        }

        void OdbcTestSuite::InsertNonFullBatchSelect(int recordsNum, int splitAt)
        {
            Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

            std::vector<SQLUSMALLINT> statuses(recordsNum, 42);

            // Binding statuses array.
            SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, &statuses[0], 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Inserting values.
            int setsProcessed = InsertTestBatch(splitAt, recordsNum, recordsNum - splitAt);

            BOOST_REQUIRE_EQUAL(setsProcessed, recordsNum - splitAt);

            for (int i = 0; i < recordsNum - splitAt; ++i)
                BOOST_REQUIRE_EQUAL(statuses[i], SQL_PARAM_SUCCESS);

            setsProcessed = InsertTestBatch(0, recordsNum, splitAt);

            BOOST_REQUIRE_EQUAL(setsProcessed, recordsNum);

            for (int i = 0; i < splitAt; ++i)
                BOOST_REQUIRE_EQUAL(statuses[i], SQL_PARAM_SUCCESS);

            for (int i = splitAt; i < recordsNum; ++i)
                BOOST_REQUIRE_EQUAL(statuses[i], SQL_PARAM_ERROR);

            int64_t key = 0;
            char strField[1024] = {0};
            SQLLEN strFieldLen = 0;

            // Binding columns.
            ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Binding columns.
            ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            // Just selecting everything to make sure everything is OK
            SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

            ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            int selectedRecordsNum = 0;

            ret = SQL_SUCCESS;

            while (ret == SQL_SUCCESS)
            {
                ret = SQLFetch(stmt);

                if (ret == SQL_NO_DATA)
                    break;

                if (!SQL_SUCCEEDED(ret))
                    BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

                std::string expectedStr = GetTestString(selectedRecordsNum);
                int64_t expectedKey = selectedRecordsNum;

                BOOST_CHECK_EQUAL(key, expectedKey);

                BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

                ++selectedRecordsNum;
            }

            BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
        }
    }
}
