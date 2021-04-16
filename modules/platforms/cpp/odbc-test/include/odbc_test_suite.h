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

#ifndef ODBC_TEST_ODBC_TEST_SUITE
#define ODBC_TEST_ODBC_TEST_SUITE

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <boost/test/unit_test.hpp>

#ifndef BOOST_TEST_CONTEXT
#   define BOOST_TEST_CONTEXT(...)
#endif

#ifndef BOOST_TEST_INFO
#   define BOOST_TEST_INFO(...)
#endif

#include <string>

#include "ignite/ignite.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Test setup fixture.
         */
        struct OdbcTestSuite
        {
            /**
             * Prepare environment.
             */
            void Prepare();

            /**
             * Establish connection to node using provided handles.
             *
             * @param conn Connection.
             * @param statement Statement to allocate.
             * @param connectStr Connection string.
             */
            void Connect(SQLHDBC& conn, SQLHSTMT& statement, const std::string& connectStr);

            /**
             * Establish connection to node using default handles.
             *
             * @param connectStr Connection string.
             */
            void Connect(const std::string& connectStr);

            /**
             * Expect connection to be rejected by the node.
             *
             * @param connectStr Connection string.
             * @return SQL State.
             */
            std::string ExpectConnectionReject(const std::string& connectStr);

            /**
             * Disconnect.
             */
            void Disconnect();

            /**
             * Clean up.
             */
            void CleanUp();

            /**
             * Start additional with the specified name and config.
             *
             * @param cfg Config path.
             * @param name Instance name.
             */
            static Ignite StartTestNode(const char* cfg, const char* name);

            /**
             * Constructor.
             */
            OdbcTestSuite();

            /**
             * Destructor.
             */
            virtual ~OdbcTestSuite();

            /**
             * Insert requested number of TestType values with all defaults except
             * for the strFields, which are generated using GetTestString().
             *
             * @param recordsNum Number of records to insert.
             * @param merge Set to true to use merge instead.
             */
            void InsertTestStrings(int recordsNum, bool merge = false);

            /**
             * Insert requested number of TestType values in a batch.
             *
             * @param from Index to start from.
             * @param to Index to stop.
             * @param expectedToAffect Expected number of affected records.
             * @param merge Set to true to use merge instead of insert.
             * @return Records inserted.
             */
            int InsertTestBatch(int from, int to, int expectedToAffect, bool merge = false);

            /**
             * Insert requested number of TestType values in a batch,
             * select them and check all the values.
             *
             * @param recordsNum Number of records.
             */
            void InsertBatchSelect(int recordsNum);

            /**
             * Insert values in two batches, select them and check all the values.
             * @param recordsNum Number of records.
             * @param splitAt Point where two batches are separated.
             */
            void InsertNonFullBatchSelect(int recordsNum, int splitAt);

            /**
             * Get test i8Field.
             *
             * @param idx Index.
             * @return Corresponding i8Field value.
             */
            static int8_t GetTestI8Field(int64_t idx);

            /**
             * Check i8Field test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestI8Value(int idx, int8_t value);

            /**
             * Get test i16Field.
             *
             * @param idx Index.
             * @return Corresponding i16Field value.
             */
            static int16_t GetTestI16Field(int64_t idx);

            /**
             * Check i16Field test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestI16Value(int idx, int16_t value);

            /**
             * Get test i32Field.
             *
             * @param idx Index.
             * @return Corresponding i32Field value.
             */
            static int32_t GetTestI32Field(int64_t idx);

            /**
             * Check i32Field test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestI32Value(int idx, int32_t value);

            /**
             * Get test string.
             *
             * @param idx Index.
             * @return Corresponding test string.
             */
            static std::string GetTestString(int64_t idx);

            /**
             * Check strField test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestStringValue(int idx, const std::string& value);

            /**
             * Get test floatField.
             *
             * @param idx Index.
             * @return Corresponding floatField value.
             */
            static float GetTestFloatField(int64_t idx);

            /**
             * Check floatField test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestFloatValue(int idx, float value);

            /**
             * Get test doubleField.
             *
             * @param idx Index.
             * @return Corresponding doubleField value.
             */
            static double GetTestDoubleField(int64_t idx);

            /**
             * Check doubleField test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestDoubleValue(int idx, double value);

            /**
             * Get test boolField.
             *
             * @param idx Index.
             * @return Corresponding boolField value.
             */
            static bool GetTestBoolField(int64_t idx);

            /**
             * Check boolField test value.
             * @param idx Index.
             * @param value Value to test.
             */
            static void CheckTestBoolValue(int idx, bool value);

            /**
             * Get test dateField.
             *
             * @param idx Index.
             * @param val Output value.
             */
            static void GetTestDateField(int64_t idx, SQL_DATE_STRUCT& val);

            /**
             * Check dateField test value.
             *
             * @param idx Index.
             * @param val Value to test.
             */
            static void CheckTestDateValue(int idx, const SQL_DATE_STRUCT& val);

            /**
             * Get test timeField.
             *
             * @param idx Index.
             * @param val Output value.
             */
            static void GetTestTimeField(int64_t idx, SQL_TIME_STRUCT& val);

            /**
             * Check timeField test value.
             *
             * @param idx Index.
             * @param val Value to test.
             */
            static void CheckTestTimeValue(int idx, const SQL_TIME_STRUCT& val);

            /**
             * Get test timestampField.
             *
             * @param idx Index.
             * @param val Output value.
             */
            static void GetTestTimestampField(int64_t idx, SQL_TIMESTAMP_STRUCT& val);

            /**
             * Check timestampField test value.
             *
             * @param idx Index.
             * @param val Value to test.
             */
            static void CheckTestTimestampValue(int idx, const SQL_TIMESTAMP_STRUCT& val);

            /**
             * Get test i8ArrayField.
             *
             * @param idx Index.
             * @param val Output value.
             * @param valLen Value length.
             */
            static void GetTestI8ArrayField(int64_t idx, int8_t* val, size_t valLen);

            /**
             * Check i8ArrayField test value.
             *
             * @param idx Index.
             * @param val Value to test.
             * @param valLen Value length.
             */
            static void CheckTestI8ArrayValue(int idx, const int8_t* val, size_t valLen);

            /**
             * Check that SQL error has expected SQL state.
             *
             * @param handleType Handle type.
             * @param handle Handle.
             * @param expectSqlState Expected state.
             */
            void CheckSQLDiagnosticError(int16_t handleType, SQLHANDLE handle, const std::string& expectSqlState);

            /**
             * Check that statement SQL error has expected SQL state.
             *
             * @param expectSqlState Expected state.
             */
            void CheckSQLStatementDiagnosticError(const std::string& expectSqlState);

            /**
             * Check that connection SQL error has expected SQL state.
             *
             * @param expectSqlState Expected state.
             */
            void CheckSQLConnectionDiagnosticError(const std::string& expectSqlState);

            /**
             * Convert string to vector of SQLCHARs.
             *
             * @param qry Query.
             * @return Corresponding vector.
             */
            static std::vector<SQLCHAR> MakeQuery(const std::string& qry);

            /**
             * Performs SQL query.
             *
             * @param qry Query.
             * @return Result.
             */
            SQLRETURN ExecQuery(const std::string& qry);

            /**
             * Prepares SQL query.
             *
             * @param qry Query.
             * @return Result.
             */
            SQLRETURN PrepareQuery(const std::string& qry);

            /** ODBC Environment. */
            SQLHENV env;

            /** ODBC Connect. */
            SQLHDBC dbc;

            /** ODBC Statement. */
            SQLHSTMT stmt;
        };
    }
}

#endif //ODBC_TEST_ODBC_TEST_SUITE
