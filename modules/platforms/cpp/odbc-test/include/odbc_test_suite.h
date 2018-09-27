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
             * Establish connection to node.
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
             * for the strFields, which are generated using getTestString().
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
             * Get test string.
             *
             * @param ind Index.
             * @return Corresponding test string.
             */
            static std::string getTestString(int64_t ind);

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
