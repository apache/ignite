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

#include <cstdio>

#include <vector>
#include <string>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct CursorBindingTestSuiteFixture : public odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-test.xml", name);
    }

    /**
     * Constructor.
     */
    CursorBindingTestSuiteFixture() :
        testCache(0)
    {
        grid = StartAdditionalNode("NodeMain");

        testCache = grid.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Destructor.
     */
    virtual ~CursorBindingTestSuiteFixture()
    {
        // No-op.
    }

    /** Node started during the test. */
    Ignite grid;

    /** Test cache instance. */
    Cache<int64_t, TestType> testCache;
};

BOOST_FIXTURE_TEST_SUITE(CursorBindingTestSuite, CursorBindingTestSuiteFixture)


#define CHECK_TEST_VALUES(idx, testIdx)                                                                             \
    do {                                                                                                            \
        BOOST_TEST_CONTEXT("Test idx: " << testIdx)                                                                 \
        {                                                                                                           \
            BOOST_CHECK(RowStatus[idx] == SQL_ROW_SUCCESS || RowStatus[idx] == SQL_ROW_SUCCESS_WITH_INFO);          \
                                                                                                                    \
            BOOST_CHECK(i8FieldsInd[idx] != SQL_NULL_DATA);                                                         \
            BOOST_CHECK(i16FieldsInd[idx] != SQL_NULL_DATA);                                                        \
            BOOST_CHECK(i32FieldsInd[idx] != SQL_NULL_DATA);                                                        \
            BOOST_CHECK(strFieldsLen[idx] != SQL_NULL_DATA);                                                        \
            BOOST_CHECK(floatFields[idx] != SQL_NULL_DATA);                                                         \
            BOOST_CHECK(doubleFieldsInd[idx] != SQL_NULL_DATA);                                                     \
            BOOST_CHECK(boolFieldsInd[idx] != SQL_NULL_DATA);                                                       \
            BOOST_CHECK(dateFieldsInd[idx] != SQL_NULL_DATA);                                                       \
            BOOST_CHECK(timeFieldsInd[idx] != SQL_NULL_DATA);                                                       \
            BOOST_CHECK(timestampFieldsInd[idx] != SQL_NULL_DATA);                                                  \
            BOOST_CHECK(i8ArrayFieldsLen[idx] != SQL_NULL_DATA);                                                    \
                                                                                                                    \
            int8_t i8Field = static_cast<int8_t>(i8Fields[idx]);                                                    \
            int16_t i16Field = static_cast<int16_t>(i16Fields[idx]);                                                \
            int32_t i32Field = static_cast<int32_t>(i32Fields[idx]);                                                \
            std::string strField(reinterpret_cast<char*>(&strFields[idx][0]),                                       \
                static_cast<size_t>(strFieldsLen[idx]));                                                            \
            float floatField = static_cast<float>(floatFields[idx]);                                                \
            double doubleField = static_cast<double>(doubleFields[idx]);                                            \
            bool boolField = boolFields[idx] != 0;                                                                  \
                                                                                                                    \
            CheckTestI8Value(testIdx, i8Field);                                                                     \
            CheckTestI16Value(testIdx, i16Field);                                                                   \
            CheckTestI32Value(testIdx, i32Field);                                                                   \
            CheckTestStringValue(testIdx, strField);                                                                \
            CheckTestFloatValue(testIdx, floatField);                                                               \
            CheckTestDoubleValue(testIdx, doubleField);                                                             \
            CheckTestBoolValue(testIdx, boolField);                                                                 \
            CheckTestDateValue(testIdx, dateFields[idx]);                                                           \
            CheckTestTimeValue(testIdx, timeFields[idx]);                                                           \
            CheckTestTimestampValue(testIdx, timestampFields[idx]);                                                 \
            CheckTestI8ArrayValue(testIdx, reinterpret_cast<int8_t*>(i8ArrayFields[idx]),                           \
                static_cast<SQLLEN>(i8ArrayFieldsLen[idx]));                                                        \
        }                                                                                                           \
    } while (false)

BOOST_AUTO_TEST_CASE(TestCursorBindingColumnWise)
{
    enum { ROWS_COUNT = 15 };
    enum { ROW_ARRAY_SIZE = 10 };
    enum { BUFFER_SIZE = 1024 };

    StartAdditionalNode("Node2");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PAGE_SIZE=8");

    // Preloading data.

    InsertTestBatch(0, ROWS_COUNT, ROWS_COUNT);

    // Setting attributes.

    SQLUSMALLINT RowStatus[ROW_ARRAY_SIZE];
    SQLUINTEGER NumRowsFetched;

    SQLRETURN ret;

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, SQL_BIND_BY_COLUMN, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER*>(ROW_ARRAY_SIZE), 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, RowStatus, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &NumRowsFetched, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Binding collumns.

    SQLSCHAR i8Fields[ROW_ARRAY_SIZE] = {0};
    SQLLEN i8FieldsInd[ROW_ARRAY_SIZE];

    SQLSMALLINT i16Fields[ROW_ARRAY_SIZE] = {0};
    SQLLEN i16FieldsInd[ROW_ARRAY_SIZE];

    SQLINTEGER i32Fields[ROW_ARRAY_SIZE] = {0};
    SQLLEN i32FieldsInd[ROW_ARRAY_SIZE];

    SQLCHAR strFields[ROW_ARRAY_SIZE][BUFFER_SIZE];
    SQLLEN strFieldsLen[ROW_ARRAY_SIZE];

    SQLREAL floatFields[ROW_ARRAY_SIZE];
    SQLLEN floatFieldsInd[ROW_ARRAY_SIZE];

    SQLDOUBLE doubleFields[ROW_ARRAY_SIZE];
    SQLLEN doubleFieldsInd[ROW_ARRAY_SIZE];

    SQLCHAR boolFields[ROW_ARRAY_SIZE];
    SQLLEN boolFieldsInd[ROW_ARRAY_SIZE];

    SQL_DATE_STRUCT dateFields[ROW_ARRAY_SIZE];
    SQLLEN dateFieldsInd[ROW_ARRAY_SIZE];

    SQL_TIME_STRUCT timeFields[ROW_ARRAY_SIZE];
    SQLLEN timeFieldsInd[ROW_ARRAY_SIZE];

    SQL_TIMESTAMP_STRUCT timestampFields[ROW_ARRAY_SIZE];
    SQLLEN timestampFieldsInd[ROW_ARRAY_SIZE];

    SQLCHAR i8ArrayFields[ROW_ARRAY_SIZE][BUFFER_SIZE];
    SQLLEN i8ArrayFieldsLen[ROW_ARRAY_SIZE];

    ret = SQLBindCol(stmt, 1, SQL_C_STINYINT, i8Fields, 0, i8FieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 2, SQL_C_SSHORT, i16Fields, 0, i16FieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 3, SQL_C_LONG, i32Fields, 0, i32FieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 4, SQL_C_CHAR, strFields, BUFFER_SIZE, strFieldsLen);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 5, SQL_C_FLOAT, floatFields, 0, floatFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 6, SQL_C_DOUBLE, doubleFields, 0, doubleFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 7, SQL_C_BIT, boolFields, 0, boolFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 8, SQL_C_TYPE_DATE, dateFields, 0, dateFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 9, SQL_C_TYPE_TIME, timeFields, 0, timeFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 10, SQL_C_TYPE_TIMESTAMP, timestampFields, 0, timestampFieldsInd);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLBindCol(stmt, 11, SQL_C_BINARY, i8ArrayFields, BUFFER_SIZE, i8ArrayFieldsLen);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCHAR sql[] = "SELECT "
            "i8Field, i16Field, i32Field, strField, floatField, doubleField, "
            "boolField, dateField, timeField, timestampField, i8ArrayField "
            "FROM TestType "
            "ORDER BY _key";

    // Execute a statement to retrieve rows from the Orders table.
    ret = SQLExecDirect(stmt, sql, SQL_NTS);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    BOOST_CHECK_EQUAL(NumRowsFetched, (SQLUINTEGER)ROW_ARRAY_SIZE);

    for (int64_t i = 0; i < NumRowsFetched; i++)
    {
        CHECK_TEST_VALUES(i, static_cast<int>(i));
    }

    ret = SQLFetch(stmt);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    BOOST_CHECK_EQUAL(NumRowsFetched, ROWS_COUNT - ROW_ARRAY_SIZE);

    for (int64_t i = 0; i < NumRowsFetched; i++)
    {
        int64_t testIdx = i + ROW_ARRAY_SIZE;
        CHECK_TEST_VALUES(i, static_cast<int>(testIdx));
    }

    for (int64_t i = NumRowsFetched; i < ROW_ARRAY_SIZE; i++)
    {
        BOOST_TEST_INFO("Checking row status for row: " << i);
        BOOST_CHECK(RowStatus[i] == SQL_ROW_NOROW);
    }

    ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0);
    BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

    // Close the cursor.
    ret = SQLCloseCursor(stmt);
    ODBC_THROW_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
}

BOOST_AUTO_TEST_CASE(TestCursorBindingRowWise)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PAGE_SIZE=8");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, reinterpret_cast<SQLPOINTER*>(42), 0);

    BOOST_CHECK_EQUAL(ret, SQL_ERROR);

    CheckSQLStatementDiagnosticError("HYC00");
}

BOOST_AUTO_TEST_SUITE_END()
