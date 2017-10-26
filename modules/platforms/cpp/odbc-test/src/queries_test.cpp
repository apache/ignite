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

#include <vector>
#include <string>
#include <algorithm>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/regex.hpp>
#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/common/fixed_size_array.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/binary/binary_object.h"

#include "test_type.h"
#include "complex_type.h"
#include "test_utils.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;
using namespace ignite::binary;
using namespace ignite::impl::binary;
using namespace ignite::impl::interop;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct QueriesTestSuiteFixture 
{
    /**
     * Establish connection to node.
     *
     * @param connectStr Connection string.
     */
    void Connect(const std::string& connectStr)
    {
        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        BOOST_REQUIRE(env != NULL);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        BOOST_REQUIRE(dbc != NULL);

        // Connect string
        std::vector<SQLCHAR> connectStr0;

        connectStr0.reserve(connectStr.size() + 1);
        std::copy(connectStr.begin(), connectStr.end(), std::back_inserter(connectStr0));

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
        {
            Ignition::Stop(grid.GetName(), true);

            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));
        }

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        BOOST_REQUIRE(stmt != NULL);
    }

    void Disconnect()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    static Ignite StartAdditionalNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        return StartNode("queries-test-noodbc-32.xml", name);
#else
        return StartNode("queries-test-noodbc.xml", name);
#endif
    }

    /**
     * Constructor.
     */
    QueriesTestSuiteFixture() :
        cache1(0),
        cache2(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
#ifdef IGNITE_TESTS_32
        grid = StartNode("queries-test-32.xml", "NodeMain");
#else
        grid = StartNode("queries-test.xml", "NodeMain");
#endif

        cache1 = grid.GetCache<int64_t, TestType>("cache");
        cache2 = grid.GetCache<int64_t, ComplexType>("cache2");
    }

    /**
     * Destructor.
     */
    ~QueriesTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
    }

    template<typename T>
    void CheckTwoRowsInt(SQLSMALLINT type)
    {
        Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

        SQLRETURN ret;

        TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
            MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

        TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
            MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 456));

        cache1.Put(1, in1);
        cache1.Put(2, in2);

        const size_t columnsCnt = 12;

        T columns[columnsCnt] = { 0 };

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        {
            ret = SQLBindCol(stmt, i + 1, type, &columns[i], sizeof(columns[i]), 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        char request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
            "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

        ret = SQLExecDirect(stmt, reinterpret_cast<SQLCHAR*>(request), SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(columns[0], 1);
        BOOST_CHECK_EQUAL(columns[1], 2);
        BOOST_CHECK_EQUAL(columns[2], 3);
        BOOST_CHECK_EQUAL(columns[3], 4);
        BOOST_CHECK_EQUAL(columns[4], 5);
        BOOST_CHECK_EQUAL(columns[5], 6);
        BOOST_CHECK_EQUAL(columns[6], 7);
        BOOST_CHECK_EQUAL(columns[7], 1);
        BOOST_CHECK_EQUAL(columns[8], 0);
        BOOST_CHECK_EQUAL(columns[9], 0);
        BOOST_CHECK_EQUAL(columns[10], 0);
        BOOST_CHECK_EQUAL(columns[11], 0);

        SQLLEN columnLens[columnsCnt] = { 0 };

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        {
            ret = SQLBindCol(stmt, i + 1, type, &columns[i], sizeof(columns[i]), &columnLens[i]);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        ret = SQLFetch(stmt);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(columns[0], 8);
        BOOST_CHECK_EQUAL(columns[1], 7);
        BOOST_CHECK_EQUAL(columns[2], 6);
        BOOST_CHECK_EQUAL(columns[3], 5);
        BOOST_CHECK_EQUAL(columns[4], 4);
        BOOST_CHECK_EQUAL(columns[5], 3);
        BOOST_CHECK_EQUAL(columns[6], 2);
        BOOST_CHECK_EQUAL(columns[7], 0);
        BOOST_CHECK_EQUAL(columns[8], 0);
        BOOST_CHECK_EQUAL(columns[9], 0);
        BOOST_CHECK_EQUAL(columns[10], 0);
        BOOST_CHECK_EQUAL(columns[11], 0);

        BOOST_CHECK_EQUAL(columnLens[0], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[1], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[2], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[3], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[4], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[5], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[6], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[7], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[8], SQL_NO_TOTAL);
        BOOST_CHECK_EQUAL(columnLens[9], SQL_NO_TOTAL);
        BOOST_CHECK_EQUAL(columnLens[10], SQL_NO_TOTAL);
        BOOST_CHECK_EQUAL(columnLens[11], SQL_NO_TOTAL);

        ret = SQLFetch(stmt);
        BOOST_CHECK(ret == SQL_NO_DATA);
    }

    void CheckParamsNum(const std::string& req, SQLSMALLINT expectedParamsNum)
    {
        std::vector<SQLCHAR> req0(req.begin(), req.end());

        SQLRETURN ret = SQLPrepare(stmt, &req0[0], static_cast<SQLINTEGER>(req0.size()));

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLSMALLINT paramsNum = -1;

        ret = SQLNumParams(stmt, &paramsNum);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(paramsNum, expectedParamsNum);
    }

    int CountRows(SQLHSTMT stmt)
    {
        int res = 0;

        SQLRETURN ret = SQL_SUCCESS;

        while (ret == SQL_SUCCESS)
        {
            ret = SQLFetch(stmt);

            if (ret == SQL_NO_DATA)
                break;

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            ++res;
        }

        return res;
    }

    static std::string getTestString(int64_t ind)
    {
        std::stringstream builder;

        builder << "String#" << ind;

        return builder.str();
    }

    /**
     * Insert requested number of TestType values with all defaults except
     * for the strFields, which are generated using getTestString().
     *
     * @param recordsNum Number of records to insert.
     * @param merge Set to true to use merge instead.
     */
    void InsertTestStrings(int recordsNum, bool merge = false)
    {
        SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(?, ?)";
        SQLCHAR mergeReq[] = "MERGE INTO TestType(_key, strField) VALUES(?, ?)";

        SQLRETURN ret;

        ret = SQLPrepare(stmt, merge ? mergeReq : insertReq, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int64_t key = 0;
        char strField[1024] = { 0 };
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
            std::string val = getTestString(i);

            strncpy(strField, val.c_str(), sizeof(strField));
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

    /**
     * Insert requested number of TestType values in a batch.
     *
     * @param from Index to start from.
     * @param to Index to stop.
     * @param expectedToAffect Expected number of affected records.
     * @param merge Set to true to use merge instead of insert.
     * @return Records inserted.
     */
    int InsertTestBatch(int from, int to, int expectedToAffect, bool merge = false)
    {
        SQLCHAR insertReq[] = "INSERT "
            "INTO TestType(_key, i8Field, i16Field, i32Field, strField, floatField, doubleField, boolField, dateField, "
            "timeField, timestampField, i8ArrayField) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SQLCHAR mergeReq[] = "MERGE "
            "INTO TestType(_key, i8Field, i16Field, i32Field, strField, floatField, doubleField, boolField, dateField, "
            "timeField, timestampField, i8ArrayField) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SQLRETURN ret;

        int recordsNum = to - from;

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

        BOOST_CHECKPOINT("Filling param data");

        for (int i = 0; i < recordsNum; ++i)
        {
            int seed = from + i;

            keys[i] = seed;
            i8Fields[i] = seed * 8;
            i16Fields[i] = seed * 16;
            i32Fields[i] = seed * 32;

            std::string val = getTestString(seed);
            strncpy(strFields.GetData() + 1024 * i, val.c_str(), 1023);
            strFieldsLen[i] = val.size();

            floatFields[i] = seed * 0.5f;
            doubleFields[i] = seed * 0.25f;
            boolFields[i] = seed % 2 == 0;

            dateFields[i].year = 2017 + seed / 365;
            dateFields[i].month = ((seed / 28) % 12) + 1;
            dateFields[i].day = (seed % 28) + 1;

            timeFields[i].hour = (seed / 3600) % 24;
            timeFields[i].minute = (seed / 60) % 60;
            timeFields[i].second = seed % 60;

            timestampFields[i].year = dateFields[i].year;
            timestampFields[i].month = dateFields[i].month;
            timestampFields[i].day = dateFields[i].day;
            timestampFields[i].hour = timeFields[i].hour;
            timestampFields[i].minute = timeFields[i].minute;
            timestampFields[i].second = timeFields[i].second;
            timestampFields[i].fraction = std::abs(seed * 914873) % 1000000000;

            for (int j = 0; j < 42; ++j)
                i8ArrayFields[i * 42 + j] = seed * 42 + j;
            i8ArrayFieldsLen[i] = 42;
        }

        SQLULEN setsProcessed = 0;

        BOOST_CHECKPOINT("Setting processed pointer");
        ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &setsProcessed, SQL_IS_POINTER);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding keys");
        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, keys.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding i8Fields");
        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_STINYINT, SQL_TINYINT, 0, 0, i8Fields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding i16Fields");
        ret = SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_SSHORT, SQL_SMALLINT, 0, 0, i16Fields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding i32Fields");
        ret = SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, i32Fields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding strFields");
        ret = SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1024, 0, strFields.GetData(), 1024, strFieldsLen.GetData());

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding floatFields");
        ret = SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_FLOAT, 0, 0, floatFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding doubleFields");
        ret = SQLBindParameter(stmt, 7, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 0, 0, doubleFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding boolFields");
        ret = SQLBindParameter(stmt, 8, SQL_PARAM_INPUT, SQL_C_BIT, SQL_BIT, 0, 0, boolFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding dateFields");
        ret = SQLBindParameter(stmt, 9, SQL_PARAM_INPUT, SQL_C_DATE, SQL_DATE, 0, 0, dateFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding timeFields");
        ret = SQLBindParameter(stmt, 10, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIME, 0, 0, timeFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding timestampFields");
        ret = SQLBindParameter(stmt, 11, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP, 0, 0, timestampFields.GetData(), 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Binding i8ArrayFields");
        ret = SQLBindParameter(stmt, 12, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 42, 0, i8ArrayFields.GetData(), 42, i8ArrayFieldsLen.GetData());

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Setting paramset size");
        ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(recordsNum), 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Executing query");
        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(affected, expectedToAffect);

        BOOST_CHECKPOINT("Getting next result set");
        ret = SQLMoreResults(stmt);

        if (ret != SQL_NO_DATA)
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Resetting parameters.");
        ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECKPOINT("Setting paramset size");
        ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, reinterpret_cast<SQLPOINTER>(1), 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        return static_cast<int>(setsProcessed);
    }

    void InsertBatchSelect(int recordsNum)
    {
        Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

        // Inserting values.
        int inserted = InsertTestBatch(0, recordsNum, recordsNum);

        BOOST_REQUIRE_EQUAL(inserted, recordsNum);

        int64_t key = 0;
        char strField[1024] = { 0 };
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

            std::string expectedStr = getTestString(selectedRecordsNum);
            int64_t expectedKey = selectedRecordsNum;

            BOOST_CHECK_EQUAL(key, expectedKey);

            BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

            ++selectedRecordsNum;
        }

        BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
    }

    void InsertNonFullBatchSelect(int recordsNum, int splitAt)
    {
        Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

        // Inserting values.
        int inserted = InsertTestBatch(splitAt, recordsNum, recordsNum - splitAt);

        BOOST_REQUIRE_EQUAL(inserted, recordsNum - splitAt);

        inserted = InsertTestBatch(0, recordsNum, splitAt);

        BOOST_REQUIRE_EQUAL(inserted, splitAt);

        int64_t key = 0;
        char strField[1024] = { 0 };
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

            std::string expectedStr = getTestString(selectedRecordsNum);
            int64_t expectedKey = selectedRecordsNum;

            BOOST_CHECK_EQUAL(key, expectedKey);

            BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

            ++selectedRecordsNum;
        }

        BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
    }

    /** Node started during the test. */
    Ignite grid;

    /** Frist cache instance. */
    Cache<int64_t, TestType> cache1;

    /** Second cache instance. */
    Cache<int64_t, ComplexType> cache2;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

BOOST_FIXTURE_TEST_SUITE(QueriesTestSuite, QueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestLegacyConnection)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_1_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.1.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_1_5)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.1.5");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt8)
{
    CheckTwoRowsInt<signed char>(SQL_C_STINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint8)
{
    CheckTwoRowsInt<unsigned char>(SQL_C_UTINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt16)
{
    CheckTwoRowsInt<signed short>(SQL_C_SSHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint16)
{
    CheckTwoRowsInt<unsigned short>(SQL_C_USHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt32)
{
    CheckTwoRowsInt<signed long>(SQL_C_SLONG);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint32)
{
    CheckTwoRowsInt<unsigned long>(SQL_C_ULONG);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt64)
{
    CheckTwoRowsInt<int64_t>(SQL_C_SBIGINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint64)
{
    CheckTwoRowsInt<uint64_t>(SQL_C_UBIGINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsString)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
        MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 999999999));

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    const size_t columnsCnt = 12;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0008-0000-000000000009");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1987-06-05 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "8");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "0");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0001-0000-000000000000");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1976-01-12 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "00:08:59");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1978-08-21 23:13:45");

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowString)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    const size_t columnsCnt = 12;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE] = { 0 };

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, CAST('12:48:12' AS TIME), timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0008-0000-000000000009");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1987-06-05 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowStringLen)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    const size_t columnsCnt = 12;

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, 0, 0, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowObject)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    ComplexType obj;

    obj.i32Field = 123;
    obj.strField = "Some string";

    obj.objField.f1 = 54321;
    obj.objField.f2 = "Hello Ignite";

    cache2.Put(1, obj);

    int64_t column1 = 0;
    int8_t column2[ODBC_BUFFER_SIZE] = { 0 };
    char column3[ODBC_BUFFER_SIZE] = { 0 };

    SQLLEN column1Len = sizeof(column1);
    SQLLEN column2Len = sizeof(column2);
    SQLLEN column3Len = sizeof(column3);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &column1, column1Len, &column1Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_BINARY, &column2, column2Len, &column2Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_CHAR, &column3, column3Len, &column3Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT i32Field, objField, strField FROM ComplexType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, obj.i32Field);
    BOOST_CHECK_EQUAL(column3, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestDataAtExecution)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
        MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 999999999));

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    const size_t columnsCnt = 12;

    SQLLEN columnLens[columnsCnt] = { 0 };
    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType "
        "WHERE i32Field = ? AND strField = ?";

    ret = SQLPrepare(stmt, request, SQL_NTS);

    SQLLEN ind1 = 1;
    SQLLEN ind2 = 2;

    SQLLEN len1 = SQL_DATA_AT_EXEC;
    SQLLEN len2 = SQL_LEN_DATA_AT_EXEC(static_cast<SQLLEN>(in1.strField.size()));

    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 100, 100, &ind2, sizeof(ind2), &len2);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    void* oind;

    ret = SQLParamData(stmt, &oind);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(stmt, &in1.i32Field, 0);
    else if (oind == &ind2)
        ret = SQLPutData(stmt, (SQLPOINTER)in1.strField.c_str(), (SQLLEN)in1.strField.size());
    else
        BOOST_FAIL("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLParamData(stmt, &oind);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(stmt, &in1.i32Field, 0);
    else if (oind == &ind2)
        ret = SQLPutData(stmt, (SQLPOINTER)in1.strField.c_str(), (SQLLEN)in1.strField.size());
    else
        BOOST_FAIL("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLParamData(stmt, &oind);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0008-0000-000000000009");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1987-06-05 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestNullFields)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType inNull;

    inNull.allNulls = true;

    cache1.Put(1, in);
    cache1.Put(2, inNull);
    cache1.Put(3, in);

    const size_t columnsCnt = 11;

    SQLLEN columnLens[columnsCnt] = { 0 };

    int8_t i8Column;
    int16_t i16Column;
    int32_t i32Column;
    int64_t i64Column;
    char strColumn[ODBC_BUFFER_SIZE];
    float floatColumn;
    double doubleColumn;
    bool boolColumn;
    SQL_DATE_STRUCT dateColumn;
    SQL_TIME_STRUCT timeColumn;
    SQL_TIMESTAMP_STRUCT timestampColumn;

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_STINYINT, &i8Column, 0, &columnLens[0]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_SSHORT, &i16Column, 0, &columnLens[1]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &i32Column, 0, &columnLens[2]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 4, SQL_C_SBIGINT, &i64Column, 0, &columnLens[3]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 5, SQL_C_CHAR, &strColumn, ODBC_BUFFER_SIZE, &columnLens[4]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 6, SQL_C_FLOAT, &floatColumn, 0, &columnLens[5]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 7, SQL_C_DOUBLE, &doubleColumn, 0, &columnLens[6]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 8, SQL_C_BIT, &boolColumn, 0, &columnLens[7]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 9, SQL_C_DATE, &dateColumn, 0, &columnLens[8]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 10, SQL_C_TIME, &timeColumn, 0, &columnLens[9]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 11, SQL_C_TIMESTAMP, &timestampColumn, 0, &columnLens[10]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, dateField, timeField, timestampField FROM TestType "
        "ORDER BY _key";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Fetching the first non-null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are not null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_NE(columnLens[i], SQL_NULL_DATA);

    // Fetching null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_EQUAL(columnLens[i], SQL_NULL_DATA);

    // Fetching the last non-null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are not null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_NE(columnLens[i], SQL_NULL_DATA);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestDistributedJoins)
{
    // Starting additional node.
    Ignite node1 = StartAdditionalNode("Node1");
    Ignite node2 = StartAdditionalNode("Node2");

    const int entriesNum = 1000;

    // Filling cache with data.
    for (int i = 0; i < entriesNum; ++i)
    {
        TestType entry;

        entry.i32Field = i;
        entry.i64Field = entriesNum - i - 1;

        cache1.Put(i, entry);
    }

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    const size_t columnsCnt = 2;

    SQLBIGINT columns[columnsCnt] = { 0 };

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_SLONG, &columns[i], 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] =
        "SELECT T0.i32Field, T1.i64Field FROM TestType AS T0 "
        "INNER JOIN TestType AS T1 "
        "ON (T0.i32Field = T1.i64Field)";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int rowsNum = CountRows(stmt);

    BOOST_CHECK_GT(rowsNum, 0);
    BOOST_CHECK_LT(rowsNum, entriesNum);

    Disconnect();

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;DISTRIBUTED_JOINS=true;");

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_SLONG, &columns[i], 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    rowsNum = CountRows(stmt);

    BOOST_CHECK_EQUAL(rowsNum, entriesNum);
}

BOOST_AUTO_TEST_CASE(TestInsertSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024] = { 0 };
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

        std::string expectedStr = getTestString(selectedRecordsNum);
        int64_t expectedKey = selectedRecordsNum + 1;

        BOOST_CHECK_EQUAL(key, expectedKey);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertUpdateSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024] = { 0 };
    SQLLEN strFieldLen = 0;

    SQLCHAR updateReq[] = "UPDATE TestType SET strField = 'Updated value' WHERE _key = 42";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

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

        int64_t expectedKey = selectedRecordsNum + 1;
        std::string expectedStr;

        BOOST_CHECK_EQUAL(key, expectedKey);

        if (expectedKey == 42)
            expectedStr = "Updated value";
        else
            expectedStr = getTestString(selectedRecordsNum);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertDeleteSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024] = { 0 };
    SQLLEN strFieldLen = 0;

    SQLCHAR updateReq[] = "DELETE FROM TestType WHERE (_key % 2) = 1";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

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

        int64_t expectedKey = (selectedRecordsNum + 1) * 2;
        std::string expectedStr = getTestString(expectedKey - 1);

        BOOST_CHECK_EQUAL(key, expectedKey);
        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum / 2, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertMergeSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum / 2);

    // Merging values.
    InsertTestStrings(recordsNum, true);

    int64_t key = 0;
    char strField[1024] = { 0 };
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

        std::string expectedStr = getTestString(selectedRecordsNum);
        int64_t expectedKey = selectedRecordsNum + 1;

        BOOST_CHECK_EQUAL(key, expectedKey);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2)
{
    InsertBatchSelect(2);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect100)
{
    InsertBatchSelect(100);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1000)
{
    InsertBatchSelect(1000);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1023)
{
    InsertBatchSelect(1023);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1024)
{
    InsertBatchSelect(1024);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1025)
{
    InsertBatchSelect(1025);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2000)
{
    InsertBatchSelect(2000);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2047)
{
    InsertBatchSelect(2047);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2048)
{
    InsertBatchSelect(2048);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2049)
{
    InsertBatchSelect(2049);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect900)
{
    InsertNonFullBatchSelect(900, 42);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect1500)
{
    InsertNonFullBatchSelect(1500, 100);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect4500)
{
    InsertNonFullBatchSelect(4500, 1500);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect4096)
{
    InsertNonFullBatchSelect(4096, 1024);
}

template<size_t n, size_t k>
void CheckMeta(char columns[n][k], SQLLEN columnsLen[n])
{
    std::string catalog(columns[0], columnsLen[0]);
    std::string schema(columns[1], columnsLen[1]);
    std::string table(columns[2], columnsLen[2]);
    std::string tableType(columns[3], columnsLen[3]);

    BOOST_CHECK_EQUAL(catalog, std::string(""));
    BOOST_CHECK_EQUAL(tableType, std::string("TABLE"));
    BOOST_CHECK_EQUAL(columnsLen[4], SQL_NULL_DATA);

    if (schema == "\"cache\"")
    {
        BOOST_CHECK_EQUAL(table, std::string("TestType"));
    }
    else if (schema == "\"cache2\"")
    {
        BOOST_CHECK_EQUAL(table, std::string("ComplexType"));
    }
    else
    {
        BOOST_FAIL("Unknown schema: " + schema);
    }
}

BOOST_AUTO_TEST_CASE(TestTablesMeta)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    enum { COLUMNS_NUM = 5 };

    // Five collumns: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
    char columns[COLUMNS_NUM][ODBC_BUFFER_SIZE];
    SQLLEN columnsLen[COLUMNS_NUM];

    // Binding columns.
    for (size_t i = 0; i < COLUMNS_NUM; ++i)
    {
        columnsLen[i] = ODBC_BUFFER_SIZE;

        ret = SQLBindCol(stmt, static_cast<SQLSMALLINT>(i + 1), SQL_C_CHAR, columns[i], columnsLen[i], &columnsLen[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR catalogPattern[] = "";
    SQLCHAR schemaPattern[] = "";
    SQLCHAR tablePattern[] = "";
    SQLCHAR tableTypePattern[] = "";

    ret = SQLTables(stmt, catalogPattern, SQL_NTS, schemaPattern,
        SQL_NTS, tablePattern, SQL_NTS, tableTypePattern, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckMeta<COLUMNS_NUM, ODBC_BUFFER_SIZE>(columns, columnsLen);

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckMeta<COLUMNS_NUM, ODBC_BUFFER_SIZE>(columns, columnsLen);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

template<typename T>
void CheckObjectData(int8_t* data, int32_t len, T const& value)
{
    InteropUnpooledMemory mem(len);
    mem.Length(len);
    memcpy(mem.Data(), data, len);

    BinaryObject obj(BinaryObjectImpl::FromMemory(mem, 0, 0));

    T actual = obj.Deserialize<T>();
    
    BOOST_CHECK_EQUAL(value, actual);
}

BOOST_AUTO_TEST_CASE(TestKeyVal)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    ComplexType obj;

    obj.i32Field = 123;
    obj.strField = "Some string";

    obj.objField.f1 = 54321;
    obj.objField.f2 = "Hello Ignite";

    cache2.Put(1, obj);

    //_key
    int64_t column1 = 0;
    //_val
    int8_t column2[ODBC_BUFFER_SIZE] = { 0 };
    //k
    int64_t column3 = 0;
    //v
    int8_t column4[ODBC_BUFFER_SIZE] = { 0 };
    //i32Field
    int64_t column5 = 0;
    //objField
    int8_t column6[ODBC_BUFFER_SIZE] = { 0 };
    //strField
    char column7[ODBC_BUFFER_SIZE] = { 0 };
    
    SQLLEN column1Len = sizeof(column1);
    SQLLEN column2Len = sizeof(column2);
    SQLLEN column3Len = sizeof(column3);
    SQLLEN column4Len = sizeof(column4);
    SQLLEN column5Len = sizeof(column5);
    SQLLEN column6Len = sizeof(column6);
    SQLLEN column7Len = sizeof(column7);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &column1, column1Len, &column1Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_BINARY, &column2, column2Len, &column2Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &column3, column3Len, &column3Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 4, SQL_C_BINARY, &column4, column4Len, &column4Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 5, SQL_C_SLONG, &column5, column5Len, &column5Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 6, SQL_C_BINARY, &column6, column6Len, &column6Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 7, SQL_C_CHAR, &column7, column7Len, &column7Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT _key, _val, k, v, i32Field, objField, strField FROM ComplexType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, 1);

    CheckObjectData(column2, static_cast<int32_t>(column2Len), obj);

    BOOST_CHECK_EQUAL(column3, 1);

    CheckObjectData(column4, static_cast<int32_t>(column4Len), obj);

    BOOST_CHECK_EQUAL(column5, obj.i32Field);
    
    CheckObjectData(column6, static_cast<int32_t>(column6Len), obj.objField);

    BOOST_CHECK_EQUAL(column7, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);

    SQLCHAR requestStar[] = "SELECT _key, _val, * FROM ComplexType";

    ret = SQLExecDirect(stmt, requestStar, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, 1);

    CheckObjectData(column2, static_cast<int32_t>(column2Len), obj);

    BOOST_CHECK_EQUAL(column3, 1);

    CheckObjectData(column4, static_cast<int32_t>(column4Len), obj);

    BOOST_CHECK_EQUAL(column5, obj.i32Field);

    CheckObjectData(column6, static_cast<int32_t>(column6Len), obj.objField);

    BOOST_CHECK_EQUAL(column7, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestParamsNum)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    CheckParamsNum("SELECT * FROM TestType", 0);
    CheckParamsNum("SELECT * FROM TestType WHERE _key=?", 1);
    CheckParamsNum("SELECT * FROM TestType WHERE _key=? AND _val=?", 2);
    CheckParamsNum("INSERT INTO TestType(_key, strField) VALUES(1, 'some')", 0);
    CheckParamsNum("INSERT INTO TestType(_key, strField) VALUES(?, ?)", 2);
}

BOOST_AUTO_TEST_CASE(TestExecuteAfterCursorClose)
{
    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    int64_t key = 0;
    char strField[1024] = { 0 };
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
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType";

    ret = SQLPrepare(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(key, 1);

    BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), "5");

    ret = SQLFetch(stmt);

    BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestCloseNonFullFetch)
{
    TestType in1;
    TestType in2;

    in1.strField = "str1";
    in2.strField = "str2";

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    int64_t key = 0;
    char strField[1024] = { 0 };
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

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(key, 1);
    BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), "str1");

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestBindNullParameter)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLLEN paramInd = SQL_NULL_DATA;

    // Binding NULL parameter.
    SQLRETURN ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 100, 100, 0, 0, &paramInd);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(1, ?)";

    ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Unbindning parameter.
    ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Selecting inserted column to make sure that everything is OK
    SQLCHAR selectReq[] = "SELECT strField FROM TestType";

    char strField[1024] = { 0 };
    SQLLEN strFieldLen = 0;

    // Binding column.
    ret = SQLBindCol(stmt, 1, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(strFieldLen, SQL_NULL_DATA);
}

BOOST_AUTO_TEST_CASE(TestErrorMessage)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT a FROM B";

    SQLRETURN ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    std::string error = GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt);
    std::string pattern = "42000: Table \"B\" not found; SQL statement:\\vSELECT a FROM B.*";

    boost::cmatch what;
    if (!boost::regex_match(error.c_str(), what, boost::regex(pattern)))
        BOOST_FAIL("'" + error + "' does not match '" + pattern + "'");
}

BOOST_AUTO_TEST_CASE(TestAffectedRows)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024] = { 0 };
    SQLLEN strFieldLen = 0;

    SQLCHAR updateReq[] = "UPDATE TestType SET strField = 'Updated value' WHERE _key > 20 AND _key < 40";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLLEN affected = 0;
    ret = SQLRowCount(stmt, &affected);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(affected, 19);

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    affected = -1;
    ret = SQLRowCount(stmt, &affected);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(affected, 0);
}

BOOST_AUTO_TEST_SUITE_END()
