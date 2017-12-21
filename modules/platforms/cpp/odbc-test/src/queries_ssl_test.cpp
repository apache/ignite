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
struct SslQueriesTestSuiteFixture
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
        return StartNode("queries-ssl-32.xml", name);
#else
        return StartNode("queries-ssl.xml", name);
#endif
    }

    /**
     * Constructor.
     */
    SslQueriesTestSuiteFixture() :
        cache1(0),
        cache2(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
#ifdef IGNITE_TESTS_32
        grid = StartNode("queries-ssl-32.xml", "NodeMain");
#else
        grid = StartNode("queries-ssl.xml", "NodeMain");
#endif

        cache1 = grid.GetCache<int64_t, TestType>("cache");
        cache2 = grid.GetCache<int64_t, ComplexType>("cache2");
    }

    /**
     * Destructor.
     */
    ~SslQueriesTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
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

        SQLLEN totallyAffected = 0;

        do
        {
            SQLLEN affected = 0;
            ret = SQLRowCount(stmt, &affected);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            totallyAffected += affected;

            BOOST_CHECKPOINT("Getting next result set");

            ret = SQLMoreResults(stmt);

            if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        } while (ret != SQL_NO_DATA);

        BOOST_CHECK_EQUAL(totallyAffected, expectedToAffect);

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

BOOST_FIXTURE_TEST_SUITE(SslQueriesTestSuite, SslQueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectionSsl)
{
    std::string cfgDirPath = GetTestConfigDir();

    std::stringstream connectString;

    connectString <<
        "DRIVER={Apache Ignite};"
        "ADDRESS=127.0.0.1:11110;"
        "SCHEMA=cache;"
        "SSL_MODE=require;"
        "SSL_KEY_FILE=" << cfgDirPath << "/ssl/client_full.pem;"
        "SSL_CERT_FILE=" << cfgDirPath << "/ssl/client_full.pem;"
        "SSL_CA_FILE=" << cfgDirPath << "/ssl/ca.pem;";

    Connect(connectString.str());

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_SUITE_END()
