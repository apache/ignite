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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"
#include "ignite/odbc/socket_client.h"
#include <boost/thread/v2/thread.hpp>

using namespace ignite;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct StreamingTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    StreamingTestSuiteFixture() :
        grid(StartPlatformNode("queries-test.xml", "NodeMain")),
        cache(grid.GetCache<int32_t, TestType>("cache"))
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~StreamingTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    void InsertTestStrings(int32_t begin, int32_t end)
    {
        SQLCHAR req[] = "INSERT INTO TestType(_key, strField) VALUES(?, ?)";

        SQLRETURN ret = SQLPrepare(stmt, req, SQL_NTS);

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
        for (SQLSMALLINT i = begin; i < end; ++i)
        {
            key = i;
            std::string val = getTestString(i);

            strncpy(strField, val.c_str(), sizeof(strField));
            strFieldLen = SQL_NTS;

            ret = SQLExecute(stmt);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        // Resetting parameters.
        ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    void CheckValues(int32_t begin, int32_t end)
    {
        SQLCHAR req[] = "SELECT _key, strField FROM TestType WHERE _key >= ? AND _key < ? ORDER BY _key";

        SQLRETURN ret = SQLPrepare(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int64_t p1 = begin;
        int64_t p2 = end;

        // Binding parameters.
        ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &p1, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &p2, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int64_t keyVal = 0;
        char strField[1024] = {0};
        SQLLEN strFieldLen = 0;

        ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &keyVal, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        // Fetching values.
        for (SQLSMALLINT i = begin; i < end; ++i)
        {
            ret = SQLFetch(stmt);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            BOOST_CHECK_EQUAL(i, keyVal);
            BOOST_CHECK_EQUAL(getTestString(i), std::string(strField, static_cast<size_t>(strFieldLen)));
        }

        // Resetting parameters.
        ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        // Resetting columns.
        ret = SQLFreeStmt(stmt, SQL_UNBIND);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    /** Node started during the test. */
    Ignite grid;

    /** Cache. */
    cache::Cache<int32_t, TestType> cache;
};

BOOST_FIXTURE_TEST_SUITE(StreamingTestSuite, StreamingTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestStreamingSimple)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    SQLRETURN res = ExecQuery("set streaming on batch_size 100 flush_frequency 100");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    InsertTestStrings(0, 10);

    BOOST_CHECK_EQUAL(cache.Size(), 0);

    InsertTestStrings(10, 110);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(250));

    BOOST_CHECK_EQUAL(cache.Size(), 100);

    res = ExecQuery("set streaming off");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(cache.Size(), 110);

    CheckValues(0, 110);
}

BOOST_AUTO_TEST_CASE(TestStreamingAllOptions)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    SQLRETURN res = ExecQuery(
        "set streaming 1 "
        "allow_overwrite on "
        "batch_size 512 "
        "per_node_buffer_size 500 "
        "per_node_parallel_operations 4 "
        "flush_frequency 100 "
        "ordered");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    InsertTestStrings(0, 10);

    BOOST_CHECK_EQUAL(cache.Size(), 0);

    InsertTestStrings(0, 512);

    res = ExecQuery("set streaming off");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(cache.Size(), 512);
}

BOOST_AUTO_TEST_CASE(TestStreamingNotAllowedOverwrite)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    SQLRETURN res = ExecQuery("set streaming 1 allow_overwrite off batch_size 10");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    InsertTestStrings(0, 10);

    BOOST_CHECK_EQUAL(cache.Size(), 0);

    InsertTestStrings(0, 10);

    res = ExecQuery("set streaming off");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(cache.Size(), 10);
}

BOOST_AUTO_TEST_CASE(TestStreamingReset)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    SQLRETURN res = ExecQuery("set streaming 1 batch_size 100 flush_frequency 1000");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    InsertTestStrings(0, 10);

    BOOST_CHECK_EQUAL(cache.Size(), 0);

    InsertTestStrings(10, 20);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(250));

    BOOST_CHECK_EQUAL(cache.Size(), 0);

    res = ExecQuery("set streaming 1 batch_size 10 flush_frequency 100");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(cache.Size(), 20);

    InsertTestStrings(20, 50);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(250));

    BOOST_CHECK_EQUAL(cache.Size(), 50);

    res = ExecQuery("set streaming 0");

    if (res != SQL_SUCCESS)
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_SUITE_END()
