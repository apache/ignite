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

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct ApiRobustnessTestSuiteFixture 
{
    void Prepare()
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

    /**
     * Establish connection to node.
     *
     * @param connectStr Connection string.
     */
    void Connect(const std::string& connectStr)
    {
        Prepare();

        // Connect string
        std::vector<SQLCHAR> connectStr0;

        connectStr0.reserve(connectStr.size() + 1);
        std::copy(connectStr.begin(), connectStr.end(), std::back_inserter(connectStr0));

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

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

    static Ignite StartNode(const char* name, const char* config)
    {
        IgniteConfiguration cfg;

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");
        cfg.jvmOpts.push_back("-Duser.timezone=GMT");

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");

        BOOST_REQUIRE(cfgPath != 0);

        cfg.springCfgPath.assign(cfgPath).append("/").append(config);

        IgniteError err;

        return Ignition::Start(cfg, name);
    }

    static Ignite StartAdditionalNode(const char* name)
    {
        return StartNode(name, "queries-test-noodbc.xml");
    }

    /**
     * Constructor.
     */
    ApiRobustnessTestSuiteFixture() :
        testCache(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
        grid = StartNode("NodeMain", "queries-test.xml");

        testCache = grid.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Destructor.
     */
    ~ApiRobustnessTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;

    /** Test cache instance. */
    Cache<int64_t, TestType> testCache;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

BOOST_FIXTURE_TEST_SUITE(ApiRobustnessTestSuite, ApiRobustnessTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestSQLDriverConnect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Prepare();

    SQLCHAR connectStr[] = "driver={Apache Ignite};address=127.0.0.1:11110;cache=cache";

    SQLCHAR outStr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outStrLen;

    // Normal connect.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr),
        outStr, sizeof(outStr), &outStrLen, SQL_DRIVER_COMPLETE);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLDisconnect(dbc);

    // Null out string resulting length.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), outStr, sizeof(outStr), 0, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null out string buffer length.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), outStr, 0, &outStrLen, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null out string.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), 0, sizeof(outStr), &outStrLen, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null all.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), 0, 0, 0, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);
}

BOOST_AUTO_TEST_CASE(TestSQLConnect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    // Everyting is ok.
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Resulting length is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, 0);

    // Buffer length is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, 0, &resLen);

    // Buffer is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, 0, ODBC_BUFFER_SIZE, &resLen);

    // Unknown info.
    SQLGetInfo(dbc, -1, buffer, ODBC_BUFFER_SIZE, &resLen);

    // All nulls.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLPrepare)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    // Everyting is ok.
    SQLRETURN ret = SQLPrepare(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    // Value length is null.
    SQLPrepare(stmt, sql, 0);

    SQLCloseCursor(stmt);

    // Value is null.
    SQLPrepare(stmt, 0, sizeof(sql));

    SQLCloseCursor(stmt);

    // All nulls.
    SQLPrepare(stmt, 0, 0);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLExecDirect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    // Everyting is ok.
    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    // Value length is null.
    SQLExecDirect(stmt, sql, 0);

    SQLCloseCursor(stmt);

    // Value is null.
    SQLExecDirect(stmt, 0, sizeof(sql));

    SQLCloseCursor(stmt);

    // All nulls.
    SQLExecDirect(stmt, 0, 0);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLExtendedFetch)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    for (int i = 0; i < 100; ++i)
    {
        TestType obj;

        obj.strField = LexicalCast<std::string>(i);

        testCache.Put(i, obj);
    }

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLULEN rowCount;
    SQLUSMALLINT rowStatus[16];

    // Everyting is ok.
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, rowStatus);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Row count is null.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, 0, rowStatus);

    // Row statuses is null.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, 0);

    // All nulls.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLNumResultCols)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    for (int i = 0; i < 100; ++i)
    {
        TestType obj;

        obj.strField = LexicalCast<std::string>(i);

        testCache.Put(i, obj);
    }

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLSMALLINT columnCount;

    // Everyting is ok.
    ret = SQLNumResultCols(stmt, &columnCount);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Column count is null.
    SQLNumResultCols(stmt, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLTables)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[ODBC_BUFFER_SIZE];
    SQLCHAR schemaName[ODBC_BUFFER_SIZE];
    SQLCHAR tableName[ODBC_BUFFER_SIZE];
    SQLCHAR tableType[ODBC_BUFFER_SIZE];

    // Everithing is ok.
    SQLRETURN ret = SQLTables(stmt, catalogName, sizeof(catalogName), schemaName,
        sizeof(schemaName), tableName, sizeof(tableName), tableType, sizeof(tableType));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Sizes are nulls.
    SQLTables(dbc, catalogName, 0, schemaName, 0, tableName, 0, tableType, 0);

    // Values are nulls.
    SQLTables(dbc, 0, sizeof(catalogName), 0, sizeof(schemaName), 0, sizeof(tableName), 0, sizeof(tableType));

    // All nulls.
    SQLTables(dbc, 0, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLColumns)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[ODBC_BUFFER_SIZE];
    SQLCHAR schemaName[ODBC_BUFFER_SIZE];
    SQLCHAR tableName[ODBC_BUFFER_SIZE];
    SQLCHAR columnName[ODBC_BUFFER_SIZE];

    // Everithing is ok.
    SQLRETURN ret = SQLColumns(stmt, catalogName, sizeof(catalogName), schemaName,
        sizeof(schemaName), tableName, sizeof(tableName), columnName, sizeof(columnName));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Sizes are nulls.
    SQLColumns(dbc, catalogName, 0, schemaName, 0, tableName, 0, columnName, 0);

    // Values are nulls.
    SQLColumns(dbc, 0, sizeof(catalogName), 0, sizeof(schemaName), 0, sizeof(tableName), 0, sizeof(columnName));

    // All nulls.
    SQLColumns(dbc, 0, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLBindParameter)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLINTEGER ind1;
    SQLLEN len1 = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT,
        SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Size is null.
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, 0, &len1);

    // Res size is null.
    SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), 0);

    // Value is null.
    SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, sizeof(ind1), &len1);

    // All nulls.
    SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLNativeSql)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("driver={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";
    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLNativeSql(dbc, sql, sizeof(sql), buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Value size is null.
    SQLNativeSql(dbc, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer size is null.
    SQLNativeSql(dbc, sql, sizeof(sql), buffer, 0, &resLen);

    // Res size is null.
    SQLNativeSql(dbc, sql, sizeof(sql), buffer, sizeof(buffer), 0);

    // Value is null.
    SQLNativeSql(dbc, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer is null.
    SQLNativeSql(dbc, sql, sizeof(sql), 0, sizeof(buffer), &resLen);

    // All nulls.
    SQLNativeSql(dbc, sql, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_SUITE_END()
