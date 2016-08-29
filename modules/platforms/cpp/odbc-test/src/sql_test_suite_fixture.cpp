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

#include "sql_test_suite_fixture.h"

#include "test_utils.h"

namespace ignite
{
    SqlTestSuiteFixture::SqlTestSuiteFixture():
        testCache(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
        IgniteConfiguration cfg;

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");

        BOOST_REQUIRE(cfgPath != 0) ;

        cfg.springCfgPath.assign(cfgPath).append("/queries-test.xml");

        IgniteError err;

        grid = Ignition::Start(cfg, &err);

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
        BOOST_FAIL(err.GetText()) ;

        testCache = grid.GetCache<int64_t, TestType>("cache");

        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        BOOST_REQUIRE(env != NULL) ;

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        BOOST_REQUIRE(dbc != NULL) ;

        // Connect string
        SQLCHAR connectStr[] = "DRIVER={Apache Ignite};SERVER=localhost;PORT=10800;CACHE=cache";

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, static_cast<SQLSMALLINT>(sizeof(connectStr)),
                                         outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
        {
            Ignition::Stop(grid.GetName(), true);

            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc)) ;
        }

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        BOOST_REQUIRE(stmt != NULL) ;
    }

    SqlTestSuiteFixture::~SqlTestSuiteFixture()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        ignite::Ignition::Stop(grid.GetName(), true);
    }

    void SqlTestSuiteFixture::CheckSingleResult0(const char* request,
        SQLSMALLINT type, void* column, SQLLEN bufSize, SQLLEN* resSize) const
    {
        SQLRETURN ret;

        ret = SQLBindCol(stmt, 1, type, column, bufSize, resSize);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt)) ;

        ret = SQLExecDirect(stmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(request)), SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt)) ;

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt)) ;

        ret = SQLFetch(stmt);
        BOOST_CHECK(ret == SQL_NO_DATA) ;
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<std::string>(const char* request, const std::string& expected)
    {
        SQLCHAR res[ODBC_BUFFER_SIZE] = { 0 };
        SQLLEN resLen = 0;

        CheckSingleResult0(request, SQL_C_CHAR, res, ODBC_BUFFER_SIZE, &resLen);

        std::string actual;

        if (resLen > 0)
            actual.assign(reinterpret_cast<char*>(res), static_cast<size_t>(resLen));

        BOOST_CHECK_EQUAL(actual, expected);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int64_t>(const char* request, const int64_t& expected)
    {
        CheckSingleResultNum0<int64_t>(request, expected, SQL_C_SBIGINT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int32_t>(const char* request, const int32_t& expected)
    {
        CheckSingleResultNum0<int32_t>(request, expected, SQL_C_SLONG);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int16_t>(const char* request, const int16_t& expected)
    {
        CheckSingleResultNum0<int16_t>(request, expected, SQL_C_SSHORT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int8_t>(const char* request, const int8_t& expected)
    {
        CheckSingleResultNum0<int8_t>(request, expected, SQL_C_STINYINT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<float>(const char* request, const float& expected)
    {
        SQLFLOAT res = 0;

        CheckSingleResult0(request, SQL_C_FLOAT, &res, 0, 0);

        BOOST_CHECK_CLOSE(static_cast<float>(res), expected, 1E-6f);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<double>(const char* request, const double& expected)
    {
        SQLDOUBLE res = 0;

        CheckSingleResult0(request, SQL_C_DOUBLE, &res, 0, 0);

        BOOST_CHECK_CLOSE(static_cast<double>(res), expected, 1E-6);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<bool>(const char* request, const bool& expected)
    {
        SQLCHAR res = 0;

        CheckSingleResult0(request, SQL_C_BIT, &res, 0, 0);

        BOOST_CHECK_EQUAL((res != 0), expected);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<ignite::Guid>(const char* request, const ignite::Guid& expected)
    {
        SQLGUID res;

        memset(&res, 0, sizeof(res));

        CheckSingleResult0(request, SQL_C_GUID, &res, 0, 0);

        BOOST_CHECK_EQUAL(res.Data1, expected.GetMostSignificantBits() & 0xFFFFFFFF00000000ULL >> 32);
        BOOST_CHECK_EQUAL(res.Data2, expected.GetMostSignificantBits() & 0x00000000FFFF0000ULL >> 16);
        BOOST_CHECK_EQUAL(res.Data3, expected.GetMostSignificantBits() & 0x000000000000FFFFULL);

        for (int i = 0; i < sizeof(res.Data4); ++i)
            BOOST_CHECK_EQUAL(res.Data4[i], (expected.GetLeastSignificantBits() & (0xFFULL << (8 * i))) >> (8 * i));
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<std::string>(const char* request)
    {
        SQLCHAR res[ODBC_BUFFER_SIZE] = { 0 };
        SQLLEN resLen = 0;

        CheckSingleResult0(request, SQL_C_CHAR, res, ODBC_BUFFER_SIZE, &resLen);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int64_t>(const char* request)
    {
        CheckSingleResultNum0<int64_t>(request, SQL_C_SBIGINT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int32_t>(const char* request)
    {
        CheckSingleResultNum0<int32_t>(request, SQL_C_SLONG);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int16_t>(const char* request)
    {
        CheckSingleResultNum0<int16_t>(request, SQL_C_SSHORT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int8_t>(const char* request)
    {
        CheckSingleResultNum0<int8_t>(request, SQL_C_STINYINT);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<float>(const char* request)
    {
        SQLFLOAT res = 0;

        CheckSingleResult0(request, SQL_C_FLOAT, &res, 0, 0);
    }

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<double>(const char* request)
    {
        SQLDOUBLE res = 0;

        CheckSingleResult0(request, SQL_C_DOUBLE, &res, 0, 0);
    }
}
