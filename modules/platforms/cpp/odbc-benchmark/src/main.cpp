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
#include <iterator>
#include <sstream>

#include <boost/random/random_device.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/uniform_real_distribution.hpp>

#include "benchmark_utils.h"
#include "test_type.h"

#define IGNITE_CHECK(stmt) \
    if (!stmt) \
        throw ignite::IgniteError(ignite::IgniteError::IGNITE_ERR_GENERIC, "Check failed: " #stmt);

/**
 * Extract error message.
 *
 * @param handleType Type of the handle.
 * @param handle Handle.
 * @return Error message.
 */
std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER nativeCode;

    SQLCHAR message[1024];
    SQLSMALLINT reallen = 0;

    SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, sizeof(message), &reallen);

    return std::string(reinterpret_cast<char*>(sqlstate)) + ": " +
        std::string(reinterpret_cast<char*>(message), reallen);
}

void ThrowOdbcError(SQLSMALLINT handleType, SQLHANDLE handle)
{
    throw ignite::IgniteError(ignite::IgniteError::IGNITE_ERR_GENERIC,
        GetOdbcErrorMessage(handleType, handle).c_str());
}

template<int64_t recordsNum>
struct OdbcFetchBenchmark
{
    /**
     * Set up Ignite environment.
     */
    void IgniteSetup()
    {
        using namespace ignite;

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

        cfg.springCfgPath.assign(getenv("IGNITE_NATIVE_BENCHMARK_ODBC_CONFIG_PATH")).append("/default.xml");

        node = Ignition::Start(cfg);

        testCache = node.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Fill Ignite caches with data.
     */
    void IgniteFill()
    {
        boost::random::random_device rng;

        boost::random::uniform_int_distribution<int8_t> i8Dist;
        boost::random::uniform_int_distribution<int16_t> i16Dist;
        boost::random::uniform_int_distribution<int32_t> i32Dist;
        boost::random::uniform_int_distribution<int64_t> i64Dist;

        boost::random::uniform_real_distribution<float> fDist;
        boost::random::uniform_real_distribution<double> dDist;

        for (int64_t i = 0; i < recordsNum; ++i)
        {
            ignite::TestType val;

            std::stringstream tmp;

            tmp << i64Dist(rng) << i64Dist(rng) << i64Dist(rng);

            std::swap(val.strField, tmp.str());

            val.i8Field = i8Dist(rng);
            val.i16Field = i16Dist(rng);
            val.i32Field = i32Dist(rng);
            val.i64Field = i64Dist(rng);
            val.floatField = fDist(rng);
            val.doubleField = dDist(rng);
            val.boolField = (i8Dist(rng) & 1) != 0;
            val.guidField = ignite::Guid(i64Dist(rng), i64Dist(rng));
            val.dateField = ignite::Date(i64Dist(rng));
            val.timestampField = ignite::Timestamp(i32Dist(rng), i32Dist(rng,
                boost::random::uniform_int_distribution<int32_t>::param_type(0, 999999999)));

            testCache.Put(i, val);
        }
    }

    /**
     * Clean up Ignite environment.
     */
    void IgniteCleanUp()
    {
        ignite::Ignition::Stop(node.GetName(), true);
    }

    /**
     * Establish ODBC connection to node.
     *
     * @param connectStr Connection string.
     */
    void OdbcConnect(const std::string& connectStr)
    {
        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        IGNITE_CHECK(env != NULL);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        IGNITE_CHECK(dbc != NULL);

        // Connect string
        std::vector<SQLCHAR> connectStr0;

        connectStr0.reserve(connectStr.size() + 1);
        std::copy(connectStr.begin(), connectStr.end(), std::back_inserter(connectStr0));

        SQLCHAR outstr[1024];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
        {
            ignite::Ignition::Stop(node.GetName(), true);

            ThrowOdbcError(SQL_HANDLE_DBC, dbc);
        }

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        IGNITE_CHECK(stmt != NULL);
    }

    void OdbcCleanup()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    OdbcFetchBenchmark() :
        node(),
        testCache(0)
    {
        IgniteSetup();

        IgniteFill();
    }

    ~OdbcFetchBenchmark()
    {
        IgniteCleanUp();
    }

    void Prepare()
    {
        OdbcConnect("driver={Apache Ignite};server=127.0.0.1;port=11111;cache=cache");

        SQLRETURN ret;

        ret = SQLBindCol(stmt, 1, SQL_C_STINYINT, &i8Field, 0, 0);
        ret = SQLBindCol(stmt, 2, SQL_C_SSHORT, &i16Field, 0, 0);
        ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &i32Field, 0, 0);
        ret = SQLBindCol(stmt, 4, SQL_C_SBIGINT, &i64Field, 0, 0);
        ret = SQLBindCol(stmt, 5, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);
        ret = SQLBindCol(stmt, 6, SQL_C_FLOAT, &floatField, 0, 0);
        ret = SQLBindCol(stmt, 7, SQL_C_DOUBLE, &doubleField, 0, 0);
        ret = SQLBindCol(stmt, 8, SQL_C_DATE, &dateField, 0, 0);
        ret = SQLBindCol(stmt, 9, SQL_C_TIMESTAMP, &timestampField, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt);

        SQLCHAR req[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, "
            "floatField, doubleField, boolField, dateField, timestampField FROM TestType";

        ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            ThrowOdbcError(SQL_HANDLE_STMT, stmt);
    }

    void Cleanup()
    {
        OdbcCleanup();
    }

    void Run()
    {
        SQLRETURN ret;

        for (int64_t i = 0; i < recordsNum; ++i)
        {
            ret = SQLFetch(stmt);

            if (!SQL_SUCCEEDED(ret))
                ThrowOdbcError(SQL_HANDLE_STMT, stmt);
        }
    }

private:
    /** Column. */
    SQLINTEGER i8Field;

    /** Column. */
    SQLINTEGER i16Field;

    /** Column. */
    SQLINTEGER i32Field;

    /** Column. */
    SQLBIGINT i64Field;

    /** Column. */
    SQLCHAR strField[1024];

    /** Column length. */
    SQLLEN strFieldLen;

    /** Column. */
    SQLFLOAT floatField;

    /** Column. */
    SQLDOUBLE doubleField;

    /** Column. */
    SQLINTEGER boolField;

    /** Column. */
    SQL_DATE_STRUCT dateField;

    /** Column. */
    SQL_TIMESTAMP_STRUCT timestampField;

    /** Test node. */
    ignite::Ignite node;

    /** Test cache instance. */
    ignite::cache::Cache<int64_t, ignite::TestType> testCache;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

int main()
{
    IGNITE_RUN_BENCHMARK(OdbcFetchBenchmark<10000>, 10, 10);

    return 0;
}