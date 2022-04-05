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

#include <vector>
#include <string>
#include <boost/test/unit_test.hpp>

#include "ignite/odbc/engine_mode.h"
#include "ignite/odbc/utility.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;
using namespace ignite::binary;
using namespace ignite::impl::binary;
using namespace ignite::impl::interop;
using namespace ignite::odbc;
using namespace ignite::utility;

using namespace boost::unit_test;

struct CrossEngineTestSuiteFixture : public odbc::OdbcTestSuite
{
    CrossEngineTestSuiteFixture() : odbc::OdbcTestSuite()
    {
        grid = StartPlatformNode("queries-test.xml", "NodeMain");
    }

    template<EngineMode::Type EType>
    void TestEngine() {
        Connect(ConnectionString<EType>());

        std::string currQryEngine = GetQueryEngine<EType>();

        BOOST_CHECK_EQUAL(currQryEngine, EType != EngineMode::DEFAULT ? EngineMode::ToString(EType)
                                                                      : EngineMode::ToString(EngineMode::H2));

        InsertTestStrings(10, false);
        InsertTestBatch(11, 2000, 1989);
    }

    template<EngineMode::Type EType>
    std::string ConnectionString() const {
        std::ostringstream oss;

        oss << "DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;";

        oss << "QUERY_ENGINE=" << EngineMode::ToString(EType);

        return oss.str();
    }

    template<EngineMode::Type EType>
    std::string GetQueryEngine() {
        SQLRETURN ret = ExecQuery(EType == EngineMode::CALCITE ? "select query_engine()"
                                                               : "select public.query_engine()");

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        SQLCHAR strBuf[1024];
        SQLLEN strLen;

        SQLBindCol(stmt, 1, SQL_CHAR, strBuf, sizeof(strBuf), &strLen);

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);
        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

        return SqlStringToString(strBuf, static_cast<int32_t>(strLen));
    }

    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(CrossEngineTestSuite, CrossEngineTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestDefaultEngine)
{
    TestEngine<EngineMode::DEFAULT>();
}

BOOST_AUTO_TEST_CASE(TestCalciteEngine)
{
    TestEngine<EngineMode::CALCITE>();
}

BOOST_AUTO_TEST_CASE(TestH2Engine)
{
    TestEngine<EngineMode::H2>();
}

BOOST_AUTO_TEST_SUITE_END()
