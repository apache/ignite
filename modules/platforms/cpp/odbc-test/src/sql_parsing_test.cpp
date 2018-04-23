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
#   include <Windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <string>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "ignite/odbc/sql/sql_lexer.h"

#include "odbc_test_suite.h"
#include "test_utils.h"


using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct SqlParsingTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    SqlParsingTestSuiteFixture()
    {
        grid = StartPlatformNode("queries-test.xml", "NodeMain");
    }

    /**
     * Destructor.
     */
    ~SqlParsingTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(SqlParsingTestSuite, SqlParsingTestSuiteFixture)

BOOST_AUTO_TEST_CASE(First)
{
    //
}

BOOST_AUTO_TEST_SUITE_END()
