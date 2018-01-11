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
#include "odbc_test_suite.h"

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
struct SslQueriesTestSuiteFixture : odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartTestNode("queries-ssl.xml", name);
    }

    /**
     * Constructor.
     */
    SslQueriesTestSuiteFixture() :
        OdbcTestSuite(),
        cache1(0),
        cache2(0)
    {
        grid = StartAdditionalNode("NodeMain");

        cache1 = grid.GetCache<int64_t, TestType>("cache");
        cache2 = grid.GetCache<int64_t, ComplexType>("cache2");
    }

    /**
     * Destructor.
     */
    virtual ~SslQueriesTestSuiteFixture()
    {
        // No-op.
    }

    /** Node started during the test. */
    Ignite grid;

    /** Frist cache instance. */
    Cache<int64_t, TestType> cache1;

    /** Second cache instance. */
    Cache<int64_t, ComplexType> cache2;
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
        "SSL_KEY_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_full.pem;"
        "SSL_CERT_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_full.pem;"
        "SSL_CA_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "ca.pem;";

    Connect(connectString.str());

    InsertTestStrings(10, false);
    InsertTestBatch(11, 2000, 1989);
}

BOOST_AUTO_TEST_SUITE_END()
