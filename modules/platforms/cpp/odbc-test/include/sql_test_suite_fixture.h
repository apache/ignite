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

#ifndef _IGNITE_ODBC_TEST_SQL_TEST_SUIT_FIXTURE
#define _IGNITE_ODBC_TEST_SQL_TEST_SUIT_FIXTURE

#ifdef _WIN32
#   include <windows.h>
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
#include "ignite/common/decimal.h"

#include "test_type.h"

namespace ignite
{
    /**
     * Test setup fixture.
     */
    struct SqlTestSuiteFixture
    {
        /**
         * Constructor.
         */
        SqlTestSuiteFixture();

        /**
         * Destructor.
         */
        ~SqlTestSuiteFixture();

        /**
         * Run query returning single result and stores it to buffer.
         *
         * @param request SQL request.
         * @param type Result type.
         * @param column Result buffer.
         * @param bufSize Result buffer size.
         * @param resSize Size of received value.
         */
        void CheckSingleResult0(const char* request, SQLSMALLINT type,
            void* column, SQLLEN bufSize, SQLLEN* resSize) const;

        /**
         * Run query returning single result and check it to be equal to expected.
         *
         * @param request SQL request.
         * @param expected Expected result.
         */
        template<typename T>
        void CheckSingleResult(const char* request, const T& expected)
        {
            BOOST_FAIL("Function is not defined for the type.");
        }

        /**
         * Run query returning single result and check it to be successful.
         *
         * @param request SQL request.
         */
        template<typename T>
        void CheckSingleResult(const char* request)
        {
            BOOST_FAIL("Function is not defined for the type.");
        }

        /**
         * Run query returning single result and check it to be equal to expected.
         *
         * @param request SQL request.
         * @param expected Expected result.
         * @param type Result type.
         */
        template<typename T>
        void CheckSingleResultNum0(const char* request, const T& expected, SQLSMALLINT type)
        {
            T res = 0;

            CheckSingleResult0(request, type, &res, 0, 0);

            BOOST_CHECK_EQUAL(res, expected);
        }

        /**
         * Run query returning single result and check it to be equal to expected.
         *
         * @param request SQL request.
         * @param expected Expected result.
         * @param type Result type.
         */
        template<typename T>
        void CheckSingleResultNum0(const char* request, SQLSMALLINT type)
        {
            T res = 0;

            CheckSingleResult0(request, type, &res, 0, 0);
        }


        /** Node started during the test. */
        Ignite grid;

        /** Test cache instance. */
        cache::Cache<int64_t, TestType> testCache;

        /** ODBC Environment. */
        SQLHENV env;

        /** ODBC Connect. */
        SQLHDBC dbc;

        /** ODBC Statement. */
        SQLHSTMT stmt;
    };

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<std::string>(const char* request, const std::string& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int64_t>(const char* request, const int64_t& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int32_t>(const char* request, const int32_t& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int16_t>(const char* request, const int16_t& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int8_t>(const char* request, const int8_t& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<float>(const char* request, const float& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<double>(const char* request, const double& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<bool>(const char* request, const bool& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<ignite::Guid>(const char* request, const ignite::Guid& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<std::string>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int64_t>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int32_t>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int16_t>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<int8_t>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<float>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<double>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<Date>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<Timestamp>(const char* request);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<std::vector<int8_t> >(const char* request, const std::vector<int8_t>& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<ignite::common::Decimal>(const char* request, const ignite::common::Decimal& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<Date>(const char* request, const Date& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<SQL_TIME_STRUCT>(const char* request, const SQL_TIME_STRUCT& expected);

    template<>
    void SqlTestSuiteFixture::CheckSingleResult<Timestamp>(const char* request, const Timestamp& expected);
}

#endif //_IGNITE_ODBC_TEST_SQL_TEST_SUIT_FIXTURE
