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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#ifdef _WIN32
#   define _WINSOCKAPI_
#   include <windows.h>
#endif //_WIN32

#include <sqlext.h>
#include <odbcinst.h>

#include <iostream>

#include <boost/test/unit_test.hpp>

#include <ignite/odbc/connection_info.h>

using namespace ignite::odbc;

BOOST_AUTO_TEST_SUITE(ConnectionInfoTestSuite)

BOOST_AUTO_TEST_CASE(TestConnectionInfoSupportedInfo)
{
    char buffer[4096];
    short reslen = 0;

    ConnectionInfo info;

    bool success;

    success = info.GetInfo(SQL_DRIVER_NAME, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_DBMS_NAME, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_DRIVER_ODBC_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_DBMS_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_DRIVER_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_COLUMN_ALIAS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_IDENTIFIER_QUOTE_CHAR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CATALOG_NAME_SEPARATOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_SPECIAL_CHARACTERS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CATALOG_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_TABLE_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_SCHEMA_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_ASYNC_DBC_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_GETDATA_EXTENSIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_ODBC_INTERFACE_CONFORMANCE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_SQL_CONFORMANCE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CATALOG_USAGE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_SCHEMA_USAGE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_MAX_IDENTIFIER_LEN, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_AGGREGATE_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_NUMERIC_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_STRING_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_TIMEDATE_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_TIMEDATE_ADD_INTERVALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_TIMEDATE_DIFF_INTERVALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_DATETIME_LITERALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_SYSTEM_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CONVERT_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_OJ_CAPABILITIES, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_POS_OPERATIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_MAX_CONCURRENT_ACTIVITIES, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CURSOR_COMMIT_BEHAVIOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_CURSOR_ROLLBACK_BEHAVIOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_TXN_CAPABLE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);

    success = info.GetInfo(SQL_QUOTED_IDENTIFIER_CASE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(success);
}

BOOST_AUTO_TEST_SUITE_END()