/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include <iostream>

#include <boost/test/unit_test.hpp>

#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/config/connection_info.h"

using namespace ignite::odbc;
using namespace ignite::odbc::config;

BOOST_AUTO_TEST_SUITE(ConnectionInfoTestSuite)

BOOST_AUTO_TEST_CASE(TestConnectionInfoSupportedInfo)
{
    char buffer[4096];
    short reslen = 0;

    Configuration cfg;
    ConnectionInfo info(cfg);

    SqlResult::Type result;

#ifdef SQL_DRIVER_NAME
    result = info.GetInfo(SQL_DRIVER_NAME, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DRIVER_NAME

#ifdef SQL_DBMS_NAME
    result = info.GetInfo(SQL_DBMS_NAME, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DBMS_NAME

#ifdef SQL_DRIVER_ODBC_VER
    result = info.GetInfo(SQL_DRIVER_ODBC_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DRIVER_ODBC_VER

#ifdef SQL_DBMS_VER
    result = info.GetInfo(SQL_DBMS_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DBMS_VER

#ifdef SQL_DRIVER_VER
    result = info.GetInfo(SQL_DRIVER_VER, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DRIVER_VER

#ifdef SQL_COLUMN_ALIAS
    result = info.GetInfo(SQL_COLUMN_ALIAS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_COLUMN_ALIAS

#ifdef SQL_IDENTIFIER_QUOTE_CHAR
    result = info.GetInfo(SQL_IDENTIFIER_QUOTE_CHAR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_IDENTIFIER_QUOTE_CHAR

#ifdef SQL_CATALOG_NAME_SEPARATOR
    result = info.GetInfo(SQL_CATALOG_NAME_SEPARATOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CATALOG_NAME_SEPARATOR

#ifdef SQL_SPECIAL_CHARACTERS
    result = info.GetInfo(SQL_SPECIAL_CHARACTERS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_SPECIAL_CHARACTERS

#ifdef SQL_CATALOG_TERM
    result = info.GetInfo(SQL_CATALOG_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CATALOG_TERM

#ifdef SQL_TABLE_TERM
    result = info.GetInfo(SQL_TABLE_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_TABLE_TERM

#ifdef SQL_SCHEMA_TERM
    result = info.GetInfo(SQL_SCHEMA_TERM, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_SCHEMA_TERM

#ifdef SQL_ASYNC_DBC_FUNCTIONS
    result = info.GetInfo(SQL_ASYNC_DBC_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_ASYNC_DBC_FUNCTIONS

#ifdef SQL_GETDATA_EXTENSIONS
    result = info.GetInfo(SQL_GETDATA_EXTENSIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_GETDATA_EXTENSIONS

#ifdef SQL_ODBC_INTERFACE_CONFORMANCE
    result = info.GetInfo(SQL_ODBC_INTERFACE_CONFORMANCE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_ODBC_INTERFACE_CONFORMANCE

#ifdef SQL_SQL_CONFORMANCE
    result = info.GetInfo(SQL_SQL_CONFORMANCE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_SQL_CONFORMANCE

#ifdef SQL_CATALOG_USAGE
    result = info.GetInfo(SQL_CATALOG_USAGE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CATALOG_USAGE

#ifdef SQL_SCHEMA_USAGE
    result = info.GetInfo(SQL_SCHEMA_USAGE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_SCHEMA_USAGE

#ifdef SQL_MAX_IDENTIFIER_LEN
    result = info.GetInfo(SQL_MAX_IDENTIFIER_LEN, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_MAX_IDENTIFIER_LEN

#ifdef SQL_AGGREGATE_FUNCTIONS
    result = info.GetInfo(SQL_AGGREGATE_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_AGGREGATE_FUNCTIONS
    result = info.GetInfo(SQL_NUMERIC_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_AGGREGATE_FUNCTIONS

#ifdef SQL_STRING_FUNCTIONS
    result = info.GetInfo(SQL_STRING_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_STRING_FUNCTIONS

#ifdef SQL_TIMEDATE_FUNCTIONS
    result = info.GetInfo(SQL_TIMEDATE_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_TIMEDATE_FUNCTIONS

#ifdef SQL_TIMEDATE_ADD_INTERVALS
    result = info.GetInfo(SQL_TIMEDATE_ADD_INTERVALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_TIMEDATE_ADD_INTERVALS

#ifdef SQL_TIMEDATE_DIFF_INTERVALS
    result = info.GetInfo(SQL_TIMEDATE_DIFF_INTERVALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_TIMEDATE_DIFF_INTERVALS

#ifdef SQL_DATETIME_LITERALS
    result = info.GetInfo(SQL_DATETIME_LITERALS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_DATETIME_LITERALS

#ifdef SQL_SYSTEM_FUNCTIONS
    result = info.GetInfo(SQL_SYSTEM_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_SYSTEM_FUNCTIONS

#ifdef SQL_CONVERT_FUNCTIONS
    result = info.GetInfo(SQL_CONVERT_FUNCTIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CONVERT_FUNCTIONS

#ifdef SQL_OJ_CAPABILITIES
    result = info.GetInfo(SQL_OJ_CAPABILITIES, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_OJ_CAPABILITIES

#ifdef SQL_POS_OPERATIONS
    result = info.GetInfo(SQL_POS_OPERATIONS, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_POS_OPERATIONS

#ifdef SQL_MAX_CONCURRENT_ACTIVITIES
    result = info.GetInfo(SQL_MAX_CONCURRENT_ACTIVITIES, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_MAX_CONCURRENT_ACTIVITIES

#ifdef SQL_CURSOR_COMMIT_BEHAVIOR
    result = info.GetInfo(SQL_CURSOR_COMMIT_BEHAVIOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CURSOR_COMMIT_BEHAVIOR

#ifdef SQL_CURSOR_ROLLBACK_BEHAVIOR
    result = info.GetInfo(SQL_CURSOR_ROLLBACK_BEHAVIOR, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_CURSOR_ROLLBACK_BEHAVIOR

#ifdef SQL_TXN_CAPABLE
    result = info.GetInfo(SQL_TXN_CAPABLE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_TXN_CAPABLE

#ifdef SQL_QUOTED_IDENTIFIER_CASE
    result = info.GetInfo(SQL_QUOTED_IDENTIFIER_CASE, buffer, sizeof(buffer), &reslen);
    BOOST_REQUIRE(result == SqlResult::AI_SUCCESS);
#endif //SQL_QUOTED_IDENTIFIER_CASE
}

BOOST_AUTO_TEST_SUITE_END()
