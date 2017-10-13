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

#include <iostream>
#include <set>

#include <boost/test/unit_test.hpp>

#include <ignite/odbc/config/configuration.h>
#include <ignite/ignite_error.h>
#include <ignite/common/utils.h>

using namespace ignite::odbc::config;

namespace
{
    const std::string testDriverName = "Ignite Driver";
    const std::string testServerHost = "testhost.com";
    const uint16_t testServerPort = 4242;
    const std::string testSchemaName = "TestSchema";
    const std::string testDsn = "Ignite DSN";
    const int32_t testPageSize = 4321;
    const bool testDistributedJoins = true;
    const bool testEnforceJoinOrder = true;
    const bool testReplicatedOnly = true;
    const bool testCollocated = true;
    const bool testLazy = true;
    const bool testSkipReducerOnUpdate = true;

    const std::string testAddress = testServerHost + ':' + ignite::common::LexicalCast<std::string>(testServerPort);
}

const char* BoolToStr(bool val, bool lowerCase = true)
{
    if (lowerCase)
        return val ? "true" : "false";

    return val ? "TRUE" : "FALSE";
}

void CheckValidAddress(const char* connectStr, uint16_t port)
{
    Configuration cfg;

    BOOST_CHECK_NO_THROW(cfg.FillFromConnectString(connectStr));

    BOOST_CHECK_EQUAL(cfg.GetTcpPort(), port);
}

void CheckValidProtocolVersion(const char* connectStr, ignite::odbc::ProtocolVersion version)
{
    Configuration cfg;

    BOOST_CHECK_NO_THROW(cfg.FillFromConnectString(connectStr));

    BOOST_CHECK(cfg.GetProtocolVersion() == version);
}

void CheckSupportedProtocolVersion(const char* connectStr)
{
    Configuration cfg;

    BOOST_CHECK_NO_THROW(cfg.FillFromConnectString(connectStr));

    BOOST_CHECK(cfg.GetProtocolVersion().IsSupported());
}

void CheckInvalidProtocolVersion(const char* connectStr)
{
    Configuration cfg;

    cfg.FillFromConnectString(connectStr);

    BOOST_CHECK_THROW(cfg.GetProtocolVersion(), ignite::IgniteError);
}

void CheckUnsupportedProtocolVersion(const char* connectStr)
{
    Configuration cfg;

    cfg.FillFromConnectString(connectStr);

    BOOST_CHECK(!cfg.GetProtocolVersion().IsSupported());
}

void CheckValidBoolValue(const std::string& connectStr, const std::string& key, bool val)
{
    Configuration cfg;

    BOOST_CHECK_NO_THROW(cfg.FillFromConnectString(connectStr));

    BOOST_CHECK_EQUAL(cfg.GetBoolValue(key, val), val);
}

void CheckInvalidBoolValue(const std::string& connectStr, const std::string& key)
{
    Configuration cfg;

    cfg.FillFromConnectString(connectStr);

    BOOST_CHECK_THROW(cfg.GetBoolValue(key, false), ignite::IgniteError);
}

void CheckConnectionConfig(const Configuration& cfg)
{
    BOOST_CHECK_EQUAL(cfg.GetDriver(), testDriverName);
    BOOST_CHECK_EQUAL(cfg.GetHost(), testServerHost);
    BOOST_CHECK_EQUAL(cfg.GetTcpPort(), testServerPort);
    BOOST_CHECK_EQUAL(cfg.GetAddress(), testAddress);
    BOOST_CHECK_EQUAL(cfg.GetSchema(), testSchemaName);
    BOOST_CHECK_EQUAL(cfg.GetDsn(), std::string());
    BOOST_CHECK_EQUAL(cfg.GetPageSize(), testPageSize);
    BOOST_CHECK_EQUAL(cfg.IsDistributedJoins(), testDistributedJoins);
    BOOST_CHECK_EQUAL(cfg.IsEnforceJoinOrder(), testEnforceJoinOrder);
    BOOST_CHECK_EQUAL(cfg.IsReplicatedOnly(), testReplicatedOnly);
    BOOST_CHECK_EQUAL(cfg.IsCollocated(), testCollocated);
    BOOST_CHECK_EQUAL(cfg.IsLazy(), testLazy);
    BOOST_CHECK_EQUAL(cfg.IsSkipReducerOnUpdate(), testSkipReducerOnUpdate);

    std::stringstream constructor;

    constructor << "address=" << testAddress << ';'
                << "collocated=" << BoolToStr(testCollocated) << ';'
                << "distributed_joins=" << BoolToStr(testDistributedJoins) << ';'
                << "driver={" << testDriverName << "};"
                << "enforce_join_order=" << BoolToStr(testEnforceJoinOrder) << ';'
                << "lazy=" << BoolToStr(testLazy) << ';'
                << "page_size=" << testPageSize << ';'
                << "replicated_only=" << BoolToStr(testReplicatedOnly) << ';'
                << "schema=" << testSchemaName << ';'
                << "skip_reducer_on_update=" << BoolToStr(testReplicatedOnly) << ';';

    const std::string& expectedStr = constructor.str();

    BOOST_CHECK_EQUAL(ignite::common::ToLower(cfg.ToConnectString()), ignite::common::ToLower(expectedStr));
}

void CheckDsnConfig(const Configuration& cfg)
{
    BOOST_CHECK_EQUAL(cfg.GetDriver(), testDriverName);
    BOOST_CHECK_EQUAL(cfg.GetDsn(), testDsn);
    BOOST_CHECK_EQUAL(cfg.GetSchema(), Configuration::DefaultValue::schema);
    BOOST_CHECK_EQUAL(cfg.GetAddress(), Configuration::DefaultValue::address);
    BOOST_CHECK_EQUAL(cfg.GetHost(), std::string());
    BOOST_CHECK_EQUAL(cfg.GetTcpPort(), Configuration::DefaultValue::port);
    BOOST_CHECK_EQUAL(cfg.GetPageSize(), Configuration::DefaultValue::pageSize);
    BOOST_CHECK_EQUAL(cfg.IsDistributedJoins(), false);
    BOOST_CHECK_EQUAL(cfg.IsEnforceJoinOrder(), false);
    BOOST_CHECK_EQUAL(cfg.IsReplicatedOnly(), false);
    BOOST_CHECK_EQUAL(cfg.IsCollocated(), false);
    BOOST_CHECK_EQUAL(cfg.IsLazy(), false);
    BOOST_CHECK_EQUAL(cfg.IsSkipReducerOnUpdate(), false);
}

BOOST_AUTO_TEST_SUITE(ConfigurationTestSuite)

BOOST_AUTO_TEST_CASE(CheckTestValuesNotEquealDefault)
{
    BOOST_CHECK_NE(testDriverName, Configuration::DefaultValue::driver);
    BOOST_CHECK_NE(testAddress, Configuration::DefaultValue::address);
    BOOST_CHECK_NE(testServerPort, Configuration::DefaultValue::port);
    BOOST_CHECK_NE(testSchemaName, Configuration::DefaultValue::schema);
    BOOST_CHECK_NE(testDsn, Configuration::DefaultValue::dsn);
    BOOST_CHECK_NE(testPageSize, Configuration::DefaultValue::pageSize);
    BOOST_CHECK_NE(testDistributedJoins, Configuration::DefaultValue::distributedJoins);
    BOOST_CHECK_NE(testEnforceJoinOrder, Configuration::DefaultValue::enforceJoinOrder);
    BOOST_CHECK_NE(testReplicatedOnly, Configuration::DefaultValue::replicatedOnly);
    BOOST_CHECK_NE(testCollocated, Configuration::DefaultValue::collocated);
    BOOST_CHECK_NE(testLazy, Configuration::DefaultValue::lazy);
    BOOST_CHECK_NE(testSkipReducerOnUpdate, Configuration::DefaultValue::skipReducerOnUpdate);
}

BOOST_AUTO_TEST_CASE(TestConnectStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER={" << testDriverName << "};"
                << "LAZY=" << BoolToStr(testLazy, false) << ';'
                << "ADDRESS=" << testAddress << ';'
                << "DISTRIBUTED_JOINS=" << BoolToStr(testDistributedJoins, false) << ';'
                << "ENFORCE_JOIN_ORDER=" << BoolToStr(testEnforceJoinOrder, false) << ';'
                << "COLLOCATED=" << BoolToStr(testCollocated, false) << ';'
                << "REPLICATED_ONLY=" << BoolToStr(testReplicatedOnly, false) << ';'
                << "PAGE_SIZE=" << testPageSize << ';'
                << "SCHEMA=" << testSchemaName << ';'
                << "SKIP_REDUCER_ON_UPDATE=" << BoolToStr(testSkipReducerOnUpdate, false);

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "lazy=" << BoolToStr(testLazy) << ';'
                << "address=" << testAddress << ';'
                << "page_size=" << testPageSize << ';'
                << "distributed_joins=" << BoolToStr(testDistributedJoins) << ';'
                << "enforce_join_order=" << BoolToStr(testEnforceJoinOrder) << ';'
                << "replicated_only=" << BoolToStr(testReplicatedOnly) << ';'
                << "collocated=" << BoolToStr(testCollocated) << ';'
                << "schema=" << testSchemaName << ';'
                << "skip_reducer_on_update=" << BoolToStr(testSkipReducerOnUpdate);

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringZeroTerminated)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "address=" << testAddress << ';'
                << "lazy=" << BoolToStr(testLazy) << ';'
                << "page_size=" << testPageSize << ';'
                << "replicated_only=" << BoolToStr(testReplicatedOnly) << ';'
                << "collocated=" << BoolToStr(testCollocated) << ';'
                << "distributed_joins=" << BoolToStr(testDistributedJoins) << ';'
                << "enforce_join_order=" << BoolToStr(testEnforceJoinOrder) << ';'
                << "schema=" << testSchemaName << ';'
                << "skip_reducer_on_update=" << BoolToStr(testSkipReducerOnUpdate);

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size() + 1);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver={" << testDriverName << "};"
                << "Lazy=" << BoolToStr(testLazy) << ';'
                << "Address=" << testAddress << ';'
                << "Page_Size=" << testPageSize << ';'
                << "Distributed_Joins=" << BoolToStr(testDistributedJoins, false) << ';'
                << "Enforce_Join_Order=" << BoolToStr(testEnforceJoinOrder) << ';'
                << "Replicated_Only=" << BoolToStr(testReplicatedOnly, false) << ';'
                << "Collocated=" << BoolToStr(testCollocated) << ';'
                << "Schema=" << testSchemaName << ';'
                << "Skip_Reducer_On_Update=" << BoolToStr(testSkipReducerOnUpdate);

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringWhitepaces)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER = {" << testDriverName << "} ;\n"
                << " ADDRESS =" << testAddress << "; "
                << "   PAGE_SIZE= " << testPageSize << ';'
                << "   DISTRIBUTED_JOINS=" << BoolToStr(testDistributedJoins, false) << ';'
                << "LAZY=" << BoolToStr(testLazy, false) << ';'
                << "COLLOCATED    =" << BoolToStr(testCollocated, false) << "  ;"
                << "  REPLICATED_ONLY=   " << BoolToStr(testReplicatedOnly, false) << ';'
                << "ENFORCE_JOIN_ORDER=   " << BoolToStr(testEnforceJoinOrder, false) << "  ;"
                << "SCHEMA = \n\r" << testSchemaName << ';'
                << " skip_reducer_on_update=" << BoolToStr(testSkipReducerOnUpdate, false);

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringInvalidAddress)
{
    Configuration cfg;

    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:0;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:00000;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:fdsf;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:123:1;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:12322221;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:12322a;"), ignite::IgniteError);
    BOOST_CHECK_THROW(cfg.FillFromConnectString("Address=example.com:;"), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(TestConnectStringValidAddress)
{
    CheckValidAddress("Address=example.com:1;", 1);
    CheckValidAddress("Address=example.com:31242;", 31242);
    CheckValidAddress("Address=example.com:55555;", 55555);
    CheckValidAddress("Address=example.com:110;", 110);
    CheckValidAddress("Address=example.com;", Configuration::DefaultValue::port);
}

BOOST_AUTO_TEST_CASE(TestConnectStringInvalidVersion)
{
    CheckInvalidProtocolVersion("Protocol_Version=0;");
    CheckInvalidProtocolVersion("Protocol_Version=1;");
    CheckInvalidProtocolVersion("Protocol_Version=2;");
    CheckInvalidProtocolVersion("Protocol_Version=2.1;");
}

BOOST_AUTO_TEST_CASE(TestConnectStringUnsupportedVersion)
{
    CheckUnsupportedProtocolVersion("Protocol_Version=1.6.1;");
    CheckUnsupportedProtocolVersion("Protocol_Version=1.7.0;");
    CheckUnsupportedProtocolVersion("Protocol_Version=1.8.1;");
}

BOOST_AUTO_TEST_CASE(TestConnectStringValidVersion)
{
    CheckValidProtocolVersion("Protocol_Version=2.1.0;", ignite::odbc::ProtocolVersion::VERSION_2_1_0);
    CheckValidProtocolVersion("Protocol_Version=1.6.1;", ignite::odbc::ProtocolVersion(1, 6, 1));
    CheckValidProtocolVersion("Protocol_Version=1.7.0;", ignite::odbc::ProtocolVersion(1, 7, 0));
    CheckValidProtocolVersion("Protocol_Version=1.8.1;", ignite::odbc::ProtocolVersion(1, 8, 1));
}

BOOST_AUTO_TEST_CASE(TestConnectStringSupportedVersion)
{
    CheckSupportedProtocolVersion("Protocol_Version=2.1.0;");
}

BOOST_AUTO_TEST_CASE(TestConnectStringInvalidBoolKeys)
{
    typedef std::set<std::string> Set;

    Set keys;

    keys.insert("distributed_joins");
    keys.insert("enforce_join_order");
    keys.insert("replicated_only");
    keys.insert("collocated");
    keys.insert("lazy");
    keys.insert("skip_reducer_on_update");

    for (Set::const_iterator it = keys.begin(); it != keys.end(); ++it)
    {
        const std::string& key = *it;

        CheckInvalidBoolValue(key + "=1;", key);
        CheckInvalidBoolValue(key + "=0;", key);
        CheckInvalidBoolValue(key + "=42;", key);
        CheckInvalidBoolValue(key + "=truee;", key);
        CheckInvalidBoolValue(key + "=flase;", key);
        CheckInvalidBoolValue(key + "=falsee;", key);
        CheckInvalidBoolValue(key + "=yes;", key);
        CheckInvalidBoolValue(key + "=no;", key);
    }
}

BOOST_AUTO_TEST_CASE(TestConnectStringValidBoolKeys)
{
    typedef std::set<std::string> Set;

    Set keys;

    keys.insert("distributed_joins");
    keys.insert("enforce_join_order");
    keys.insert("replicated_only");
    keys.insert("collocated");
    keys.insert("lazy");
    keys.insert("skip_reducer_on_update");

    for (Set::const_iterator it = keys.begin(); it != keys.end(); ++it)
    {
        const std::string& key = *it;

        CheckValidBoolValue(key + "=true;", key, true);
        CheckValidBoolValue(key + "=True;", key, true);
        CheckValidBoolValue(key + "=TRUE;", key, true);

        CheckValidBoolValue(key + "=false;", key, false);
        CheckValidBoolValue(key + "=False;", key, false);
        CheckValidBoolValue(key + "=FALSE;", key, false);
    }
}

BOOST_AUTO_TEST_CASE(TestDsnStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER=" << testDriverName << '\0'
                << "DSN={" << testDsn << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStringLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver=" << testDriverName << '\0'
                << "dsn={" << testDsn << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStringMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver=" << testDriverName << '\0'
                << "Dsn={" << testDsn << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStringWhitespaces)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << " DRIVER =  " << testDriverName << "\r\n" << '\0'
                << "DSN= {" << testDsn << "} \n" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_SUITE_END()
