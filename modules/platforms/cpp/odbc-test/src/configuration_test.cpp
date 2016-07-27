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
    const std::string testCacheName = "TestCache";
    const std::string testDsn = "Ignite DSN";

    const std::string testAddress = testServerHost + ':' + ignite::common::LexicalCast<std::string>(testServerPort);
}

void CheckValidAddress(const char* connectStr, uint16_t port)
{
    Configuration cfg;

    BOOST_CHECK_NO_THROW(cfg.FillFromConnectString(connectStr));

    BOOST_CHECK_EQUAL(cfg.GetPort(), port);
}

void CheckConnectionConfig(const Configuration& cfg)
{
    BOOST_CHECK_EQUAL(cfg.GetDriver(), testDriverName);
    BOOST_CHECK_EQUAL(cfg.GetHost(), testServerHost);
    BOOST_CHECK_EQUAL(cfg.GetPort(), testServerPort);
    BOOST_CHECK_EQUAL(cfg.GetAddress(), testAddress);
    BOOST_CHECK_EQUAL(cfg.GetCache(), testCacheName);
    BOOST_CHECK_EQUAL(cfg.GetDsn(), std::string());

    std::stringstream constructor;

    constructor << "address=" << testAddress << ';'
                << "cache=" << testCacheName << ';'
                << "driver={" << testDriverName << "};";

    const std::string& expectedStr = constructor.str();

    BOOST_CHECK_EQUAL(cfg.ToConnectString(), expectedStr);
}

void CheckDsnConfig(const Configuration& cfg)
{
    BOOST_CHECK_EQUAL(cfg.GetDriver(), testDriverName);
    BOOST_CHECK_EQUAL(cfg.GetDsn(), testDsn);
    BOOST_CHECK_EQUAL(cfg.GetCache(), Configuration::DefaultValue::cache);
    BOOST_CHECK_EQUAL(cfg.GetAddress(), Configuration::DefaultValue::address);
    BOOST_CHECK_EQUAL(cfg.GetHost(), std::string());
    BOOST_CHECK_EQUAL(cfg.GetPort(), Configuration::DefaultValue::uintPort);
}

BOOST_AUTO_TEST_SUITE(ConfigurationTestSuite)

BOOST_AUTO_TEST_CASE(CheckTestValuesNotEquealDefault)
{
    BOOST_CHECK_NE(testDriverName, Configuration::DefaultValue::driver);
    BOOST_CHECK_NE(testAddress, Configuration::DefaultValue::address);
    BOOST_CHECK_NE(testServerPort, Configuration::DefaultValue::uintPort);
    BOOST_CHECK_NE(testCacheName, Configuration::DefaultValue::cache);
    BOOST_CHECK_NE(testDsn, Configuration::DefaultValue::dsn);
}

BOOST_AUTO_TEST_CASE(TestConnectStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER={" << testDriverName << "};"
                << "ADDRESS=" << testAddress << ';'
                << "CACHE=" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "address=" << testAddress << ';'
                << "cache=" << testCacheName;

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
                << "cache=" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size() + 1);

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver={" << testDriverName << "};"
                << "Address=" << testAddress << ';'
                << "Cache=" << testCacheName;

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
                << "CACHE = \n\r" << testCacheName;

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
    Configuration cfg;

    CheckValidAddress("Address=example.com:1;", 1);
    CheckValidAddress("Address=example.com:31242;", 31242);
    CheckValidAddress("Address=example.com:55555;", 55555);
    CheckValidAddress("Address=example.com:110;", 110);
    CheckValidAddress("Address=example.com;", Configuration::DefaultValue::uintPort);
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

BOOST_AUTO_TEST_CASE(TestDsnStrinLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver=" << testDriverName << '\0'
                << "dsn={" << testDsn << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStrinMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver=" << testDriverName << '\0'
                << "Dsn={" << testDsn << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStrinWhitespaces)
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
