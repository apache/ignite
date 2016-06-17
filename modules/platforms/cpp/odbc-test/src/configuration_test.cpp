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

using namespace ignite::odbc::config;

namespace
{
    const char* testDriverName = "Ignite";
    const char* testServerHost = "testhost.com";
    const uint16_t testServerPort = 4242;
    const char* testCacheName = "TestCache";
    const char* testDsn = "Ignite DSN";
}

BOOST_AUTO_TEST_SUITE(ConfigurationTestSuite)

void CheckConnectionConfig(const Configuration& cfg)
{
    BOOST_REQUIRE(cfg.GetDriver() == testDriverName);
    BOOST_REQUIRE(cfg.GetHost() == testServerHost);
    BOOST_REQUIRE(cfg.GetPort() == testServerPort);
    BOOST_REQUIRE(cfg.GetCache() == testCacheName);
    BOOST_REQUIRE(cfg.GetDsn().empty());

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "server=" << testServerHost << ";"
                << "port=" << testServerPort << ";"
                << "cache=" << testCacheName << ";";

    const std::string& expectedStr = constructor.str();

    BOOST_REQUIRE(cfg.ToConnectString() == expectedStr);
}

void CheckDsnConfig(const Configuration& cfg)
{
    BOOST_REQUIRE(cfg.GetDriver() == testDriverName);
    BOOST_REQUIRE(cfg.GetDsn() == testDsn);
    BOOST_REQUIRE(cfg.GetHost().empty());
    BOOST_REQUIRE(cfg.GetCache().empty());
    BOOST_REQUIRE(cfg.GetPort() == 0);
}

BOOST_AUTO_TEST_CASE(TestConnectStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER={" << testDriverName << "};"
                << "SERVER=" << testServerHost <<";"
                << "PORT=" << testServerPort << ";"
                << "CACHE=" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "server=" << testServerHost << ";"
                << "port=" << testServerPort << ";"
                << "cache=" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringZeroTerminated)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << testDriverName << "};"
                << "server=" << testServerHost << ";"
                << "port=" << testServerPort << ";"
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
                << "Server=" << testServerHost << ";"
                << "Port=" << testServerPort << ";"
                << "Cache=" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringWhitepaces)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER = {" << testDriverName << "} ;\n"
                << " SERVER =" << testServerHost << " ; \n"
                << "PORT= " << testServerPort << "; "
                << "CACHE = \n\r" << testCacheName;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
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
