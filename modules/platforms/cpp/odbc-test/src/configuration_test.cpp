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

#include <ignite/odbc/configuration.h>

using namespace ignite::odbc;

#define DRIVER_VAL "Ignite"
#define SERVER_VAL "testhost.com"
#define PORT_VAL 4242
#define CACHE_VAL "TestCache"
#define DSN_VAL "Ignite DSN"

BOOST_AUTO_TEST_SUITE(ConfigurationTestSuite)

void CheckConnectionConfig(const Configuration& cfg)
{
    BOOST_REQUIRE(cfg.GetDriver() == DRIVER_VAL);
    BOOST_REQUIRE(cfg.GetHost() == SERVER_VAL);
    BOOST_REQUIRE(cfg.GetPort() == PORT_VAL);
    BOOST_REQUIRE(cfg.GetCache() == CACHE_VAL);
    BOOST_REQUIRE(cfg.GetDsn().empty());

    std::stringstream constructor;

    constructor << "driver={" << DRIVER_VAL << "};"
                << "server=" << SERVER_VAL << ";"
                << "port=" << PORT_VAL << ";"
                << "cache=" << CACHE_VAL << ";";

    const std::string& expectedStr = constructor.str();

    BOOST_REQUIRE(cfg.ToConnectString() == expectedStr);
}

void CheckDsnConfig(const Configuration& cfg)
{
    BOOST_REQUIRE(cfg.GetDriver() == DRIVER_VAL);
    BOOST_REQUIRE(cfg.GetDsn() == DSN_VAL);
    BOOST_REQUIRE(cfg.GetHost().empty());
    BOOST_REQUIRE(cfg.GetCache().empty());
    BOOST_REQUIRE(cfg.GetPort() == 0);
}

BOOST_AUTO_TEST_CASE(TestConnectStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER={" << DRIVER_VAL << "};"
                << "SERVER=" << SERVER_VAL <<";"
                << "PORT=" << PORT_VAL << ";"
                << "CACHE=" << CACHE_VAL;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver={" << DRIVER_VAL << "};"
                << "server=" << SERVER_VAL << ";"
                << "port=" << PORT_VAL << ";"
                << "cache=" << CACHE_VAL;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver={" << DRIVER_VAL << "};"
                << "Server=" << SERVER_VAL << ";"
                << "Port=" << PORT_VAL << ";"
                << "Cache=" << CACHE_VAL;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestConnectStringWhitepaces)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER = {" << DRIVER_VAL << "} ;\n"
                << " SERVER =" << SERVER_VAL << " ; \n"
                << "PORT= " << PORT_VAL << "; "
                << "CACHE = \n\r" << CACHE_VAL;

    const std::string& connectStr = constructor.str();

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    CheckConnectionConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStringUppercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "DRIVER=" << DRIVER_VAL << '\0'
                << "DSN={" << DSN_VAL << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStrinLowercase)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "driver=" << DRIVER_VAL << '\0'
                << "dsn={" << DSN_VAL << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStrinMixed)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << "Driver=" << DRIVER_VAL << '\0'
                << "Dsn={" << DSN_VAL << "}" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_CASE(TestDsnStrinWhitespaces)
{
    Configuration cfg;

    std::stringstream constructor;

    constructor << " DRIVER =  " << DRIVER_VAL << "\r\n" << '\0'
                << "DSN= {" << DSN_VAL << "} \n" << '\0' << '\0';

    const std::string& configStr = constructor.str();

    cfg.FillFromConfigAttributes(configStr.data());

    CheckDsnConfig(cfg);
}

BOOST_AUTO_TEST_SUITE_END()