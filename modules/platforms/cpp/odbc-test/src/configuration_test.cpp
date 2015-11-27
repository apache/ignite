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
    #define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include <ignite/odbc/configuration.h>

using namespace ignite::odbc;

#define DRIVER_VAL "Ignite"
#define SERVER_VAL "testhost.com"
#define PORT_VAL 4242
#define PORT_SVAL "4242"
#define CACHE_VAL "TestCache"

BOOST_AUTO_TEST_SUITE(ConfigurationTestSuite)

BOOST_AUTO_TEST_CASE(TestConnectString)
{
    Configuration cfg;

    std::string connectStr = "DRIVER={" DRIVER_VAL "};"
                             "SERVER=" SERVER_VAL ";"
                             "PORT=" PORT_SVAL ";"
                             "CACHE=" CACHE_VAL;

    cfg.FillFromConnectString(connectStr.c_str(), connectStr.size());

    BOOST_REQUIRE(cfg.GetDriver() == DRIVER_VAL);
    BOOST_REQUIRE(cfg.GetHost() == SERVER_VAL);
    BOOST_REQUIRE(cfg.GetPort() == PORT_VAL);
    BOOST_REQUIRE(cfg.GetCache() == CACHE_VAL);
}

BOOST_AUTO_TEST_SUITE_END()