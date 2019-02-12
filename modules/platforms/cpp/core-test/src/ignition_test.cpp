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

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/test_utils.h"

using namespace ignite;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(IgnitionTestSuite)

BOOST_AUTO_TEST_CASE(TestIgnition)
{
    IgniteConfiguration cfg;

#ifdef IGNITE_TESTS_32
    ignite_test::InitConfig(cfg, "persistence-store-32.xml");
#else
    ignite_test::InitConfig(cfg, "persistence-store.xml");
#endif

    IgniteError err;

    // Start two Ignite instances.
    Ignite grid1 = Ignition::Start(cfg, "ignitionTest-1", err);
    
    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());
    
    BOOST_REQUIRE(strcmp(grid1.GetName(), "ignitionTest-1") == 0);

    Ignite grid2 = Ignition::Start(cfg, "ignitionTest-2", err);

    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(strcmp(grid2.GetName(), "ignitionTest-2") == 0);

    // Test get
    Ignite grid0 = Ignition::Get("ignitionTest-1", err);
    
    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(strcmp(grid0.GetName(), grid1.GetName()) == 0);

    // Stop one grid
    Ignition::Stop(grid1.GetName(), true);
    
    Ignition::Get("ignitionTest-1", err);
    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_GENERIC);
    
    Ignition::Get("ignitionTest-2", err);
    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_SUCCESS);

    // Stop all
    Ignition::StopAll(true);
    
    Ignition::Get("ignitionTest-2", err);
    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_ERR_GENERIC);    
}

BOOST_AUTO_TEST_CASE(TestStartWithpersistence)
{
    IgniteConfiguration cfg;

#ifdef IGNITE_TESTS_32
    ignite_test::InitConfig(cfg, "persistence-store-32.xml");
#else
    ignite_test::InitConfig(cfg, "persistence-store.xml");
#endif
    try
    {
        Ignite grid = Ignition::Start(cfg, "test");
    }
    catch (...)
    {
        // Stop all
        Ignition::StopAll(true);

        throw;
    }
}

BOOST_AUTO_TEST_CASE(GracefulDeathOnInvalidConfig)
{
    IgniteConfiguration cfg;

    ignite_test::InitConfig(cfg, "invalid.xml");

    BOOST_CHECK_THROW(Ignition::Start(cfg), IgniteError);

    Ignition::StopAll(false);
}

BOOST_AUTO_TEST_SUITE_END()
