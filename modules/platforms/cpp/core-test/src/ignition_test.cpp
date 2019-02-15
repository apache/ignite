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
