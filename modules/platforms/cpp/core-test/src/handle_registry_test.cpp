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

#include "ignite/impl/handle_registry.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl;

struct HandleRegistryTestProbe
{
    bool deleted;
    
    HandleRegistryTestProbe()
    {
        deleted = false;
    }
};

class HandleRegistryTestEntry
{
public:
    HandleRegistryTestEntry(HandleRegistryTestProbe* probe) : probe(probe)
    {
        // No-op.
    }

    virtual ~HandleRegistryTestEntry()
    {
        probe->deleted = true;
    }

private:
    HandleRegistryTestProbe* probe;
};

BOOST_AUTO_TEST_SUITE(HandleRegistryTestSuite)

BOOST_AUTO_TEST_CASE(TestCritical)
{
    HandleRegistry reg(2, 1);

    HandleRegistryTestProbe probe0;
    HandleRegistryTestProbe probe1;
    HandleRegistryTestProbe probe2;

    HandleRegistryTestEntry* entry0 = new HandleRegistryTestEntry(&probe0);
    HandleRegistryTestEntry* entry1 = new HandleRegistryTestEntry(&probe1);
    HandleRegistryTestEntry* entry2 = new HandleRegistryTestEntry(&probe2);

    int64_t hnd0 = reg.AllocateCritical(SharedPointer<HandleRegistryTestEntry>(entry0));
    int64_t hnd1 = reg.AllocateCritical(SharedPointer<HandleRegistryTestEntry>(entry1));
    int64_t hnd2 = reg.AllocateCritical(SharedPointer<HandleRegistryTestEntry>(entry2));

    BOOST_REQUIRE(reg.Get(hnd0).Get() == entry0);
    BOOST_REQUIRE(!probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == entry1);
    BOOST_REQUIRE(!probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == entry2);
    BOOST_REQUIRE(!probe2.deleted);

    reg.Release(hnd0);

    BOOST_REQUIRE(reg.Get(hnd0).Get() == NULL);
    BOOST_REQUIRE(probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == entry1);
    BOOST_REQUIRE(!probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == entry2);
    BOOST_REQUIRE(!probe2.deleted);

    reg.Close();

    BOOST_REQUIRE(reg.Get(hnd0).Get() == NULL);
    BOOST_REQUIRE(probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == NULL);
    BOOST_REQUIRE(probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == NULL);
    BOOST_REQUIRE(probe2.deleted);

    HandleRegistry closedReg(2, 1);

    closedReg.Close();

    HandleRegistryTestProbe closedProbe;
    HandleRegistryTestEntry* closedEntry = new HandleRegistryTestEntry(&closedProbe);

    int64_t closedHnd = closedReg.AllocateCritical(SharedPointer<HandleRegistryTestEntry>(closedEntry));
    BOOST_REQUIRE(closedHnd == -1);
    BOOST_REQUIRE(closedProbe.deleted);
}

BOOST_AUTO_TEST_CASE(TestNonCritical)
{
    HandleRegistry reg(0, 2);

    HandleRegistryTestProbe probe0;
    HandleRegistryTestProbe probe1;
    HandleRegistryTestProbe probe2;

    HandleRegistryTestEntry* entry0 = new HandleRegistryTestEntry(&probe0);
    HandleRegistryTestEntry* entry1 = new HandleRegistryTestEntry(&probe1);
    HandleRegistryTestEntry* entry2 = new HandleRegistryTestEntry(&probe2);

    int64_t hnd0 = reg.AllocateCritical(SharedPointer<HandleRegistryTestEntry>(entry0));
    int64_t hnd1 = reg.Allocate(SharedPointer<HandleRegistryTestEntry>(entry1));
    int64_t hnd2 = reg.Allocate(SharedPointer<HandleRegistryTestEntry>(entry2));

    BOOST_REQUIRE(reg.Get(hnd0).Get() == entry0);
    BOOST_REQUIRE(!probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == entry1);
    BOOST_REQUIRE(!probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == entry2);
    BOOST_REQUIRE(!probe2.deleted);

    reg.Release(hnd0);

    BOOST_REQUIRE(reg.Get(hnd0).Get() == NULL);
    BOOST_REQUIRE(probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == entry1);
    BOOST_REQUIRE(!probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == entry2);
    BOOST_REQUIRE(!probe2.deleted);

    reg.Close();

    BOOST_REQUIRE(reg.Get(hnd0).Get() == NULL);
    BOOST_REQUIRE(probe0.deleted);

    BOOST_REQUIRE(reg.Get(hnd1).Get() == NULL);
    BOOST_REQUIRE(probe1.deleted);

    BOOST_REQUIRE(reg.Get(hnd2).Get() == NULL);
    BOOST_REQUIRE(probe2.deleted);

    HandleRegistry closedReg(0, 2);

    closedReg.Close();

    HandleRegistryTestProbe closedProbe;
    HandleRegistryTestEntry* closedEntry = new HandleRegistryTestEntry(&closedProbe);

    int64_t closedHnd = closedReg.Allocate(SharedPointer<HandleRegistryTestEntry>(closedEntry));
    BOOST_REQUIRE(closedHnd == -1);
    BOOST_REQUIRE(closedProbe.deleted);
}

BOOST_AUTO_TEST_SUITE_END()
