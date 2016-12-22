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