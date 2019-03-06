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

using namespace ignite;
using namespace impl;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(MemoryTestSuite)

BOOST_AUTO_TEST_CASE(MemoryReallocationTest)
{
    using impl::interop::InteropMemory;
    using common::concurrent::SharedPointer;

    IgniteConfiguration cfg;
    IgniteEnvironment env(cfg);

    SharedPointer<InteropMemory> mem = env.AllocateMemory();

    BOOST_CHECK_EQUAL(mem.Get()->Capacity(),
        static_cast<int32_t>(IgniteEnvironment::DEFAULT_ALLOCATION_SIZE));

    BOOST_CHECK(mem.Get()->Data() != NULL);

    // Checking memory for write access.
    int32_t capBeforeReallocation = mem.Get()->Capacity();

    for (int32_t i = 0; i <capBeforeReallocation; ++i)
    {
        int8_t *data = mem.Get()->Data();

        data[i] = static_cast<int8_t>(i);
    }

    mem.Get()->Reallocate(mem.Get()->Capacity() * 3);

    BOOST_CHECK(mem.Get()->Capacity() >= IgniteEnvironment::DEFAULT_ALLOCATION_SIZE * 3);

    // Checking memory data.
    for (int32_t i = 0; i <capBeforeReallocation; ++i)
    {
        int8_t *data = mem.Get()->Data();

        BOOST_REQUIRE_EQUAL(data[i], static_cast<int8_t>(i));
    }

    // Checking memory for write access.
    capBeforeReallocation = mem.Get()->Capacity();

    for (int32_t i = 0; i <capBeforeReallocation; ++i)
    {
        int8_t *data = mem.Get()->Data();

        data[i] = static_cast<int8_t>(i + 42);
    }

    // Trying reallocate memory once more.
    mem.Get()->Reallocate(mem.Get()->Capacity() * 3);

    // Checking memory data.
    for (int32_t i = 0; i <capBeforeReallocation; ++i)
    {
        int8_t *data = mem.Get()->Data();

        BOOST_REQUIRE_EQUAL(data[i], static_cast<int8_t>(i + 42));
    }

    BOOST_CHECK(mem.Get()->Capacity() >= IgniteEnvironment::DEFAULT_ALLOCATION_SIZE * 9);

    // Checking memory for write access.
    memset(mem.Get()->Data(), 0xF0F0F0F0, mem.Get()->Capacity());
}

BOOST_AUTO_TEST_SUITE_END()
