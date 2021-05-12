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

using namespace ignite;
using namespace impl;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(MemoryTestSuite)

BOOST_AUTO_TEST_CASE(MemoryReallocationTest)
{
    using impl::interop::InteropMemory;
    using common::concurrent::SharedPointer;

    IgniteConfiguration cfg;
    SP_IgniteEnvironment env = SP_IgniteEnvironment(new IgniteEnvironment(cfg));

    SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();

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
