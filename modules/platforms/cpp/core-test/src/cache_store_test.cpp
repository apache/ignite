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

#include <boost/test/unit_test.hpp>

#include "ignite/common/utils.h"

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/test_utils.h"

using namespace ignite;
using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct CacheStoreTestSuiteFixture
{
    /* Nodes started during the test. */
    Ignite node1;

    /*
     * Constructor.
     */
    CacheStoreTestSuiteFixture() : 
        node1(ignite_test::StartNode("cache-store.xml", "node1"))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~CacheStoreTestSuiteFixture()
    {
        GetCache().RemoveAll();

        Ignition::StopAll(true);
    }

    /**
     * Cache accessor.
     */
    cache::Cache<int64_t, std::string> GetCache()
    {
        return node1.GetOrCreateCache<int64_t, std::string>("cache1");
    }
};

void FillStore(cache::Cache<int64_t, std::string>& cache, int64_t n)
{
    for (int64_t i = 0; i < n; ++i)
        cache.Put(i, common::LexicalCast<std::string>(i));

    cache.Clear();
}

BOOST_FIXTURE_TEST_SUITE(CacheStoreTestSuite, CacheStoreTestSuiteFixture)

BOOST_AUTO_TEST_CASE(LoadCacheSingleNodeNoPredicate)
{
    const int64_t entriesNum = 100;

    cache::Cache<int64_t, std::string> cache = GetCache();

    BOOST_CHECK(cache.IsEmpty());

    FillStore(cache, entriesNum);

    BOOST_CHECK(cache.IsEmpty());

    cache.LoadCache();

    BOOST_CHECK(!cache.IsEmpty());

    BOOST_CHECK_EQUAL(cache.Size(cache::IGNITE_PEEK_MODE_PRIMARY), entriesNum);

    std::string val42 = cache.Get(42);

    BOOST_CHECK_EQUAL(val42, "42");
}

BOOST_AUTO_TEST_CASE(LoadCacheSeveralNodesNoPredicate)
{
    BOOST_CHECKPOINT("Starting additional node");
    Ignite node2 = ignite_test::StartNode("cache-store.xml", "node2");

    const int64_t entriesNum = 100;

    cache::Cache<int64_t, std::string> cache = GetCache();

    BOOST_CHECK(cache.IsEmpty());

    FillStore(cache, entriesNum);

    BOOST_CHECK(cache.IsEmpty());

    cache.LoadCache();

    BOOST_CHECK(!cache.IsEmpty());

    BOOST_CHECK_EQUAL(cache.Size(cache::IGNITE_PEEK_MODE_PRIMARY), entriesNum);

    std::string val42 = cache.Get(42);

    BOOST_CHECK_EQUAL(val42, "42");
}

BOOST_AUTO_TEST_CASE(LocalLoadCacheSingleNodeNoPredicate)
{
    const int64_t entriesNum = 100;

    cache::Cache<int64_t, std::string> cache = GetCache();

    BOOST_CHECK(cache.IsEmpty());

    FillStore(cache, entriesNum);

    BOOST_CHECK(cache.IsEmpty());

    cache.LocalLoadCache();

    BOOST_CHECK(!cache.IsEmpty());

    BOOST_CHECK_EQUAL(cache.Size(cache::IGNITE_PEEK_MODE_PRIMARY), entriesNum);

    std::string val42 = cache.Get(42);

    BOOST_CHECK_EQUAL(val42, "42");
}

BOOST_AUTO_TEST_SUITE_END()