/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/test/unit_test.hpp>
#include <boost/chrono.hpp>
#include <boost/thread.hpp>

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cluster;
using namespace ignite::common::concurrent;
using namespace ignite_test;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct AffinityTestSuiteFixture
{
    Ignite node;

    Cache<int32_t, int32_t> cache;
    CacheAffinity<int32_t> affinity;

    Ignite MakeNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "cache-test-32.xml";
#else
        const char* config = "cache-test.xml";
#endif
        return StartNode(config, name);
    }

    /*
     * Constructor.
     */
    AffinityTestSuiteFixture() :
        node(MakeNode("AffinityNode1")),
        cache(node.GetCache<int32_t, int32_t>("partitioned3")),
        affinity(node.GetAffinity<int32_t>(cache.GetName()))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~AffinityTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(AffinityTestSuite, AffinityTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteAffinityGetPartition)
{
    Ignite node0 = MakeNode("AffinityNode2");
    Cache<int32_t, int32_t> cache0 = node.GetCache<int32_t, int32_t>("partitioned2");
    CacheAffinity<int32_t> affinity0 = node.GetAffinity<int32_t>(cache.GetName());

    BOOST_CHECK_EQUAL(affinity.GetPartition(0), affinity0.GetPartition(0));
    BOOST_CHECK_EQUAL(affinity.GetPartition(1), affinity0.GetPartition(1));
    BOOST_CHECK_EQUAL(affinity.GetPartition(2), affinity0.GetPartition(2));
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetDifferentPartitions)
{
    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_CHECK_EQUAL(affinity.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity.GetPrimaryPartitions(nodes.front()).size(),
        affinity.GetAllPartitions(nodes.front()).size());

    Ignite node0 = MakeNode("AffinityNode2");
    Cache<int32_t, int32_t> cache0 = node0.GetCache<int32_t, int32_t>("partitioned2");
    CacheAffinity<int32_t> affinity0 = node0.GetAffinity<int32_t>(cache.GetName());

    BOOST_CHECK_EQUAL(affinity0.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity0.GetPrimaryPartitions(nodes.front()).size(),
        affinity0.GetAllPartitions(nodes.front()).size());
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetAffinityKey)
{
    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(10)), 10);
    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(20)), 20);

    Ignite node0 = MakeNode("AffinityNode2");
    Cache<int32_t, int32_t> cache0 = node.GetCache<int32_t, int32_t>("partitioned2");
    CacheAffinity<int32_t> affinity0 = node.GetAffinity<int32_t>(cache.GetName());

    BOOST_CHECK_EQUAL((affinity0.GetAffinityKey<int>(10)), 10);
    BOOST_CHECK_EQUAL((affinity0.GetAffinityKey<int>(20)), 20);
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeysToNodes)
{
    std::list<int32_t> keys;
    for (int i = 1; i < 10000; i++)
        keys.push_back(i);

    std::map<ClusterNode, std::list<int32_t> > map = affinity.MapKeysToNodes(keys);

    BOOST_REQUIRE(map.size() == 1);

    for (std::list<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    {
        ClusterNode clusterNode = affinity.MapKeyToNode(*it);
        BOOST_REQUIRE(map.find(clusterNode) != map.end());

        std::list<int32_t> nodeKeys = map[clusterNode];
        BOOST_REQUIRE(nodeKeys.size() > 0);
        BOOST_REQUIRE(std::find(nodeKeys.begin(), nodeKeys.end(), *it) != nodeKeys.end());
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeyToPrimaryAndBackups)
{
    const int32_t key = 1;

    std::vector<ClusterNode> nodes = affinity.MapKeyToPrimaryAndBackups(key);

    BOOST_REQUIRE(nodes.size() == 1);
    BOOST_REQUIRE(true == affinity.IsPrimary(nodes.front(), key));

    int part = affinity.GetPartition(key);
    std::vector<ClusterNode> partNodes = affinity.MapPartitionToPrimaryAndBackups(part);

    BOOST_REQUIRE(nodes.front().GetId() == partNodes.front().GetId());
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapPartitionsToNodes)
{
    Ignite node0 = MakeNode("AffinityNode2");
    Cache<int32_t, int32_t> cache0 = node.GetCache<int32_t, int32_t>("partitioned2");
    CacheAffinity<int32_t> affinity0 = node.GetAffinity<int32_t>(cache.GetName());

    Ignite node1 = MakeNode("AffinityNode3");

    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_REQUIRE(nodes.size() == 3);

    std::vector<int32_t> primary = affinity.GetPrimaryPartitions(nodes[0]);
    std::vector<int32_t> primary0 = affinity.GetPrimaryPartitions(nodes[1]);

    std::sort(primary.begin(), primary.end());
    std::sort(primary0.begin(), primary0.end());

    BOOST_REQUIRE(primary != primary0);

    primary.insert(primary.end(), primary0.begin(), primary0.end());
    std::map<int32_t, ClusterNode> map = affinity.MapPartitionsToNodes(primary);
    for (std::map<int32_t, ClusterNode>::const_iterator it = map.begin(); it != map.end(); ++it)
    {
        std::vector<cluster::ClusterNode> nodes = affinity.MapPartitionToPrimaryAndBackups(it->first);
        BOOST_REQUIRE(nodes.front().GetId() == it->second.GetId());
    }
}

BOOST_AUTO_TEST_SUITE_END()