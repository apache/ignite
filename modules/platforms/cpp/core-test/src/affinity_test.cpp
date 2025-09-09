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

    Cache<int32_t, int32_t> cache_backups_0;
    CacheAffinity<int32_t> affinity;

    static Ignite StartNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        const char* config = "affinity-test-32.xml";
#else
        const char* config = "affinity-test.xml";
#endif
        return ::StartNode(config, name);
    }

    /*
     * Constructor.
     */
    AffinityTestSuiteFixture() :
        node(StartNode("AffinityNode1")),
        cache_backups_0(node.GetCache<int32_t, int32_t>("test_backups_0")),
        affinity(node.GetAffinity<int32_t>(cache_backups_0.GetName()))
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

    /**
     * Check whether rebalance is complete for the cluster.
     * @param node0 Ignite node.
     * @param part Partition to check.
     * @return true if complete.
     */
    bool IsRebalanceComplete(Ignite& node0, int32_t part)
    {
        return node0.GetAffinity<int32_t>(cache_backups_0.GetName()).MapKeyToNode(part).IsLocal();
    }

    /**
     * Wait for rebalance.
     * @param node0 Ignite node.
     * @param part Partition to check.
     * @param timeout Timeout to wait.
     * @return True if condition was met, false if timeout has been reached.
     */
    bool WaitForRebalance(Ignite& node0, int32_t part = 1, int32_t timeout = 5000)
    {
        return WaitForCondition(
                boost::bind(&AffinityTestSuiteFixture::IsRebalanceComplete, this, node0, part),
                timeout);
    }
};

BOOST_FIXTURE_TEST_SUITE(AffinityTestSuite, AffinityTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteAffinityGetPartition)
{
    Ignite node0 = StartNode("AffinityNode2");

    Cache<int32_t, int32_t> cache0 = node.GetCache<int32_t, int32_t>("test_backups_0");
    CacheAffinity<int32_t> affinity0 = node.GetAffinity<int32_t>(cache_backups_0.GetName());

    BOOST_REQUIRE(WaitForRebalance(node0));

    BOOST_CHECK_EQUAL(affinity.GetPartition(0), affinity0.GetPartition(0));
    BOOST_CHECK_EQUAL(affinity.GetPartition(1), affinity0.GetPartition(1));
    BOOST_CHECK_EQUAL(affinity.GetPartition(2), affinity0.GetPartition(2));
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetDifferentPartitions)
{
    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_REQUIRE(WaitForRebalance(node));

    BOOST_CHECK_EQUAL(affinity.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity.GetPrimaryPartitions(nodes.front()).size(),
                      affinity.GetAllPartitions(nodes.front()).size());

    Ignite node0 = StartNode("AffinityNode2");

    Cache<int32_t, int32_t> cache0 = node0.GetCache<int32_t, int32_t>("test_backups_0");
    CacheAffinity<int32_t> affinity0 = node0.GetAffinity<int32_t>(cache_backups_0.GetName());

    BOOST_REQUIRE(WaitForRebalance(node0));

    BOOST_CHECK_EQUAL(affinity0.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity0.GetPrimaryPartitions(nodes.front()).size(),
                      affinity0.GetAllPartitions(nodes.front()).size());

    Cache<int32_t, int32_t> cache_backups_1 = node0.GetCache<int32_t, int32_t>("test_backups_1");
    CacheAffinity<int32_t> affinity1 = node0.GetAffinity<int32_t>(cache_backups_1.GetName());

    BOOST_REQUIRE(WaitForRebalance(node0));

    BOOST_CHECK_NE(affinity1.GetBackupPartitions(nodes.front()).size(), 0);
    BOOST_CHECK_EQUAL(affinity1.GetPrimaryPartitions(nodes.front()).size() +
        affinity1.GetBackupPartitions(nodes.front()).size(),
        affinity1.GetAllPartitions(nodes.front()).size());
}

BOOST_AUTO_TEST_CASE(IgniteAffinityGetAffinityKey)
{
    BOOST_REQUIRE(WaitForRebalance(node));

    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(10)), 10);
    BOOST_CHECK_EQUAL((affinity.GetAffinityKey<int>(20)), 20);

    Ignite node0 = StartNode("AffinityNode2");

    Cache<int32_t, int32_t> cache_backups_1 = node.GetCache<int32_t, int32_t>("test_backups_1");
    CacheAffinity<int32_t> affinity0 = node.GetAffinity<int32_t>(cache_backups_1.GetName());

    BOOST_REQUIRE(WaitForRebalance(node0));

    BOOST_CHECK_EQUAL((affinity0.GetAffinityKey<int>(10)), 10);
    BOOST_CHECK_EQUAL((affinity0.GetAffinityKey<int>(20)), 20);
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeysToNodes)
{
    BOOST_REQUIRE(WaitForRebalance(node));

    std::vector<int32_t> keys;

    keys.reserve(10000);

    for (int i = 1; i < 10000; i++)
        keys.push_back(i);

    std::map<ClusterNode, std::vector<int32_t> > map = affinity.MapKeysToNodes(keys);

    BOOST_REQUIRE(map.size() == 1);

    for (std::vector<int>::iterator it = keys.begin(); it != keys.end(); ++it)
    {
        ClusterNode clusterNode = affinity.MapKeyToNode(*it);
        BOOST_REQUIRE(map.find(clusterNode) != map.end());

        std::vector<int32_t> nodeKeys = map[clusterNode];

        BOOST_REQUIRE(nodeKeys.size() > 0);
        BOOST_REQUIRE(std::find(nodeKeys.begin(), nodeKeys.end(), *it) != nodeKeys.end());
    }
}

BOOST_AUTO_TEST_CASE(IgniteAffinityMapKeyToPrimaryAndBackups)
{
    BOOST_REQUIRE(WaitForRebalance(node));

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
    Ignite node0 = StartNode("AffinityNode2");
    Ignite node1 = StartNode("AffinityNode3");
    Ignite node2 = StartNode("AffinityNode4");

    BOOST_REQUIRE(WaitForRebalance(node2, 0));

    std::vector<ClusterNode> nodes = node.GetCluster().AsClusterGroup().GetNodes();

    BOOST_REQUIRE(nodes.size() == 4);

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

        Guid nodeId = it->second.GetId();
        BOOST_REQUIRE_EQUAL(nodes.front().GetId(), nodeId);
        BOOST_REQUIRE(nodeId == nodes[0].GetId() || nodeId == nodes[1].GetId());
    }
}

BOOST_AUTO_TEST_SUITE_END()
