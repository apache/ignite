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

#include <ignite/ignition.h>
#include <ignite/test_utils.h>

using namespace ignite;
using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;

using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct ClusterNodeTestSuiteFixture
{
    Ignite server;
    Ignite client;

    /*
     * Constructor.
     */
    ClusterNodeTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        server(ignite_test::StartNode("compute-server0-32.xml", "ClusterTestServer0")),
        client(ignite_test::StartNode("compute-client-32.xml", "ClusterTestClient"))
#else
        server(ignite_test::StartNode("compute-server0.xml", "ClusterTestServer0")),
        client(ignite_test::StartNode("compute-client.xml", "ClusterTestClient"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ClusterNodeTestSuiteFixture()
    {
        Ignition::Stop(client.GetName(), true);
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(ClusterNodeTestSuite, ClusterNodeTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteGetCluster)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());
}

BOOST_AUTO_TEST_CASE(IgniteGetAddresses)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    std::vector<std::string> addrs = nodes[0].GetAddresses();

    BOOST_REQUIRE(addrs.size() != 0);
}

BOOST_AUTO_TEST_CASE(IgniteGetAttribute)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    const std::string ATTR_BUILD_DATE = "org.apache.ignite.build.date";
    BOOST_REQUIRE(nodes[0].IsAttributeSet(ATTR_BUILD_DATE));
    std::string date = nodes[0].GetAttribute<std::string>(ATTR_BUILD_DATE);
    BOOST_REQUIRE(!date.empty());

    const std::string ATTR_BUILD_VER = "org.apache.ignite.build.ver";
    BOOST_REQUIRE(nodes[0].IsAttributeSet(ATTR_BUILD_VER));
    std::string ver = nodes[0].GetAttribute<std::string>(ATTR_BUILD_VER);
    BOOST_REQUIRE(!ver.empty());

    const std::string ATTR_CLIENT_MODE = "org.apache.ignite.cache.client";
    BOOST_REQUIRE(nodes[0].IsAttributeSet(ATTR_CLIENT_MODE));
    BOOST_REQUIRE(nodes[1].IsAttributeSet(ATTR_CLIENT_MODE));
    bool isClient0 = nodes[0].GetAttribute<bool>(ATTR_CLIENT_MODE);
    bool isClient1 = nodes[1].GetAttribute<bool>(ATTR_CLIENT_MODE);
    BOOST_REQUIRE(!isClient0 && isClient1);

    const std::string ATTR_GRID_NAME = "org.apache.ignite.ignite.name";
    BOOST_REQUIRE(nodes[0].IsAttributeSet(ATTR_GRID_NAME));
    std::string gridName = nodes[0].GetAttribute<std::string>(ATTR_GRID_NAME);
    BOOST_REQUIRE(!gridName.empty());


    const std::string ATTR_PHY_RAM = "org.apache.ignite.phy.ram";
    BOOST_REQUIRE(nodes[0].IsAttributeSet(ATTR_PHY_RAM));
    int64_t phyRam = nodes[0].GetAttribute<int64_t>(ATTR_PHY_RAM);
    BOOST_REQUIRE(phyRam > 0);
}

BOOST_AUTO_TEST_CASE(IgniteGetAttributes)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    std::vector<std::string> attrs = nodes[0].GetAttributes();

    BOOST_REQUIRE(!attrs.empty());
}

BOOST_AUTO_TEST_CASE(IgniteGetConsistentId)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    std::string consistentId0 = nodes[0].GetConsistentId();
    std::string consistentId1 = nodes[1].GetConsistentId();

    BOOST_REQUIRE(!consistentId0.empty());
    BOOST_REQUIRE(!consistentId1.empty());
}

BOOST_AUTO_TEST_CASE(IgniteGetHostNames)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    std::vector<std::string> hosts = nodes[0].GetHostNames();

    BOOST_REQUIRE(hosts.empty());
}

BOOST_AUTO_TEST_CASE(IgniteGetId)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    Guid id1 = nodes[0].GetId();
    Guid id2 = nodes[1].GetId();

    BOOST_REQUIRE(id1 != Guid() && id2 != Guid() && id1 != id2);
}

BOOST_AUTO_TEST_CASE(IgniteIsClientDaemonLocal)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    bool isClient = nodes[0].IsClient();
    bool isDaemon = nodes[0].IsDaemon();
    bool isLocal = nodes[0].IsLocal();

    BOOST_REQUIRE(!isClient && !isDaemon && isLocal);

    isClient = nodes[1].IsClient();
    isDaemon = nodes[1].IsDaemon();
    isLocal = nodes[1].IsLocal();

    BOOST_REQUIRE(isClient && !isDaemon && !isLocal);
}

BOOST_AUTO_TEST_CASE(IgniteGetOrder)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    int64_t order1 = nodes[0].GetOrder();
    int64_t order2 = nodes[1].GetOrder();

    BOOST_REQUIRE(order1 > 0 && order2 > 0);
}

BOOST_AUTO_TEST_CASE(IgniteGetVersion)
{
    IgniteCluster cluster = server.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 2);

    IgniteProductVersion ver = nodes[0].GetVersion();

    BOOST_REQUIRE(ver.revHash.size() == IgniteProductVersion::SHA1_LENGTH);
}

BOOST_AUTO_TEST_SUITE_END()