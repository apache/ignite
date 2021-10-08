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
#include <ignite/compute_types.h>

using namespace ignite;
using namespace ignite::common;
using namespace ignite::common::concurrent;
using namespace ignite::cluster;

using namespace boost::unit_test;

/*
 * Predicate holder is required to demonstrate
 * how to pass IgnitePredicate pointer to the stl container.
 */
class PredHolder
{
public:
    PredHolder(IgnitePredicate<ClusterNode>* p) :
        p(p)
    {
        // No-op.
    }

    bool operator()(ClusterNode& node)
    {
        return p->operator()(node);
    }

private:
    IgnitePredicate<ClusterNode>* p;
};

/*
 * Test setup fixture.
 */
struct ClusterGroupTestSuiteFixture
{
    Ignite server1;
    Ignite server2;
    Ignite server3;
    Ignite client;

    /*
     * Constructor.
     */
    ClusterGroupTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        server1(ignite_test::StartNode("compute-server0-32.xml", "ClusterTestServer0")),
        server2(ignite_test::StartNode("compute-server1-32.xml", "ClusterTestServer1")),
        server3(ignite_test::StartNode("compute-server1-32.xml", "ClusterTestServer2")),
        client(ignite_test::StartNode("compute-client-32.xml", "ClusterTestClient"))
#else
        server1(ignite_test::StartNode("compute-server0.xml", "ClusterTestServer0")),
        server2(ignite_test::StartNode("compute-server1.xml", "ClusterTestServer1")),
        server3(ignite_test::StartNode("compute-server1.xml", "ClusterTestServer2")),
        client(ignite_test::StartNode("compute-client.xml", "ClusterTestClient"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ClusterGroupTestSuiteFixture()
    {
        Ignition::Stop(client.GetName(), true);
        Ignition::StopAll(true);
    }
};

/*
 * Test setup fixture.
 */
struct ClusterGroupTestSuiteFixtureIsolated
{
    Ignite node;

    /*
     * Constructor.
     */
    ClusterGroupTestSuiteFixtureIsolated() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("isolated-32.xml", "ClusterTestIsolated"))
#else
        node(ignite_test::StartNode("isolated.xml", "ClusterTestIsolated"))
#endif
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ClusterGroupTestSuiteFixtureIsolated()
    {
        Ignition::StopAll(true);
    }
};

BOOST_FIXTURE_TEST_SUITE(ClusterGroupTestSuite, ClusterGroupTestSuiteFixture)

BOOST_AUTO_TEST_CASE(IgniteGetCluster)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());
}

BOOST_AUTO_TEST_CASE(IgniteForAttribute)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForAttribute("TestAttribute", "Value0");
    ClusterGroup group2 = cluster.AsClusterGroup().ForAttribute("TestAttribute", "Value1");
    ClusterGroup group3 = cluster.AsClusterGroup().ForAttribute("NotExistAttribute", "Value0");
    ClusterGroup group4 = cluster.AsClusterGroup().ForAttribute("TestAttribute", "NotExistValue");

    BOOST_REQUIRE(group1.GetNodes().size() == 1);
    BOOST_REQUIRE(group2.GetNodes().size() == 2);
    BOOST_REQUIRE(group3.GetNodes().size() == 0);
    BOOST_REQUIRE(group4.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForCacheNodes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForCacheNodes("CacheName0");
    ClusterGroup group2 = cluster.AsClusterGroup().ForCacheNodes("CacheName1");
    ClusterGroup group3 = cluster.AsClusterGroup().ForCacheNodes("CacheName2");
    ClusterGroup group4 = cluster.AsClusterGroup().ForCacheNodes("InvalidCacheName");

    BOOST_REQUIRE(group1.GetNodes().size() == 3);
    BOOST_REQUIRE(group2.GetNodes().size() == 3);
    BOOST_REQUIRE(group3.GetNodes().size() == 4);
    BOOST_REQUIRE(group4.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForClientNodes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForClientNodes("CacheName0");
    ClusterGroup group2 = cluster.AsClusterGroup().ForClientNodes("CacheName2");
    ClusterGroup group3 = cluster.AsClusterGroup().ForClientNodes("InvalidCacheName");

    BOOST_REQUIRE(group1.GetNodes().size() == 0);
    BOOST_REQUIRE(group2.GetNodes().size() == 1);
    BOOST_REQUIRE(group3.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForClients)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group0 = cluster.AsClusterGroup().ForClients();

    BOOST_REQUIRE(group0.GetNodes().size() == 1);
    BOOST_REQUIRE(group0.GetNodes().front().IsClient());
}

BOOST_AUTO_TEST_CASE(IgniteForDaemons)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup().ForDaemons();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForDataNodes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForDataNodes("CacheName0");
    ClusterGroup group2 = cluster.AsClusterGroup().ForDataNodes("CacheName1");
    ClusterGroup group3 = cluster.AsClusterGroup().ForDataNodes("CacheName2");
    ClusterGroup group4 = cluster.AsClusterGroup().ForDataNodes("InvalidCacheName");

    BOOST_REQUIRE(group1.GetNodes().size() == 3);
    BOOST_REQUIRE(group2.GetNodes().size() == 3);
    BOOST_REQUIRE(group3.GetNodes().size() == 3);
    BOOST_REQUIRE(group4.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForHost)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup();
    ClusterGroup group2 = cluster.AsClusterGroup().ForHost(group1.GetNodes().front());

    BOOST_REQUIRE(group1.GetNodes().size() == 4);
    BOOST_REQUIRE(group2.GetNodes().size() == 4);
}

BOOST_AUTO_TEST_CASE(IgniteForHostName)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    std::string hostName = "someHostName";

    ClusterGroup group0 = cluster.AsClusterGroup().ForHost(hostName);

    BOOST_REQUIRE(group0.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForHostNames)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    std::vector<std::string> hostNames;
    hostNames.push_back("hostName0");
    hostNames.push_back("hostName1");

    ClusterGroup group0 = cluster.AsClusterGroup().ForHosts(hostNames);

    BOOST_REQUIRE(group0.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForNode)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup();
    ClusterGroup group2 = cluster.AsClusterGroup().ForNode(group1.GetNodes().front());

    BOOST_REQUIRE(group1.GetNodes().size() == 4);
    BOOST_REQUIRE(group2.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForNodeId)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup();
    ClusterGroup group2 = cluster.AsClusterGroup().ForNodeId(group1.GetNodes().front().GetId());
    ClusterGroup group3 = cluster.AsClusterGroup().ForNodeId(Guid(100, 500));

    BOOST_REQUIRE(group1.GetNodes().size() == 4);
    BOOST_REQUIRE(group2.GetNodes().size() == 1);
    BOOST_REQUIRE(group3.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForNodeIds)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group1.GetNodes();
    std::vector<Guid> ids;
    ids.push_back(nodes.at(0).GetId());
    ids.push_back(nodes.at(1).GetId());

    ClusterGroup group2 = cluster.AsClusterGroup().ForNodeIds(ids);

    BOOST_REQUIRE(group1.GetNodes().size() == 4);
    BOOST_REQUIRE(group2.GetNodes().size() == 2);

    std::vector<Guid> emptyIds;
    BOOST_CHECK_THROW(cluster.AsClusterGroup().ForNodeIds(emptyIds), IgniteError);

    std::vector<Guid> notExistIds;
    notExistIds.push_back(Guid(100, 500));
    ClusterGroup group3 = cluster.AsClusterGroup().ForNodeIds(notExistIds);

    BOOST_REQUIRE(group3.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForNodes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup();
    ClusterGroup group2 = cluster.AsClusterGroup().ForServers();
    ClusterGroup group3 = cluster.AsClusterGroup().ForNodes(group2.GetNodes());

    BOOST_REQUIRE(group1.GetNodes().size() == 4);
    BOOST_REQUIRE(group2.GetNodes().size() == 3);
    BOOST_REQUIRE(group3.GetNodes().size() == 3);
}

BOOST_AUTO_TEST_CASE(IgniteForOldest)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForOldest();

    BOOST_REQUIRE(group1.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForPredicate)
{
    IgniteCluster cluster = server1.GetCluster();
    ClusterGroup group0 = cluster.AsClusterGroup();

    ClusterGroup groupServers = group0.ForServers();
    ClusterGroup groupClients = group0.ForClients();
    ClusterGroup group1 = groupServers.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "Value0"));
    ClusterGroup group2 = groupServers.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "Value1"));
    ClusterGroup group3 = groupServers.ForClients();

    BOOST_REQUIRE(group0.GetNodes().size() == 4);
    BOOST_REQUIRE(groupServers.GetNodes().size() == 3);
    BOOST_REQUIRE(groupClients.GetNodes().size() == 1);
    BOOST_REQUIRE(group1.GetNodes().size() == 1);
    BOOST_REQUIRE(group2.GetNodes().size() == 2);
    BOOST_REQUIRE(group3.GetNodes().size() == 0);

    ClusterGroup group4 = group0.ForPredicate(new ignite_test::HasAttrName("TestAttribute"));
    ClusterGroup group5 = group4.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "Value0"));
    ClusterGroup group6 = group4.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "Value1"));
    ClusterGroup group7 = group4.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "ValueInvalid"));

    BOOST_REQUIRE(group4.GetNodes().size() == 3);
    BOOST_REQUIRE(group5.GetNodes().size() == 1);
    BOOST_REQUIRE(group6.GetNodes().size() == 2);
    BOOST_REQUIRE(group7.GetNodes().size() == 0);
}

BOOST_AUTO_TEST_CASE(IgniteForRandom)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForRandom();

    BOOST_REQUIRE(group1.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForRemotes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForRemotes();

    BOOST_REQUIRE(group1.GetNodes().size() == 3);
}

BOOST_AUTO_TEST_CASE(IgniteForServers)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group1 = cluster.AsClusterGroup().ForServers();
    ClusterGroup group2 = cluster.AsClusterGroup().ForServers().ForAttribute("TestAttribute", "Value0");
    ClusterGroup group3 = cluster.AsClusterGroup().ForServers().ForAttribute("TestAttribute", "Value1");

    BOOST_REQUIRE(group1.GetNodes().size() == 3);
    BOOST_REQUIRE(group2.GetNodes().size() == 1);
    BOOST_REQUIRE(group3.GetNodes().size() == 2);
}

BOOST_AUTO_TEST_CASE(IgniteForYoungest)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup().ForYoungest();

    BOOST_REQUIRE(group.GetNodes().size() == 1);
}

BOOST_AUTO_TEST_CASE(IgniteForCpp)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup().ForCpp();

    BOOST_REQUIRE(group.GetNodes().size() == 4);
}

BOOST_AUTO_TEST_CASE(IgniteForLocal)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.ForLocal();

    BOOST_REQUIRE(group.GetNodes().size() == 1);
    BOOST_REQUIRE(group.GetNodes().front().IsLocal());
}

BOOST_AUTO_TEST_CASE(IgniteGetLocalNode)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterNode node = cluster.GetLocalNode();

    BOOST_REQUIRE(node.IsLocal());
}

BOOST_AUTO_TEST_CASE(IgniteGetNode)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    ClusterNode node1 = group.GetNode();
    ClusterNode node2 = group.GetNodes().at(0);
    ClusterNode node3 = group.GetNode(node1.GetId());

    BOOST_REQUIRE(node1.GetId() == node2.GetId());
    BOOST_REQUIRE(node2.GetId() == node3.GetId());
}

BOOST_AUTO_TEST_CASE(IgniteGetNodes)
{
    IgniteCluster cluster = server1.GetCluster();

    BOOST_REQUIRE(cluster.IsActive());

    ClusterGroup group = cluster.AsClusterGroup();

    std::vector<ClusterNode> nodes = group.GetNodes();

    BOOST_REQUIRE(nodes.size() == 4);
}

BOOST_AUTO_TEST_CASE(IgniteGetPredicate)
{
    IgniteCluster cluster = server1.GetCluster();
    ClusterGroup group0 = cluster.AsClusterGroup();
    ClusterGroup group1 = group0.ForPredicate(new ignite_test::HasAttrValue("TestAttribute", "Value1"));

    std::vector<ClusterNode> nodes0 = group0.GetNodes();
    std::vector<ClusterNode> nodes1 = group1.GetNodes();
    int64_t count = std::count_if(nodes0.begin(), nodes0.end(), PredHolder(group1.GetPredicate()));

    BOOST_CHECK_EQUAL(nodes1.size(), count);
}

BOOST_AUTO_TEST_SUITE_END()

BOOST_FIXTURE_TEST_SUITE(ClusterGroupTestSuiteIsolated, ClusterGroupTestSuiteFixtureIsolated)

BOOST_AUTO_TEST_CASE(IgniteSetActive)
{
    BOOST_REQUIRE(node.IsActive());

    node.SetActive(false);

    BOOST_REQUIRE(!node.IsActive());

    node.SetActive(true);

    BOOST_REQUIRE(node.IsActive());
}

BOOST_AUTO_TEST_SUITE_END()