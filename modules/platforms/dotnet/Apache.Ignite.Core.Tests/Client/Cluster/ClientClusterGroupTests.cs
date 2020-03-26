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

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Client.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Cluster group API tests for thin client.
    /// </summary>
    [TestFixture]
    public class ClientClusterGroupTests : ClientTestBase
    {
        private static readonly string ExpectedErrorMessage =
            "'name' argument should not be null or empty." + Environment.NewLine + "Parameter name: name";

        /// <summary>
        /// Test thin client cluster group returns the same nodes collection as thick one.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodesAsThickOne()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();

            Assert.IsNotEmpty(nodes);
            Assert.IsNotEmpty(clientNodes);
            AssertNodesAreEqual(nodes, clientNodes);
        }

        /// <summary>
        /// Test thin client cluster group returns the same node as thick one.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodeAsThickOne()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();
            var clientNode = Client.GetCluster().GetNode();

            AssertNodesAreEqual(node, clientNode);
        }

        /// <summary>
        /// Test cluster returns the same node instance over
        /// the different calls when no topology changes have been made.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodeWithSameTopology()
        {
            var clientNode = Client.GetCluster().GetNodes().SingleOrDefault();
            var clientNode2 = Client.GetCluster().GetNodes().SingleOrDefault();

            Assert.AreSame(clientNode, clientNode2);
            Assert.AreSame(Client.GetCluster().GetNode(), Client.GetCluster().GetNode());
        }

        /// <summary>
        /// Test cluster returns node by id.
        /// </summary>
        [Test]
        public void TestClusterGroupReturnsNodeById()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();
            var clientNode = Client.GetCluster().GetNode(node.Id);

            AssertNodesAreEqual(node, clientNode);
        }

        /// <summary>
        /// Test cluster throws exception when node is accessed with empty Guid.
        /// </summary>
        [Test]
        public void TestClusterGroupGetNodeChecksNodeId()
        {
            TestDelegate action = () => Client.GetCluster().GetNode(Guid.Empty);

            var ex = Assert.Throws<ArgumentException>(action);
            Assert.AreEqual("Node id should not be empty.", ex.Message);
        }

        /// <summary>
        /// Test cluster group reflects new nodes changes.
        /// </summary>
        [Test]
        public void TestClusterGroupDetectsNewTopologyChanges()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();

            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                var nodesNew = Ignition.GetIgnite().GetCluster().GetNodes();
                var clientNodesNew = Client.GetCluster().GetNodes();

                Assert.AreEqual(2, clientNodesNew.Count);
                AssertNodesAreEqual(nodesNew, clientNodesNew);

                var newNode = nodesNew.Single(x => x.Id == nodes.Single().Id);
                var newClientNode = clientNodesNew.Single(x => x.Id == clientNodes.Single().Id);

                AssertNodesAreEqual(newNode, newClientNode);
            }
        }

        /// <summary>
        /// Test cluster group throws exception if unknown
        /// node ids have been requested from a client.
        /// </summary>
        [Test]
        public void TestClusterGroupThrowsExceptionInCaseOfUnknownNodes()
        {
            var invalidNodeIds = new[] {Guid.Empty};
            var clusterGroup = (ClientClusterGroup) Client.GetCluster();

            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            clusterGroup.UpdateTopology(1000L, invalidNodeIds);

            TestDelegate action = () => clusterGroup.GetNode();

            ArgumentException exception = Assert.Throws<ArgumentException>(action);
            Assert.AreEqual("Unable to find node with id='00000000-0000-0000-0000-000000000000'",
                exception.Message);
        }

        /// <summary>
        /// Test cluster group doesn't update properties if no changes have been detected.
        /// </summary>
        [Test]
        public void TestClusterGroupDoesNotUpdateTopologyIfNoChangesDetected()
        {
            var clusterGroup = (ClientClusterGroup) Client.GetCluster();
            IClientClusterNode node = clusterGroup.GetNode();

            // Set the wrong ids, but keep the same topology version.
            var invalidNodeIds = new[] {Guid.NewGuid(), Guid.Empty};
            clusterGroup.UpdateTopology(1L, invalidNodeIds);

            Assert.AreSame(node, clusterGroup.GetNode());
        }

        /// <summary>
        /// Test cluster group throws exception if predicate is null.
        /// </summary>
        [Test]
        public void TestClusterGroupForPredicateThrowsExceptionIfItNull()
        {
            TestDelegate action = () => Client.GetCluster().ForPredicate(null);

            var ex = Assert.Throws<ArgumentNullException>(action);
            Assert.AreEqual("Value cannot be null." + Environment.NewLine + "Parameter name: p", ex.Message);
        }

        /// <summary>
        /// Test cluster group applies a native predicate to nodes result set.
        /// </summary>
        [Test]
        public void TestClusterGroupAppliesPredicate()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();

            var clientNode = Client
                .GetCluster()
                .ForPredicate(x => x.Id != Guid.Empty)
                .ForPredicate(x => x.Id != node.Id)
                .GetNode();

            Assert.IsNull(clientNode);
        }

        /// <summary>
        /// Test cluster group applies simple filters.
        /// </summary>
        [Test]
        public void TestClusterGroupAppliesFilters()
        {
            var node = Ignition.GetIgnite().GetCluster().ForDotNet().GetNode();
            var clientNode = Client.GetCluster().ForDotNet().GetNode();

            AssertNodesAreEqual(node, clientNode);

            const string attrName = "unknownAttr";
            var unknownNode = Ignition.GetIgnite().GetCluster().ForAttribute(attrName, null).GetNode();
            var unknownClientNode = Client.GetCluster().ForDotNet().ForAttribute(attrName, null).GetNode();

            Assert.IsNull(unknownNode);
            Assert.IsNull(unknownClientNode);
        }

        /// <summary>
        /// Test cluster group <see cref="IClientClusterGroup.ForAttribute"/>
        /// does not allow empty attribute names.
        /// </summary>
        [Test]
        public void TestClusterGroupThrownExceptionForNullAttributeName()
        {
            TestDelegate action = () => Client.GetCluster().ForAttribute(null, null);

            var ex = Assert.Throws<ArgumentException>(action);
            Assert.AreEqual(ExpectedErrorMessage, ex.Message);
        }


        /// <summary>
        /// Test cluster group <see cref="IClientClusterGroup.ForAttribute"/>
        /// does not allow empty attribute names.
        /// </summary>
        [Test]
        public void TestClusterGroupThrownExceptionForEmptyAttributeName()
        {
            TestDelegate action = () => Client.GetCluster().ForAttribute(string.Empty, null);

            var ex = Assert.Throws<ArgumentException>(action);
            Assert.AreEqual(ExpectedErrorMessage, ex.Message);
        }

        /// <summary>
        /// Test that derived cluster group has no affect on it's parent.
        /// </summary>
        [Test]
        public void TestClusterGroupFiltersDoesNotAffectEachOther()
        {
            const string nodeAttrKey = "myAttr";
            const string nodeAttrVal = "myVal";
            var cfg = TestUtils.GetTestConfiguration();
            cfg.UserAttributes = new Dictionary<string, object> {{nodeAttrKey, nodeAttrVal}};
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                var dotNetNodes = Client.GetCluster().ForServers().ForDotNet();
                Assert.AreEqual(2, dotNetNodes.GetNodes().Count);

                var dotNetWithMyAttr = dotNetNodes.ForAttribute(nodeAttrKey, nodeAttrVal);
                Assert.AreEqual(1, dotNetWithMyAttr.GetNodes().Count);

                // Start client node and force topology changes.
                var clientCfg = TestUtils.GetTestConfiguration(name: "clientNode");
                clientCfg.ClientMode = true;
                using (Ignition.Start(clientCfg))
                {
                    var newNodes = Client.GetCluster().ForServers().ForDotNet().GetNodes();
                    Assert.AreEqual(2, newNodes.Count);

                    // Initial projection should keep it's result set.
                    Assert.AreEqual(newNodes.Count, dotNetNodes.GetNodes().Count);
                }
            }
        }

        /// <summary>
        /// Asserts that client and server node representations are equals.
        /// </summary>
        /// <param name="clusterNode"></param>
        /// <param name="clientNode"></param>
        private static void AssertNodesAreEqual(IClusterNode clusterNode, IClientClusterNode clientNode)
        {
            AssertExtensions.ReflectionEqual(clusterNode.Id, clientNode.Id);
            AssertExtensions.ReflectionEqual(clusterNode.Addresses, clientNode.Addresses);
            AssertExtensions.ReflectionEqual(clusterNode.HostNames, clientNode.HostNames);
            AssertExtensions.ReflectionEqual(clusterNode.IsClient, clientNode.IsClient);
            AssertExtensions.ReflectionEqual(clusterNode.IsDaemon, clientNode.IsDaemon);
            AssertExtensions.ReflectionEqual(clusterNode.IsLocal, clientNode.IsLocal);
            AssertExtensions.ReflectionEqual(clusterNode.Order, clientNode.Order);
            AssertExtensions.ReflectionEqual(clusterNode.Version, clientNode.Version);
            AssertExtensions.ReflectionEqual(clusterNode.Attributes, clientNode.Attributes);
        }

        /// <summary>
        /// Asserts that client and server nodes collections are equals.
        /// </summary>
        /// <param name="clusterNodes"></param>
        /// <param name="clientNodes"></param>
        private static void AssertNodesAreEqual(ICollection<IClusterNode> clusterNodes,
            ICollection<IClientClusterNode> clientNodes)
        {
            Assert.AreEqual(clusterNodes.Count, clientNodes.Count);

            using (IEnumerator<IClusterNode> enumServer = clusterNodes.GetEnumerator())
            using (IEnumerator<IClientClusterNode> enumClient = clientNodes.GetEnumerator())
            {
                while (enumServer.MoveNext() && enumClient.MoveNext())
                {
                    AssertNodesAreEqual(enumServer.Current, enumClient.Current);
                }
            }
        }
    }
}
