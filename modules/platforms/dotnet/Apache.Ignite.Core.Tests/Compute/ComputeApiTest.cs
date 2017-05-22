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

// ReSharper disable SpecifyACultureInStringConversionExplicitly
// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for compute.
    /// </summary>
    public class ComputeApiTest
    {
        /** Echo task name. */
        public const string EchoTask = "org.apache.ignite.platform.PlatformComputeEchoTask";

        /** Binary argument task name. */
        public const string BinaryArgTask = "org.apache.ignite.platform.PlatformComputeBinarizableArgTask";

        /** Broadcast task name. */
        public const string BroadcastTask = "org.apache.ignite.platform.PlatformComputeBroadcastTask";

        /** Broadcast task name. */
        private const string DecimalTask = "org.apache.ignite.platform.PlatformComputeDecimalTask";

        /** Java binary class name. */
        private const string JavaBinaryCls = "PlatformComputeJavaBinarizable";

        /** Echo type: null. */
        private const int EchoTypeNull = 0;

        /** Echo type: byte. */
        private const int EchoTypeByte = 1;

        /** Echo type: bool. */
        private const int EchoTypeBool = 2;

        /** Echo type: short. */
        private const int EchoTypeShort = 3;

        /** Echo type: char. */
        private const int EchoTypeChar = 4;

        /** Echo type: int. */
        private const int EchoTypeInt = 5;

        /** Echo type: long. */
        private const int EchoTypeLong = 6;

        /** Echo type: float. */
        private const int EchoTypeFloat = 7;

        /** Echo type: double. */
        private const int EchoTypeDouble = 8;

        /** Echo type: array. */
        private const int EchoTypeArray = 9;

        /** Echo type: collection. */
        private const int EchoTypeCollection = 10;

        /** Echo type: map. */
        private const int EchoTypeMap = 11;

        /** Echo type: binarizable. */
        public const int EchoTypeBinarizable = 12;

        /** Echo type: binary (Java only). */
        private const int EchoTypeBinarizableJava = 13;

        /** Type: object array. */
        private const int EchoTypeObjArray = 14;

        /** Type: binary object array. */
        private const int EchoTypeBinarizableArray = 15;

        /** Type: enum. */
        private const int EchoTypeEnum = 16;

        /** Type: enum array. */
        private const int EchoTypeEnumArray = 17;

        /** Type: enum field. */
        private const int EchoTypeEnumField = 18;

        /** Type: affinity key. */
        public const int EchoTypeAffinityKey = 19;

        /** Type: enum from cache. */
        private const int EchoTypeEnumFromCache = 20;

        /** Type: enum array from cache. */
        private const int EchoTypeEnumArrayFromCache = 21;
                
        /** Echo type: IgniteUuid. */
        private const int EchoTypeIgniteUuid = 22;

        /** */
        private const string DefaultCacheName = "default";

        /** First node. */
        private IIgnite _grid1;

        /** Second node. */
        private IIgnite _grid2;

        /** Third node. */
        private IIgnite _grid3;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            TestUtils.KillProcesses();

            var configs = GetConfigs();

            _grid1 = Ignition.Start(Configuration(configs.Item1));
            _grid2 = Ignition.Start(Configuration(configs.Item2));
            _grid3 = Ignition.Start(Configuration(configs.Item3));
        }

        /// <summary>
        /// Gets the configs.
        /// </summary>
        protected virtual Tuple<string, string, string> GetConfigs()
        {
            return Tuple.Create(
                "config\\compute\\compute-grid1.xml",
                "config\\compute\\compute-grid2.xml",
                "config\\compute\\compute-grid3.xml");
        }

        /// <summary>
        /// Gets the expected compact footers setting.
        /// </summary>
        protected virtual bool CompactFooter
        {
            get { return true; }
        }


        [TestFixtureTearDown]
        public void StopClient()
        {
            Ignition.StopAll(true);
        }

        [TearDown]
        public void AfterTest()
        {
            TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1, _grid2, _grid3);
        }

        /// <summary>
        /// Test that it is possible to get projection from grid.
        /// </summary>
        [Test]
        public void TestProjection()
        {
            IClusterGroup prj = _grid1.GetCluster();

            Assert.NotNull(prj);

            Assert.AreEqual(prj, prj.Ignite);

            // Check that default Compute projection excludes client nodes.
            CollectionAssert.AreEquivalent(prj.ForServers().GetNodes(), prj.GetCompute().ClusterGroup.GetNodes());
        }

        /// <summary>
        /// Test non-existent cache.
        /// </summary>
        [Test]
        public void TestNonExistentCache()
        {
            Assert.Catch(typeof(ArgumentException), () =>
            {
                _grid1.GetCache<int, int>("bad_name");
            });
        }

        /// <summary>
        /// Test node content.
        /// </summary>
        [Test]
        public void TestNodeContent()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            foreach (IClusterNode node in nodes)
            {
                Assert.NotNull(node.Addresses);
                Assert.IsTrue(node.Addresses.Count > 0);
                Assert.Throws<NotSupportedException>(() => node.Addresses.Add("addr"));

                Assert.NotNull(node.GetAttributes());
                Assert.IsTrue(node.GetAttributes().Count > 0);
                Assert.Throws<NotSupportedException>(() => node.GetAttributes().Add("key", "val"));

                Assert.NotNull(node.HostNames);
                Assert.Throws<NotSupportedException>(() => node.HostNames.Add("h"));

                Assert.IsTrue(node.Id != Guid.Empty);

                Assert.IsTrue(node.Order > 0);

                Assert.NotNull(node.GetMetrics());
            }
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestClusterMetrics()
        {
            var cluster = _grid1.GetCluster();

            IClusterMetrics metrics = cluster.GetMetrics();

            Assert.IsNotNull(metrics);

            Assert.AreEqual(cluster.GetNodes().Count, metrics.TotalNodes);

            Thread.Sleep(2000);

            IClusterMetrics newMetrics = cluster.GetMetrics();

            Assert.IsFalse(metrics == newMetrics);
            Assert.IsTrue(metrics.LastUpdateTime < newMetrics.LastUpdateTime);
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestNodeMetrics()
        {
            var node = _grid1.GetCluster().GetNode();

            IClusterMetrics metrics = node.GetMetrics();

            Assert.IsNotNull(metrics);

            Assert.IsTrue(metrics == node.GetMetrics());

            Thread.Sleep(2000);

            IClusterMetrics newMetrics = node.GetMetrics();

            Assert.IsFalse(metrics == newMetrics);
            Assert.IsTrue(metrics.LastUpdateTime < newMetrics.LastUpdateTime);
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestResetMetrics()
        {
            var cluster = _grid1.GetCluster();

            Thread.Sleep(2000);

            var metrics1 = cluster.GetMetrics();

            cluster.ResetMetrics();

            var metrics2 = cluster.GetMetrics();

            Assert.IsNotNull(metrics1);
            Assert.IsNotNull(metrics2);
        }

        /// <summary>
        /// Test node ping.
        /// </summary>
        [Test]
        public void TestPingNode()
        {
            var cluster = _grid1.GetCluster();

            Assert.IsTrue(cluster.GetNodes().Select(node => node.Id).All(cluster.PingNode));
            
            Assert.IsFalse(cluster.PingNode(Guid.NewGuid()));
        }

        /// <summary>
        /// Tests the topology version.
        /// </summary>
        [Test]
        public void TestTopologyVersion()
        {
            var cluster = _grid1.GetCluster();
            
            var topVer = cluster.TopologyVersion;

            Ignition.Stop(_grid3.Name, true);

            Assert.AreEqual(topVer + 1, _grid1.GetCluster().TopologyVersion);

            _grid3 = Ignition.Start(Configuration(GetConfigs().Item3));

            Assert.AreEqual(topVer + 2, _grid1.GetCluster().TopologyVersion);
        }

        /// <summary>
        /// Tests the topology by version.
        /// </summary>
        [Test]
        public void TestTopology()
        {
            var cluster = _grid1.GetCluster();

            Assert.AreEqual(1, cluster.GetTopology(1).Count);

            Assert.AreEqual(null, cluster.GetTopology(int.MaxValue));

            // Check that Nodes and Topology return the same for current version
            var topVer = cluster.TopologyVersion;

            var top = cluster.GetTopology(topVer);

            var nodes = cluster.GetNodes();

            Assert.AreEqual(top.Count, nodes.Count);

            Assert.IsTrue(top.All(nodes.Contains));

            // Stop/start node to advance version and check that history is still correct
            Assert.IsTrue(Ignition.Stop(_grid2.Name, true));

            try
            {
                top = cluster.GetTopology(topVer);

                Assert.AreEqual(top.Count, nodes.Count);

                Assert.IsTrue(top.All(nodes.Contains));
            }
            finally 
            {
                _grid2 = Ignition.Start(Configuration(GetConfigs().Item2));
            }
        }

        /// <summary>
        /// Test nodes in full topology.
        /// </summary>
        [Test]
        public void TestNodes()
        {
            Assert.IsNotNull(_grid1.GetCluster().GetNode());

            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count == 3);

            // Check subsequent call on the same topology.
            nodes = _grid1.GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count == 3);

            Assert.IsTrue(Ignition.Stop(_grid2.Name, true));

            // Check subsequent calls on updating topologies.
            nodes = _grid1.GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count == 2);

            nodes = _grid1.GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count == 2);

            _grid2 = Ignition.Start(Configuration(GetConfigs().Item2));

            nodes = _grid1.GetCluster().GetNodes();

            Assert.IsTrue(nodes.Count == 3);
        }

        /// <summary>
        /// Test "ForNodes" and "ForNodeIds".
        /// </summary>
        [Test]
        public void TestForNodes()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            IClusterNode first = nodes.ElementAt(0);
            IClusterNode second = nodes.ElementAt(1);

            IClusterGroup singleNodePrj = _grid1.GetCluster().ForNodeIds(first.Id);
            Assert.AreEqual(1, singleNodePrj.GetNodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.GetNodes().First().Id);

            singleNodePrj = _grid1.GetCluster().ForNodeIds(new List<Guid> { first.Id });
            Assert.AreEqual(1, singleNodePrj.GetNodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.GetNodes().First().Id);

            singleNodePrj = _grid1.GetCluster().ForNodes(first);
            Assert.AreEqual(1, singleNodePrj.GetNodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.GetNodes().First().Id);

            singleNodePrj = _grid1.GetCluster().ForNodes(new List<IClusterNode> { first });
            Assert.AreEqual(1, singleNodePrj.GetNodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.GetNodes().First().Id);

            IClusterGroup multiNodePrj = _grid1.GetCluster().ForNodeIds(first.Id, second.Id);
            Assert.AreEqual(2, multiNodePrj.GetNodes().Count);
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(first));
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(second));

            multiNodePrj = _grid1.GetCluster().ForNodeIds(new[] {first, second}.Select(x => x.Id));
            Assert.AreEqual(2, multiNodePrj.GetNodes().Count);
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(first));
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(second));

            multiNodePrj = _grid1.GetCluster().ForNodes(first, second);
            Assert.AreEqual(2, multiNodePrj.GetNodes().Count);
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(first));
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(second));

            multiNodePrj = _grid1.GetCluster().ForNodes(new List<IClusterNode> { first, second });
            Assert.AreEqual(2, multiNodePrj.GetNodes().Count);
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(first));
            Assert.IsTrue(multiNodePrj.GetNodes().Contains(second));
        }

        /// <summary>
        /// Test "ForNodes" and "ForNodeIds". Make sure lazy enumerables are enumerated only once.
        /// </summary>
        [Test]
        public void TestForNodesLaziness()
        {
            var nodes = _grid1.GetCluster().GetNodes().Take(2).ToArray();

            var callCount = 0;
            
            Func<IClusterNode, IClusterNode> nodeSelector = node =>
            {
                callCount++;
                return node;
            };

            Func<IClusterNode, Guid> idSelector = node =>
            {
                callCount++;
                return node.Id;
            };

            var projection = _grid1.GetCluster().ForNodes(nodes.Select(nodeSelector));
            Assert.AreEqual(2, projection.GetNodes().Count);
            Assert.AreEqual(2, callCount);
            
            projection = _grid1.GetCluster().ForNodeIds(nodes.Select(idSelector));
            Assert.AreEqual(2, projection.GetNodes().Count);
            Assert.AreEqual(4, callCount);
        }

        /// <summary>
        /// Test for local node projection.
        /// </summary>
        [Test]
        public void TestForLocal()
        {
            IClusterGroup prj = _grid1.GetCluster().ForLocal();

            Assert.AreEqual(1, prj.GetNodes().Count);
            Assert.AreEqual(_grid1.GetCluster().GetLocalNode(), prj.GetNodes().First());
        }

        /// <summary>
        /// Test for remote nodes projection.
        /// </summary>
        [Test]
        public void TestForRemotes()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            IClusterGroup prj = _grid1.GetCluster().ForRemotes();

            Assert.AreEqual(2, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prj.GetNodes().ElementAt(1)));
        }

        /// <summary>
        /// Test for daemon nodes projection.
        /// </summary>
        [Test]
        public void TestForDaemons()
        {
            Assert.AreEqual(0, _grid1.GetCluster().ForDaemons().GetNodes().Count);

            using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    SpringConfigUrl = GetConfigs().Item1,
                    IgniteInstanceName = "daemonGrid",
                    IsDaemon = true
                })
            )
            {
                var prj = _grid1.GetCluster().ForDaemons();

                Assert.AreEqual(1, prj.GetNodes().Count);
                Assert.AreEqual(ignite.GetCluster().GetLocalNode().Id, prj.GetNode().Id);

                Assert.IsTrue(prj.GetNode().IsDaemon);
                Assert.IsTrue(ignite.GetCluster().GetLocalNode().IsDaemon);
            }
        }

        /// <summary>
        /// Test for host nodes projection.
        /// </summary>
        [Test]
        public void TestForHost()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            IClusterGroup prj = _grid1.GetCluster().ForHost(nodes.First());

            Assert.AreEqual(3, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prj.GetNodes().ElementAt(1)));
            Assert.IsTrue(nodes.Contains(prj.GetNodes().ElementAt(2)));
        }

        /// <summary>
        /// Test for oldest, youngest and random projections.
        /// </summary>
        [Test]
        public void TestForOldestYoungestRandom()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            IClusterGroup prj = _grid1.GetCluster().ForYoungest();
            Assert.AreEqual(1, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNode()));

            prj = _grid1.GetCluster().ForOldest();
            Assert.AreEqual(1, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNode()));

            prj = _grid1.GetCluster().ForRandom();
            Assert.AreEqual(1, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNode()));
        }

        /// <summary>
        /// Tests ForServers projection.
        /// </summary>
        [Test]
        public void TestForServers()
        {
            var cluster = _grid1.GetCluster();

            var servers = cluster.ForServers().GetNodes();
            Assert.AreEqual(2, servers.Count);
            Assert.IsTrue(servers.All(x => !x.IsClient));

            var serverAndClient =
                cluster.ForNodeIds(new[] { _grid2, _grid3 }.Select(x => x.GetCluster().GetLocalNode().Id));
            Assert.AreEqual(1, serverAndClient.ForServers().GetNodes().Count);

            var client = cluster.ForNodeIds(new[] { _grid3 }.Select(x => x.GetCluster().GetLocalNode().Id));
            Assert.AreEqual(0, client.ForServers().GetNodes().Count);
        }

        /// <summary>
        /// Test for attribute projection.
        /// </summary>
        [Test]
        public void TestForAttribute()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            IClusterGroup prj = _grid1.GetCluster().ForAttribute("my_attr", "value1");
            Assert.AreEqual(1, prj.GetNodes().Count);
            Assert.IsTrue(nodes.Contains(prj.GetNode()));
            Assert.AreEqual("value1", prj.GetNodes().First().GetAttribute<string>("my_attr"));
        }
        
        /// <summary>
        /// Test for cache/data/client projections.
        /// </summary>
        [Test]
        public void TestForCacheNodes()
        {
            ICollection<IClusterNode> nodes = _grid1.GetCluster().GetNodes();

            // Cache nodes.
            IClusterGroup prjCache = _grid1.GetCluster().ForCacheNodes("cache1");

            Assert.AreEqual(2, prjCache.GetNodes().Count);

            Assert.IsTrue(nodes.Contains(prjCache.GetNodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prjCache.GetNodes().ElementAt(1)));
            
            // Data nodes.
            IClusterGroup prjData = _grid1.GetCluster().ForDataNodes("cache1");

            Assert.AreEqual(2, prjData.GetNodes().Count);

            Assert.IsTrue(prjCache.GetNodes().Contains(prjData.GetNodes().ElementAt(0)));
            Assert.IsTrue(prjCache.GetNodes().Contains(prjData.GetNodes().ElementAt(1)));

            // Client nodes.
            IClusterGroup prjClient = _grid1.GetCluster().ForClientNodes("cache1");

            Assert.AreEqual(0, prjClient.GetNodes().Count);
        }
        
        /// <summary>
        /// Test for cache predicate.
        /// </summary>
        [Test]
        public void TestForPredicate()
        {
            IClusterGroup prj1 = _grid1.GetCluster().ForPredicate(new NotAttributePredicate("value1").Apply);
            Assert.AreEqual(2, prj1.GetNodes().Count);

            IClusterGroup prj2 = prj1.ForPredicate(new NotAttributePredicate("value2").Apply);
            Assert.AreEqual(1, prj2.GetNodes().Count);

            string val;

            prj2.GetNodes().First().TryGetAttribute("my_attr", out val);

            Assert.IsTrue(val == null || (!val.Equals("value1") && !val.Equals("value2")));
        }

        /// <summary>
        /// Attribute predicate.
        /// </summary>
        private class NotAttributePredicate
        {
            /** Required attribute value. */
            private readonly string _attrVal;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="attrVal">Required attribute value.</param>
            public NotAttributePredicate(string attrVal)
            {
                _attrVal = attrVal;
            }

            /** <inhreitDoc /> */
            public bool Apply(IClusterNode node)
            {
                string val;

                node.TryGetAttribute("my_attr", out val);

                return val == null || !val.Equals(_attrVal);
            }
        }

        /// <summary>
        /// Test echo with decimals.
        /// </summary>
        [Test]
        public void TestEchoDecimal()
        {
            decimal val;

            Assert.AreEqual(val = decimal.Zero, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("65536"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("4294967296"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("281474976710656"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("18446744073709551616"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.MaxValue, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MaxValue - 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue + 1, _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("11,12"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-11,12"), _grid1.GetCompute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            // Test echo with overflow.
            var ex = Assert.Throws<BinaryObjectException>(() => _grid1.GetCompute()
                .ExecuteJavaTask<object>(DecimalTask, new object[] {null, decimal.MaxValue.ToString() + 1}));

            Assert.AreEqual("Decimal magnitude overflow (must be less than 96 bits): 104", ex.Message);

            // Negative scale. 1E+1 parses to "1 scale -1" on Java side.
            ex = Assert.Throws<BinaryObjectException>(() => _grid1.GetCompute()
                .ExecuteJavaTask<object>(DecimalTask, new object[] {null, "1E+1"}));

            Assert.AreEqual("Decimal value scale overflow (must be between 0 and 28): -1", ex.Message);
        }

        /// <summary>
        /// Test echo task returning null.
        /// </summary>
        [Test]
        public void TestEchoTaskNull()
        {
            Assert.IsNull(_grid1.GetCompute().ExecuteJavaTask<object>(EchoTask, EchoTypeNull));
        }

        /// <summary>
        /// Test echo task returning various primitives.
        /// </summary>
        [Test]
        public void TestEchoTaskPrimitives()
        {
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<byte>(EchoTask, EchoTypeByte));
            Assert.AreEqual(true, _grid1.GetCompute().ExecuteJavaTask<bool>(EchoTask, EchoTypeBool));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<short>(EchoTask, EchoTypeShort));
            Assert.AreEqual((char)1, _grid1.GetCompute().ExecuteJavaTask<char>(EchoTask, EchoTypeChar));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<int>(EchoTask, EchoTypeInt));
            Assert.AreEqual(1, _grid1.GetCompute().ExecuteJavaTask<long>(EchoTask, EchoTypeLong));
            Assert.AreEqual((float)1, _grid1.GetCompute().ExecuteJavaTask<float>(EchoTask, EchoTypeFloat));
            Assert.AreEqual((double)1, _grid1.GetCompute().ExecuteJavaTask<double>(EchoTask, EchoTypeDouble));
        }

        /// <summary>
        /// Test echo task returning compound types.
        /// </summary>
        [Test]
        public void TestEchoTaskCompound()
        {
            int[] res1 = _grid1.GetCompute().ExecuteJavaTask<int[]>(EchoTask, EchoTypeArray);

            Assert.AreEqual(1, res1.Length);
            Assert.AreEqual(1, res1[0]);

            var res2 = _grid1.GetCompute().ExecuteJavaTask<IList>(EchoTask, EchoTypeCollection);

            Assert.AreEqual(1, res2.Count);
            Assert.AreEqual(1, res2[0]);

            var res3 = _grid1.GetCompute().ExecuteJavaTask<IDictionary>(EchoTask, EchoTypeMap);

            Assert.AreEqual(1, res3.Count);
            Assert.AreEqual(1, res3[1]);
        }

        /// <summary>
        /// Test echo task returning binary object.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizable()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeBinarizable>(EchoTask, EchoTypeBinarizable);

            Assert.AreEqual(1, res.Field);
        }

        /// <summary>
        /// Test echo task returning binary object with no corresponding class definition.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizableNoClass()
        {
            ICompute compute = _grid1.GetCompute();

            compute.WithKeepBinary();

            IBinaryObject res = compute.ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinarizableJava);

            Assert.AreEqual(1, res.GetField<int>("field"));

            // This call must fail because "keepBinary" flag is reset.
            var ex = Assert.Throws<BinaryObjectException>(() =>
            {
                compute.ExecuteJavaTask<IBinaryObject>(EchoTask, EchoTypeBinarizableJava);
            });

            Assert.AreEqual("Unknown pair [platformId=1, typeId=2009791293]", ex.Message);
        }

        /// <summary>
        /// Tests the echo task returning object array.
        /// </summary>
        [Test]
        public void TestEchoTaskObjectArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<string[]>(EchoTask, EchoTypeObjArray);
            
            Assert.AreEqual(new[] {"foo", "bar", "baz"}, res);
        }

        /// <summary>
        /// Tests the echo task returning binary array.
        /// </summary>
        [Test]
        public void TestEchoTaskBinarizableArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<object[]>(EchoTask, EchoTypeBinarizableArray);
            
            Assert.AreEqual(3, res.Length);

            for (var i = 0; i < res.Length; i++)
                Assert.AreEqual(i + 1, ((PlatformComputeBinarizable) res[i]).Field);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnum()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnum);

            Assert.AreEqual(PlatformComputeEnum.Bar, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumFromCache()
        {
            var cache = _grid1.GetCache<int, PlatformComputeEnum>(DefaultCacheName);

            foreach (PlatformComputeEnum val in Enum.GetValues(typeof(PlatformComputeEnum)))
            {
                cache[EchoTypeEnumFromCache] = val;

                var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnumFromCache);

                Assert.AreEqual(val, res);
            }
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArray()
        {
            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum[]>(EchoTask, EchoTypeEnumArray);

            Assert.AreEqual(new[]
            {
                PlatformComputeEnum.Bar,
                PlatformComputeEnum.Baz,
                PlatformComputeEnum.Foo
            }, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArrayFromCache()
        {
            var cache = _grid1.GetCache<int, PlatformComputeEnum[]>(DefaultCacheName);

            foreach (var val in new[]
            {
                new[] {PlatformComputeEnum.Bar, PlatformComputeEnum.Baz, PlatformComputeEnum.Foo },
                new[] {PlatformComputeEnum.Foo, PlatformComputeEnum.Baz},
                new[] {PlatformComputeEnum.Bar}
            })
            {
                cache[EchoTypeEnumArrayFromCache] = val;

                var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum[]>(
                    EchoTask, EchoTypeEnumArrayFromCache);

                Assert.AreEqual(val, res);
            }
        }

        /// <summary>
        /// Tests the echo task reading enum from a binary object field.
        /// Ensures that Java can understand enums written by .NET.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumField()
        {
            var enumVal = PlatformComputeEnum.Baz;

            _grid1.GetCache<int, InteropComputeEnumFieldTest>(DefaultCacheName)
                .Put(EchoTypeEnumField, new InteropComputeEnumFieldTest {InteropEnum = enumVal});

            var res = _grid1.GetCompute().ExecuteJavaTask<PlatformComputeEnum>(EchoTask, EchoTypeEnumField);

            var enumMeta = _grid1.GetBinary().GetBinaryType(typeof (PlatformComputeEnum));

            Assert.IsTrue(enumMeta.IsEnum);
            Assert.AreEqual(enumMeta.TypeName, typeof(PlatformComputeEnum).Name);
            Assert.AreEqual(0, enumMeta.Fields.Count);

            Assert.AreEqual(enumVal, res);
        }

        /// <summary>
        /// Tests that IgniteGuid in .NET maps to IgniteUuid in Java.
        /// </summary>
        [Test]
        public void TestEchoTaskIgniteUuid()
        {
            var guid = Guid.NewGuid();

            _grid1.GetCache<int, object>(DefaultCacheName)[EchoTypeIgniteUuid] = new IgniteGuid(guid, 25);

            var res = _grid1.GetCompute().ExecuteJavaTask<IgniteGuid>(EchoTask, EchoTypeIgniteUuid);

            Assert.AreEqual(guid, res.GlobalId);
            Assert.AreEqual(25, res.LocalId);
        }

        /// <summary>
        /// Test for binary argument in Java.
        /// </summary>
        [Test]
        public void TestBinarizableArgTask()
        {
            ICompute compute = _grid1.GetCompute();

            compute.WithKeepBinary();

            PlatformComputeNetBinarizable arg = new PlatformComputeNetBinarizable {Field = 100};

            int res = compute.ExecuteJavaTask<int>(BinaryArgTask, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Test running broadcast task.
        /// </summary>
        [Test]
        public void TestBroadcastTask([Values(false, true)] bool isAsync)
        {
            var execTask =
                isAsync
                    ? (Func<ICompute, List<Guid>>) (
                        c => c.ExecuteJavaTaskAsync<ICollection>(BroadcastTask, null).Result.OfType<Guid>().ToList())
                    : c => c.ExecuteJavaTask<ICollection>(BroadcastTask, null).OfType<Guid>().ToList();

            var res = execTask(_grid1.GetCompute());

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);

            var prj = _grid1.GetCluster().ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.GetNodes().Count);

            var filteredRes = execTask(prj.GetCompute());

            Assert.AreEqual(2, filteredRes.Count);
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(0)));
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(1)));
        }

        /// <summary>
        /// Tests the action broadcast.
        /// </summary>
        [Test]
        public void TestBroadcastAction()
        {
            var id = Guid.NewGuid();
            _grid1.GetCompute().Broadcast(new ComputeAction(id));
            Assert.AreEqual(2, ComputeAction.InvokeCount(id));

            id = Guid.NewGuid();
            _grid1.GetCompute().BroadcastAsync(new ComputeAction(id)).Wait();
            Assert.AreEqual(2, ComputeAction.InvokeCount(id));
        }

        /// <summary>
        /// Tests single action run.
        /// </summary>
        [Test]
        public void TestRunAction()
        {
            var id = Guid.NewGuid();
            _grid1.GetCompute().Run(new ComputeAction(id));
            Assert.AreEqual(1, ComputeAction.InvokeCount(id));

            id = Guid.NewGuid();
            _grid1.GetCompute().RunAsync(new ComputeAction(id)).Wait();
            Assert.AreEqual(1, ComputeAction.InvokeCount(id));
        }

        /// <summary>
        /// Tests single action run.
        /// </summary>
        [Test]
        public void TestRunActionAsyncCancel()
        {
            using (var cts = new CancellationTokenSource())
            {
                // Cancel while executing
                var task = _grid1.GetCompute().RunAsync(new ComputeAction(), cts.Token);
                cts.Cancel();
                Assert.IsTrue(task.IsCanceled);

                // Use cancelled token
                task = _grid1.GetCompute().RunAsync(new ComputeAction(), cts.Token);
                Assert.IsTrue(task.IsCanceled);
            }
        }

        /// <summary>
        /// Tests multiple actions run.
        /// </summary>
        [Test]
        public void TestRunActions()
        {
            var id = Guid.NewGuid();
            _grid1.GetCompute().Run(Enumerable.Range(0, 10).Select(x => new ComputeAction(id)));
            Assert.AreEqual(10, ComputeAction.InvokeCount(id));

            var id2 = Guid.NewGuid();
            _grid1.GetCompute().RunAsync(Enumerable.Range(0, 10).Select(x => new ComputeAction(id2))).Wait();
            Assert.AreEqual(10, ComputeAction.InvokeCount(id2));
        }

        /// <summary>
        /// Tests affinity run.
        /// </summary>
        [Test]
        public void TestAffinityRun()
        {
            const string cacheName = DefaultCacheName;

            // Test keys for non-client nodes
            var nodes = new[] {_grid1, _grid2}.Select(x => x.GetCluster().GetLocalNode());

            var aff = _grid1.GetAffinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = TestUtils.GetPrimaryKey(_grid1, cacheName, node);

                var affinityKey = aff.GetAffinityKey<int, int>(primaryKey);

                _grid1.GetCompute().AffinityRun(cacheName, affinityKey, new ComputeAction());
                Assert.AreEqual(node.Id, ComputeAction.LastNodeId);

                _grid1.GetCompute().AffinityRunAsync(cacheName, affinityKey, new ComputeAction()).Wait();
                Assert.AreEqual(node.Id, ComputeAction.LastNodeId);
            }
        }

        /// <summary>
        /// Tests affinity call.
        /// </summary>
        [Test]
        public void TestAffinityCall()
        {
            const string cacheName = DefaultCacheName;

            // Test keys for non-client nodes
            var nodes = new[] { _grid1, _grid2 }.Select(x => x.GetCluster().GetLocalNode());

            var aff = _grid1.GetAffinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = TestUtils.GetPrimaryKey(_grid1, cacheName, node);

                var affinityKey = aff.GetAffinityKey<int, int>(primaryKey);

                var result = _grid1.GetCompute().AffinityCall(cacheName, affinityKey, new ComputeFunc());

                Assert.AreEqual(result, ComputeFunc.InvokeCount);

                Assert.AreEqual(node.Id, ComputeFunc.LastNodeId);

                // Async.
                ComputeFunc.InvokeCount = 0;

                result = _grid1.GetCompute().AffinityCallAsync(cacheName, affinityKey, new ComputeFunc()).Result;

                Assert.AreEqual(result, ComputeFunc.InvokeCount);

                Assert.AreEqual(node.Id, ComputeFunc.LastNodeId);
            }
        }

        /// <summary>
        /// Test "withNoFailover" feature.
        /// </summary>
        [Test]
        public void TestWithNoFailover()
        {
            var res = _grid1.GetCompute().WithNoFailover().ExecuteJavaTask<ICollection>(BroadcastTask, null)
                .OfType<Guid>().ToList();

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);
        }

        /// <summary>
        /// Test "withTimeout" feature.
        /// </summary>
        [Test]
        public void TestWithTimeout()
        {
            var res = _grid1.GetCompute().WithTimeout(1000).ExecuteJavaTask<ICollection>(BroadcastTask, null)
                .OfType<Guid>().ToList();

            Assert.AreEqual(2, res.Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(0)).GetNodes().Count);
            Assert.AreEqual(1, _grid1.GetCluster().ForNodeIds(res.ElementAt(1)).GetNodes().Count);
        }

        /// <summary>
        /// Test simple dotNet task execution.
        /// </summary>
        [Test]
        public void TestNetTaskSimple()
        {
            Assert.AreEqual(2, _grid1.GetCompute()
                .Execute<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                typeof(NetSimpleTask), new NetSimpleJobArgument(1)).Res);

            Assert.AreEqual(2, _grid1.GetCompute()
                .ExecuteAsync<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                typeof(NetSimpleTask), new NetSimpleJobArgument(1)).Result.Res);

            Assert.AreEqual(4, _grid1.GetCompute().Execute(new NetSimpleTask(), new NetSimpleJobArgument(2)).Res);

            Assert.AreEqual(6, _grid1.GetCompute().ExecuteAsync(new NetSimpleTask(), new NetSimpleJobArgument(3))
                .Result.Res);
        }

        /// <summary>
        /// Tests the exceptions.
        /// </summary>
        [Test]
        public void TestExceptions()
        {
            Assert.Throws<BinaryObjectException>(() => _grid1.GetCompute().Broadcast(new InvalidComputeAction()));

            Assert.Throws<BinaryObjectException>(
                () => _grid1.GetCompute().Execute<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                    typeof (NetSimpleTask), new NetSimpleJobArgument(-1)));

            // Local.
            var ex = Assert.Throws<IgniteException>(() =>
                _grid1.GetCluster().ForLocal().GetCompute().Broadcast(new ExceptionalComputeAction()));

            Assert.AreEqual("Compute job has failed on local node, examine InnerException for details.", ex.Message);
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual(ExceptionalComputeAction.ErrorText, ex.InnerException.Message);

            // Remote.
            ex = Assert.Throws<IgniteException>(() =>
                _grid1.GetCluster().ForRemotes().GetCompute().Broadcast(new ExceptionalComputeAction()));

            Assert.AreEqual("Compute job has failed on remote node, examine InnerException for details.", ex.Message);
            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual(ExceptionalComputeAction.ErrorText, ex.InnerException.Message);
        }

        /// <summary>
        /// Tests the footer setting.
        /// </summary>
        [Test]
        public void TestFooterSetting()
        {
            Assert.AreEqual(CompactFooter, ((Ignite)_grid1).Marshaller.CompactFooter);

            foreach (var g in new[] {_grid1, _grid2, _grid3})
                Assert.AreEqual(CompactFooter, g.GetConfiguration().BinaryConfiguration.CompactFooter);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="path">XML config path.</param>
        private static IgniteConfiguration Configuration(string path)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(PlatformComputeBinarizable)),
                        new BinaryTypeConfiguration(typeof(PlatformComputeNetBinarizable)),
                        new BinaryTypeConfiguration(JavaBinaryCls),
                        new BinaryTypeConfiguration(typeof(PlatformComputeEnum)),
                        new BinaryTypeConfiguration(typeof(InteropComputeEnumFieldTest))
                    },
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                },
                SpringConfigUrl = path
            };
        }
    }

    class PlatformComputeBinarizable
    {
        public int Field
        {
            get;
            set;
        }
    }

    class PlatformComputeNetBinarizable : PlatformComputeBinarizable
    {

    }

    [Serializable]
    class NetSimpleTask : IComputeTask<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>
    {
        /** <inheritDoc /> */

        public IDictionary<IComputeJob<NetSimpleJobResult>, IClusterNode> Map(IList<IClusterNode> subgrid,
            NetSimpleJobArgument arg)
        {
            var jobs = new Dictionary<IComputeJob<NetSimpleJobResult>, IClusterNode>();

            for (int i = 0; i < subgrid.Count; i++)
            {
                var job = arg.Arg > 0 ? new NetSimpleJob {Arg = arg} : new InvalidNetSimpleJob();

                jobs[job] = subgrid[i];
            }

            return jobs;
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy OnResult(IComputeJobResult<NetSimpleJobResult> res,
            IList<IComputeJobResult<NetSimpleJobResult>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        /** <inheritDoc /> */
        public NetSimpleTaskResult Reduce(IList<IComputeJobResult<NetSimpleJobResult>> results)
        {
            return new NetSimpleTaskResult(results.Sum(res => res.Data.Res));
        }
    }

    [Serializable]
    class NetSimpleJob : IComputeJob<NetSimpleJobResult>
    {
        public NetSimpleJobArgument Arg;

        /** <inheritDoc /> */
        public NetSimpleJobResult Execute()
        {
            return new NetSimpleJobResult(Arg.Arg);
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            // No-op.
        }
    }

    class InvalidNetSimpleJob : NetSimpleJob, IBinarizable
    {
        public void WriteBinary(IBinaryWriter writer)
        {
            throw new BinaryObjectException("Expected");
        }

        public void ReadBinary(IBinaryReader reader)
        {
            throw new BinaryObjectException("Expected");
        }
    }

    [Serializable]
    class NetSimpleJobArgument
    {
        public int Arg;

        public NetSimpleJobArgument(int arg)
        {
            Arg = arg;
        }
    }

    [Serializable]
    class NetSimpleTaskResult
    {
        public int Res;

        public NetSimpleTaskResult(int res)
        {
            Res = res;
        }
    }

    [Serializable]
    class NetSimpleJobResult
    {
        public int Res;

        public NetSimpleJobResult(int res)
        {
            Res = res;
        }
    }

    [Serializable]
    class ComputeAction : IComputeAction
    {
        [InstanceResource]
        #pragma warning disable 649
        private IIgnite _grid;

        public static ConcurrentBag<Guid> Invokes = new ConcurrentBag<Guid>();

        public static Guid LastNodeId;

        public Guid Id { get; set; }

        public ComputeAction()
        {
            // No-op.
        }

        public ComputeAction(Guid id)
        {
            Id = id;
        }

        public void Invoke()
        {
            Thread.Sleep(10);
            Invokes.Add(Id);
            LastNodeId = _grid.GetCluster().GetLocalNode().Id;
        }

        public static int InvokeCount(Guid id)
        {
            return Invokes.Count(x => x == id);
        }
    }

    class InvalidComputeAction : ComputeAction, IBinarizable
    {
        public void WriteBinary(IBinaryWriter writer)
        {
            throw new BinaryObjectException("Expected");
        }

        public void ReadBinary(IBinaryReader reader)
        {
            throw new BinaryObjectException("Expected");
        }
    }

    class ExceptionalComputeAction : IComputeAction
    {
        public const string ErrorText = "Expected user exception";

        public void Invoke()
        {
            throw new OverflowException(ErrorText);
        }
    }

    interface IUserInterface<out T>
    {
        T Invoke();
    }

    interface INestedComputeFunc : IComputeFunc<int>
    {
        
    }

    [Serializable]
    class ComputeFunc : INestedComputeFunc, IUserInterface<int>
    {
        [InstanceResource]
        private IIgnite _grid;

        public static int InvokeCount;

        public static Guid LastNodeId;

        int IComputeFunc<int>.Invoke()
        {
            Thread.Sleep(10);
            InvokeCount++;
            LastNodeId = _grid.GetCluster().GetLocalNode().Id;
            return InvokeCount;
        }

        int IUserInterface<int>.Invoke()
        {
            // Same signature as IComputeFunc<int>, but from different interface
            throw new Exception("Invalid method");
        }

        public int Invoke()
        {
            // Same signature as IComputeFunc<int>, but due to explicit interface implementation this is a wrong method
            throw new Exception("Invalid method");
        }
    }

    public enum PlatformComputeEnum : ushort
    {
        Foo,
        Bar,
        Baz
    }

    public class InteropComputeEnumFieldTest
    {
        public PlatformComputeEnum InteropEnum { get; set; }
    }
}
