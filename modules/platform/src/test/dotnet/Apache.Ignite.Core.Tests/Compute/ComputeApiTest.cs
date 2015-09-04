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
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for compute.
    /// </summary>
    public class ComputeApiTest
    {
        /** Echo task name. */
        private const string EchoTask = "org.apache.ignite.platform.PlatformComputeEchoTask";

        /** Portable argument task name. */
        private const string PortableArgTask = "org.apache.ignite.platform.PlatformComputePortableArgTask";

        /** Broadcast task name. */
        private const string BroadcastTask = "org.apache.ignite.platform.PlatformComputeBroadcastTask";

        /** Broadcast task name. */
        private const string DecimalTask = "org.apache.ignite.platform.PlatformComputeDecimalTask";

        /** Java portable class name. */
        private const string JavaPortableCls = "GridInteropComputeJavaPortable";

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

        /** Echo type: portable. */
        private const int EchoTypePortable = 12;

        /** Echo type: portable (Java only). */
        private const int EchoTypePortableJava = 13;

        /** Type: object array. */
        private const int EchoTypeObjArray = 14;

        /** Type: portable object array. */
        private const int EchoTypePortableArray = 15;

        /** Type: enum. */
        private const int EchoTypeEnum = 16;

        /** Type: enum array. */
        private const int EchoTypeEnumArray = 17;

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
            //TestUtils.JVM_DEBUG = true;
            TestUtils.KillProcesses();

            _grid1 = Ignition.Start(Configuration("config\\compute\\compute-grid1.xml"));
            _grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            _grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));
        }

        [TestFixtureTearDown]
        public void StopClient()
        {
            if (_grid1 != null)
                Ignition.Stop(_grid1.Name, true);

            if (_grid2 != null)
                Ignition.Stop(_grid2.Name, true);

            if (_grid3 != null)
                Ignition.Stop(_grid3.Name, true);
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
            IClusterGroup prj = _grid1.Cluster;

            Assert.NotNull(prj);

            Assert.IsTrue(prj == prj.Ignite);
        }

        /// <summary>
        /// Test getting cache with default (null) name.
        /// </summary>
        [Test]
        public void TestCacheDefaultName()
        {
            var cache = _grid1.Cache<int, int>(null);

            Assert.IsNotNull(cache);

            cache.GetAndPut(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }

        /// <summary>
        /// Test non-existent cache.
        /// </summary>
        [Test]
        public void TestNonExistentCache()
        {
            Assert.Catch(typeof(ArgumentException), () =>
            {
                _grid1.Cache<int, int>("bad_name");
            });
        }

        /// <summary>
        /// Test node content.
        /// </summary>
        [Test]
        public void TestNodeContent()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            foreach (IClusterNode node in nodes)
            {
                Assert.NotNull(node.Addresses);
                Assert.IsTrue(node.Addresses.Count > 0);
                Assert.Throws<NotSupportedException>(() => node.Addresses.Add("addr"));

                Assert.NotNull(node.Attributes());
                Assert.IsTrue(node.Attributes().Count > 0);
                Assert.Throws<NotSupportedException>(() => node.Attributes().Add("key", "val"));

                Assert.NotNull(node.HostNames);
                Assert.Throws<NotSupportedException>(() => node.HostNames.Add("h"));

                Assert.IsTrue(node.Id != Guid.Empty);

                Assert.IsTrue(node.Order > 0);

                Assert.NotNull(node.Metrics());
            }
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestClusterMetrics()
        {
            var cluster = _grid1.Cluster;

            IClusterMetrics metrics = cluster.Metrics();

            Assert.IsNotNull(metrics);

            Assert.AreEqual(cluster.Nodes().Count, metrics.TotalNodes);

            Thread.Sleep(2000);

            IClusterMetrics newMetrics = cluster.Metrics();

            Assert.IsFalse(metrics == newMetrics);
            Assert.IsTrue(metrics.LastUpdateTime < newMetrics.LastUpdateTime);
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestNodeMetrics()
        {
            var node = _grid1.Cluster.Node();

            IClusterMetrics metrics = node.Metrics();

            Assert.IsNotNull(metrics);

            Assert.IsTrue(metrics == node.Metrics());

            Thread.Sleep(2000);

            IClusterMetrics newMetrics = node.Metrics();

            Assert.IsFalse(metrics == newMetrics);
            Assert.IsTrue(metrics.LastUpdateTime < newMetrics.LastUpdateTime);
        }

        /// <summary>
        /// Test cluster metrics.
        /// </summary>
        [Test]
        public void TestResetMetrics()
        {
            var cluster = _grid1.Cluster;

            Thread.Sleep(2000);

            var metrics1 = cluster.Metrics();

            cluster.ResetMetrics();

            var metrics2 = cluster.Metrics();

            Assert.IsNotNull(metrics1);
            Assert.IsNotNull(metrics2);
        }

        /// <summary>
        /// Test node ping.
        /// </summary>
        [Test]
        public void TestPingNode()
        {
            var cluster = _grid1.Cluster;

            Assert.IsTrue(cluster.Nodes().Select(node => node.Id).All(cluster.PingNode));
            
            Assert.IsFalse(cluster.PingNode(Guid.NewGuid()));
        }

        /// <summary>
        /// Tests the topology version.
        /// </summary>
        [Test]
        public void TestTopologyVersion()
        {
            var cluster = _grid1.Cluster;
            
            var topVer = cluster.TopologyVersion;

            Ignition.Stop(_grid3.Name, true);

            Assert.AreEqual(topVer + 1, _grid1.Cluster.TopologyVersion);

            _grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));

            Assert.AreEqual(topVer + 2, _grid1.Cluster.TopologyVersion);
        }

        /// <summary>
        /// Tests the topology by version.
        /// </summary>
        [Test]
        public void TestTopology()
        {
            var cluster = _grid1.Cluster;

            Assert.AreEqual(1, cluster.Topology(1).Count);

            Assert.AreEqual(null, cluster.Topology(int.MaxValue));

            // Check that Nodes and Topology return the same for current version
            var topVer = cluster.TopologyVersion;

            var top = cluster.Topology(topVer);

            var nodes = cluster.Nodes();

            Assert.AreEqual(top.Count, nodes.Count);

            Assert.IsTrue(top.All(nodes.Contains));

            // Stop/start node to advance version and check that history is still correct
            Assert.IsTrue(Ignition.Stop(_grid2.Name, true));

            try
            {
                top = cluster.Topology(topVer);

                Assert.AreEqual(top.Count, nodes.Count);

                Assert.IsTrue(top.All(nodes.Contains));
            }
            finally 
            {
                _grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            }
        }

        /// <summary>
        /// Test nodes in full topology.
        /// </summary>
        [Test]
        public void TestNodes()
        {
            Assert.IsNotNull(_grid1.Cluster.Node());

            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);

            // Check subsequent call on the same topology.
            nodes = _grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);

            Assert.IsTrue(Ignition.Stop(_grid2.Name, true));

            // Check subsequent calls on updating topologies.
            nodes = _grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 2);

            nodes = _grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 2);

            _grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));

            nodes = _grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);
        }

        /// <summary>
        /// Test "ForNodes" and "ForNodeIds".
        /// </summary>
        [Test]
        public void TestForNodes()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            IClusterNode first = nodes.ElementAt(0);
            IClusterNode second = nodes.ElementAt(1);

            IClusterGroup singleNodePrj = _grid1.Cluster.ForNodeIds(first.Id);
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = _grid1.Cluster.ForNodeIds(new List<Guid> { first.Id });
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = _grid1.Cluster.ForNodes(first);
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = _grid1.Cluster.ForNodes(new List<IClusterNode> { first });
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            IClusterGroup multiNodePrj = _grid1.Cluster.ForNodeIds(first.Id, second.Id);
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = _grid1.Cluster.ForNodeIds(new[] {first, second}.Select(x => x.Id));
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = _grid1.Cluster.ForNodes(first, second);
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = _grid1.Cluster.ForNodes(new List<IClusterNode> { first, second });
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));
        }

        /// <summary>
        /// Test "ForNodes" and "ForNodeIds". Make sure lazy enumerables are enumerated only once.
        /// </summary>
        [Test]
        public void TestForNodesLaziness()
        {
            var nodes = _grid1.Cluster.Nodes().Take(2).ToArray();

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

            var projection = _grid1.Cluster.ForNodes(nodes.Select(nodeSelector));
            Assert.AreEqual(2, projection.Nodes().Count);
            Assert.AreEqual(2, callCount);
            
            projection = _grid1.Cluster.ForNodeIds(nodes.Select(idSelector));
            Assert.AreEqual(2, projection.Nodes().Count);
            Assert.AreEqual(4, callCount);
        }

        /// <summary>
        /// Test for local node projection.
        /// </summary>
        [Test]
        public void TestForLocal()
        {
            IClusterGroup prj = _grid1.Cluster.ForLocal();

            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.AreEqual(_grid1.Cluster.LocalNode, prj.Nodes().First());
        }

        /// <summary>
        /// Test for remote nodes projection.
        /// </summary>
        [Test]
        public void TestForRemotes()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            IClusterGroup prj = _grid1.Cluster.ForRemotes();

            Assert.AreEqual(2, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Nodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prj.Nodes().ElementAt(1)));
        }

        /// <summary>
        /// Test for host nodes projection.
        /// </summary>
        [Test]
        public void TestForHost()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            IClusterGroup prj = _grid1.Cluster.ForHost(nodes.First());

            Assert.AreEqual(3, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Nodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prj.Nodes().ElementAt(1)));
            Assert.IsTrue(nodes.Contains(prj.Nodes().ElementAt(2)));
        }

        /// <summary>
        /// Test for oldest, youngest and random projections.
        /// </summary>
        [Test]
        public void TestForOldestYoungestRandom()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            IClusterGroup prj = _grid1.Cluster.ForYoungest();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));

            prj = _grid1.Cluster.ForOldest();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));

            prj = _grid1.Cluster.ForRandom();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));
        }

        /// <summary>
        /// Test for attribute projection.
        /// </summary>
        [Test]
        public void TestForAttribute()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            IClusterGroup prj = _grid1.Cluster.ForAttribute("my_attr", "value1");
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));
            Assert.AreEqual("value1", prj.Nodes().First().Attribute<string>("my_attr"));
        }
        
        /// <summary>
        /// Test for cache/data/client projections.
        /// </summary>
        [Test]
        public void TestForCacheNodes()
        {
            ICollection<IClusterNode> nodes = _grid1.Cluster.Nodes();

            // Cache nodes.
            IClusterGroup prjCache = _grid1.Cluster.ForCacheNodes("cache1");

            Assert.AreEqual(2, prjCache.Nodes().Count);

            Assert.IsTrue(nodes.Contains(prjCache.Nodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prjCache.Nodes().ElementAt(1)));
            
            // Data nodes.
            IClusterGroup prjData = _grid1.Cluster.ForDataNodes("cache1");

            Assert.AreEqual(2, prjData.Nodes().Count);

            Assert.IsTrue(prjCache.Nodes().Contains(prjData.Nodes().ElementAt(0)));
            Assert.IsTrue(prjCache.Nodes().Contains(prjData.Nodes().ElementAt(1)));

            // Client nodes.
            IClusterGroup prjClient = _grid1.Cluster.ForClientNodes("cache1");

            Assert.AreEqual(0, prjClient.Nodes().Count);
        }
        
        /// <summary>
        /// Test for cache predicate.
        /// </summary>
        [Test]
        public void TestForPredicate()
        {
            IClusterGroup prj1 = _grid1.Cluster.ForPredicate(new NotAttributePredicate("value1").Apply);
            Assert.AreEqual(2, prj1.Nodes().Count);

            IClusterGroup prj2 = prj1.ForPredicate(new NotAttributePredicate("value2").Apply);
            Assert.AreEqual(1, prj2.Nodes().Count);

            string val;

            prj2.Nodes().First().TryGetAttribute("my_attr", out val);

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

            Assert.AreEqual(val = decimal.Zero, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("65536"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("4294967296"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("281474976710656"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("18446744073709551616"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.MaxValue, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MaxValue - 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue + 1, _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("11,12"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-11,12"), _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { val, val.ToString() }));

            // Test echo with overflow.
            try
            {
                _grid1.Compute().ExecuteJavaTask<object>(DecimalTask, new object[] { null, decimal.MaxValue.ToString() + 1 });

                Assert.Fail();
            }
            catch (IgniteException)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Test echo task returning null.
        /// </summary>
        [Test]
        public void TestEchoTaskNull()
        {
            Assert.IsNull(_grid1.Compute().ExecuteJavaTask<object>(EchoTask, EchoTypeNull));
        }

        /// <summary>
        /// Test echo task returning various primitives.
        /// </summary>
        [Test]
        public void TestEchoTaskPrimitives()
        {
            Assert.AreEqual(1, _grid1.Compute().ExecuteJavaTask<byte>(EchoTask, EchoTypeByte));
            Assert.AreEqual(true, _grid1.Compute().ExecuteJavaTask<bool>(EchoTask, EchoTypeBool));
            Assert.AreEqual(1, _grid1.Compute().ExecuteJavaTask<short>(EchoTask, EchoTypeShort));
            Assert.AreEqual((char)1, _grid1.Compute().ExecuteJavaTask<char>(EchoTask, EchoTypeChar));
            Assert.AreEqual(1, _grid1.Compute().ExecuteJavaTask<int>(EchoTask, EchoTypeInt));
            Assert.AreEqual(1, _grid1.Compute().ExecuteJavaTask<long>(EchoTask, EchoTypeLong));
            Assert.AreEqual((float)1, _grid1.Compute().ExecuteJavaTask<float>(EchoTask, EchoTypeFloat));
            Assert.AreEqual((double)1, _grid1.Compute().ExecuteJavaTask<double>(EchoTask, EchoTypeDouble));
        }

        /// <summary>
        /// Test echo task returning compound types.
        /// </summary>
        [Test]
        public void TestEchoTaskCompound()
        {
            int[] res1 = _grid1.Compute().ExecuteJavaTask<int[]>(EchoTask, EchoTypeArray);

            Assert.AreEqual(1, res1.Length);
            Assert.AreEqual(1, res1[0]);

            IList<int> res2 = _grid1.Compute().ExecuteJavaTask<IList<int>>(EchoTask, EchoTypeCollection);

            Assert.AreEqual(1, res2.Count);
            Assert.AreEqual(1, res2[0]);

            IDictionary<int, int> res3 = _grid1.Compute().ExecuteJavaTask<IDictionary<int, int>>(EchoTask, EchoTypeMap);

            Assert.AreEqual(1, res3.Count);
            Assert.AreEqual(1, res3[1]);
        }

        /// <summary>
        /// Test echo task returning portable object.
        /// </summary>
        [Test]
        public void TestEchoTaskPortable()
        {
            PlatformComputePortable res = _grid1.Compute().ExecuteJavaTask<PlatformComputePortable>(EchoTask, EchoTypePortable);

            Assert.AreEqual(1, res.Field);
        }

        /// <summary>
        /// Test echo task returning portable object with no corresponding class definition.
        /// </summary>
        [Test]
        public void TestEchoTaskPortableNoClass()
        {
            ICompute compute = _grid1.Compute();

            compute.WithKeepPortable();

            IPortableObject res = compute.ExecuteJavaTask<IPortableObject>(EchoTask, EchoTypePortableJava);

            Assert.AreEqual(1, res.Field<int>("field"));

            // This call must fail because "keepPortable" flag is reset.
            Assert.Catch(typeof(PortableException), () =>
            {
                compute.ExecuteJavaTask<IPortableObject>(EchoTask, EchoTypePortableJava);
            });
        }

        /// <summary>
        /// Tests the echo task returning object array.
        /// </summary>
        [Test]
        public void TestEchoTaskObjectArray()
        {
            var res = _grid1.Compute().ExecuteJavaTask<string[]>(EchoTask, EchoTypeObjArray);
            
            Assert.AreEqual(new[] {"foo", "bar", "baz"}, res);
        }

        /// <summary>
        /// Tests the echo task returning portable array.
        /// </summary>
        [Test]
        public void TestEchoTaskPortableArray()
        {
            var res = _grid1.Compute().ExecuteJavaTask<PlatformComputePortable[]>(EchoTask, EchoTypePortableArray);
            
            Assert.AreEqual(3, res.Length);

            for (var i = 0; i < res.Length; i++)
                Assert.AreEqual(i + 1, res[i].Field);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnum()
        {
            var res = _grid1.Compute().ExecuteJavaTask<InteropComputeEnum>(EchoTask, EchoTypeEnum);

            Assert.AreEqual(InteropComputeEnum.Bar, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArray()
        {
            var res = _grid1.Compute().ExecuteJavaTask<InteropComputeEnum[]>(EchoTask, EchoTypeEnumArray);

            Assert.AreEqual(new[]
            {
                InteropComputeEnum.Bar,
                InteropComputeEnum.Baz,
                InteropComputeEnum.Foo
            }, res);
        }

        /// <summary>
        /// Test for portable argument in Java.
        /// </summary>
        [Test]
        public void TestPortableArgTask()
        {
            ICompute compute = _grid1.Compute();

            compute.WithKeepPortable();

            PlatformComputeNetPortable arg = new PlatformComputeNetPortable();

            arg.Field = 100;

            int res = compute.ExecuteJavaTask<int>(PortableArgTask, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Test running broadcast task.
        /// </summary>
        [Test]
        public void TestBroadcastTask()
        {
            ICollection<Guid> res = _grid1.Compute().ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);

            var prj = _grid1.Cluster.ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.Nodes().Count);

            ICollection<Guid> filteredRes = prj.Compute().ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null);

            Assert.AreEqual(2, filteredRes.Count);
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(0)));
            Assert.IsTrue(filteredRes.Contains(res.ElementAt(1)));
        }

        /// <summary>
        /// Test running broadcast task in async mode.
        /// </summary>
        [Test]
        public void TestBroadcastTaskAsync()
        {
            var gridCompute = _grid1.Compute().WithAsync();
            Assert.IsNull(gridCompute.ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null));
            ICollection<Guid> res = gridCompute.GetFuture<ICollection<Guid>>().Get();

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);

            var prj = _grid1.Cluster.ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.Nodes().Count);

            var compute = prj.Compute().WithAsync();
            Assert.IsNull(compute.ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null));
            ICollection<Guid> filteredRes = compute.GetFuture<ICollection<Guid>>().Get();

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
            ComputeAction.InvokeCount = 0;
            
            _grid1.Compute().Broadcast(new ComputeAction());

            Assert.AreEqual(_grid1.Cluster.Nodes().Count, ComputeAction.InvokeCount);
        }

        /// <summary>
        /// Tests single action run.
        /// </summary>
        [Test]
        public void TestRunAction()
        {
            ComputeAction.InvokeCount = 0;
            
            _grid1.Compute().Run(new ComputeAction());

            Assert.AreEqual(1, ComputeAction.InvokeCount);
        }

        /// <summary>
        /// Tests multiple actions run.
        /// </summary>
        [Test]
        public void TestRunActions()
        {
            ComputeAction.InvokeCount = 0;

            var actions = Enumerable.Range(0, 10).Select(x => new ComputeAction());
            
            _grid1.Compute().Run(actions);

            Assert.AreEqual(10, ComputeAction.InvokeCount);
        }

        /// <summary>
        /// Tests affinity run.
        /// </summary>
        [Test]
        public void TestAffinityRun()
        {
            const string cacheName = null;

            // Test keys for non-client nodes
            var nodes = new[] {_grid1, _grid2}.Select(x => x.Cluster.LocalNode);

            var aff = _grid1.Affinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = Enumerable.Range(1, int.MaxValue).First(x => aff.IsPrimary(node, x));

                var affinityKey = _grid1.Affinity(cacheName).AffinityKey<int, int>(primaryKey);

                _grid1.Compute().AffinityRun(cacheName, affinityKey, new ComputeAction());

                Assert.AreEqual(node.Id, ComputeAction.LastNodeId);
            }
        }

        /// <summary>
        /// Tests affinity call.
        /// </summary>
        [Test]
        public void TestAffinityCall()
        {
            const string cacheName = null;

            // Test keys for non-client nodes
            var nodes = new[] { _grid1, _grid2 }.Select(x => x.Cluster.LocalNode);

            var aff = _grid1.Affinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = Enumerable.Range(1, int.MaxValue).First(x => aff.IsPrimary(node, x));

                var affinityKey = _grid1.Affinity(cacheName).AffinityKey<int, int>(primaryKey);

                var result = _grid1.Compute().AffinityCall(cacheName, affinityKey, new ComputeFunc());

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
            ICollection<Guid> res = _grid1.Compute().WithNoFailover().ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);
        }

        /// <summary>
        /// Test "withTimeout" feature.
        /// </summary>
        [Test]
        public void TestWithTimeout()
        {
            ICollection<Guid> res = _grid1.Compute().WithTimeout(1000).ExecuteJavaTask<ICollection<Guid>>(BroadcastTask, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, _grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);
        }

        /// <summary>
        /// Test simple dotNet task execution.
        /// </summary>
        [Test]
        public void TestNetTaskSimple()
        {
            int res = _grid1.Compute().Execute<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                    typeof(NetSimpleTask), new NetSimpleJobArgument(1)).Res;

            Assert.AreEqual(_grid1.Compute().ClusterGroup.Nodes().Count, res);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="path">XML config path.</param>
        private IgniteConfiguration Configuration(string path)
        {
            IgniteConfiguration cfg = new IgniteConfiguration();

            PortableConfiguration portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PlatformComputePortable)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PlatformComputeNetPortable)));
            portTypeCfgs.Add(new PortableTypeConfiguration(JavaPortableCls));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;

            cfg.JvmClasspath = IgniteManager.CreateClasspath(cfg, true);

            cfg.JvmOptions = TestUtils.TestJavaOptions();

            cfg.SpringConfigUrl = path;

            return cfg;
        }
    }

    class PlatformComputePortable
    {
        public int Field
        {
            get;
            set;
        }
    }

    class PlatformComputeNetPortable : PlatformComputePortable
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
                NetSimpleJob job = new NetSimpleJob {Arg = arg};

                jobs[job] = subgrid[i];
            }

            return jobs;
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy Result(IComputeJobResult<NetSimpleJobResult> res,
            IList<IComputeJobResult<NetSimpleJobResult>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        /** <inheritDoc /> */
        public NetSimpleTaskResult Reduce(IList<IComputeJobResult<NetSimpleJobResult>> results)
        {
            return new NetSimpleTaskResult(results.Sum(res => res.Data().Res));
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

        public static int InvokeCount;

        public static Guid LastNodeId;

        public void Invoke()
        {
            Interlocked.Increment(ref InvokeCount);
            LastNodeId = _grid.Cluster.LocalNode.Id;
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
            InvokeCount++;
            LastNodeId = _grid.Cluster.LocalNode.Id;
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

    public enum InteropComputeEnum
    {
        Foo,
        Bar,
        Baz
    }
}
