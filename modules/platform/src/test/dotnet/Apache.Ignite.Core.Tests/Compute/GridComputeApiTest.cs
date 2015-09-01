/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

// ReSharper disable SpecifyACultureInStringConversionExplicitly
namespace GridGain.Client.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Compute;
    using GridGain.Impl;
    using GridGain.Portable;
    using GridGain.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for compute.
    /// </summary>
    public class GridComputeApiTest
    {
        /** Echo task name. */
        private const string ECHO_TASK = "org.gridgain.interop.GridInteropComputeEchoTask";

        /** Portable argument task name. */
        private const string PORTABLE_ARG_TASK = "org.gridgain.interop.GridInteropComputePortableArgTask";

        /** Broadcast task name. */
        private const string BROADCAST_TASK = "org.gridgain.interop.GridInteropComputeBroadcastTask";

        /** Broadcast task name. */
        private const string DECIMAL_TASK = "org.gridgain.interop.GridInteropComputeDecimalTask";

        /** Java portable class name. */
        private const string JAVA_PORTABLE_CLS = "GridInteropComputeJavaPortable";

        /** Echo type: null. */
        private const int ECHO_TYPE_NULL = 0;

        /** Echo type: byte. */
        private const int ECHO_TYPE_BYTE = 1;

        /** Echo type: bool. */
        private const int ECHO_TYPE_BOOL = 2;

        /** Echo type: short. */
        private const int ECHO_TYPE_SHORT = 3;

        /** Echo type: char. */
        private const int ECHO_TYPE_CHAR = 4;

        /** Echo type: int. */
        private const int ECHO_TYPE_INT = 5;

        /** Echo type: long. */
        private const int ECHO_TYPE_LONG = 6;

        /** Echo type: float. */
        private const int ECHO_TYPE_FLOAT = 7;

        /** Echo type: double. */
        private const int ECHO_TYPE_DOUBLE = 8;

        /** Echo type: array. */
        private const int ECHO_TYPE_ARRAY = 9;

        /** Echo type: collection. */
        private const int ECHO_TYPE_COLLECTION = 10;

        /** Echo type: map. */
        private const int ECHO_TYPE_MAP = 11;

        /** Echo type: portable. */
        private const int ECHO_TYPE_PORTABLE = 12;

        /** Echo type: portable (Java only). */
        private const int ECHO_TYPE_PORTABLE_JAVA = 13;

        /** Type: object array. */
        private const int ECHO_TYPE_OBJ_ARRAY = 14;

        /** Type: portable object array. */
        private const int ECHO_TYPE_PORTABLE_ARRAY = 15;

        /** Type: enum. */
        private const int ECHO_TYPE_ENUM = 16;

        /** Type: enum array. */
        private const int ECHO_TYPE_ENUM_ARRAY = 17;

        /** First node. */
        private IGrid grid1;

        /** Second node. */
        private IGrid grid2;

        /** Third node. */
        private IGrid grid3;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            //GridTestUtils.JVM_DEBUG = true;
            GridTestUtils.KillProcesses();

            grid1 = GridFactory.Start(Configuration("config\\compute\\compute-grid1.xml"));
            grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));
            grid3 = GridFactory.Start(Configuration("config\\compute\\compute-grid3.xml"));
        }

        [TestFixtureTearDown]
        public void StopClient()
        {
            if (grid1 != null)
                GridFactory.Stop(grid1.Name, true);

            if (grid2 != null)
                GridFactory.Stop(grid2.Name, true);

            if (grid3 != null)
                GridFactory.Stop(grid3.Name, true);
        }

        [TearDown]
        public void AfterTest()
        {
            GridTestUtils.AssertHandleRegistryIsEmpty(1000, grid1, grid2, grid3);
        }

        /// <summary>
        /// Test that it is possible to get projection from grid.
        /// </summary>
        [Test]
        public void TestProjectionGrid()
        {
            IClusterGroup prj = grid1.Cluster;

            Assert.NotNull(prj);

            Assert.IsTrue(prj == prj.Grid);
        }

        /// <summary>
        /// Test getting cache with default (null) name.
        /// </summary>
        [Test]
        public void TestCacheDefaultName()
        {
            var cache = grid1.Cache<int, int>(null);

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
                grid1.Cache<int, int>("bad_name");
            });
        }

        /// <summary>
        /// Test node content.
        /// </summary>
        [Test]
        public void TestNodeContent()
        {
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

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
            var cluster = grid1.Cluster;

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
            var node = grid1.Cluster.Node();

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
            var cluster = grid1.Cluster;

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
            var cluster = grid1.Cluster;

            Assert.IsTrue(cluster.Nodes().Select(node => node.Id).All(cluster.PingNode));
            
            Assert.IsFalse(cluster.PingNode(Guid.NewGuid()));
        }

        /// <summary>
        /// Tests the topology version.
        /// </summary>
        [Test]
        public void TestTopologyVersion()
        {
            var cluster = grid1.Cluster;
            
            var topVer = cluster.TopologyVersion;

            GridFactory.Stop(grid3.Name, true);

            Assert.AreEqual(topVer + 1, grid1.Cluster.TopologyVersion);

            grid3 = GridFactory.Start(Configuration("config\\compute\\compute-grid3.xml"));

            Assert.AreEqual(topVer + 2, grid1.Cluster.TopologyVersion);
        }

        /// <summary>
        /// Tests the topology by version.
        /// </summary>
        [Test]
        public void TestTopology()
        {
            var cluster = grid1.Cluster;

            Assert.AreEqual(1, cluster.Topology(1).Count);

            Assert.AreEqual(null, cluster.Topology(int.MaxValue));

            // Check that Nodes and Topology return the same for current version
            var topVer = cluster.TopologyVersion;

            var top = cluster.Topology(topVer);

            var nodes = cluster.Nodes();

            Assert.AreEqual(top.Count, nodes.Count);

            Assert.IsTrue(top.All(nodes.Contains));

            // Stop/start node to advance version and check that history is still correct
            Assert.IsTrue(GridFactory.Stop(grid2.Name, true));

            try
            {
                top = cluster.Topology(topVer);

                Assert.AreEqual(top.Count, nodes.Count);

                Assert.IsTrue(top.All(nodes.Contains));
            }
            finally 
            {
                grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));
            }
        }

        /// <summary>
        /// Test nodes in full topology.
        /// </summary>
        [Test]
        public void TestNodes()
        {
            Assert.IsNotNull(grid1.Cluster.Node());

            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);

            // Check subsequent call on the same topology.
            nodes = grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);

            Assert.IsTrue(GridFactory.Stop(grid2.Name, true));

            // Check subsequent calls on updating topologies.
            nodes = grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 2);

            nodes = grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 2);

            grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));

            nodes = grid1.Cluster.Nodes();

            Assert.IsTrue(nodes.Count == 3);
        }

        /// <summary>
        /// Test "ForNodes" and "ForNodeIds".
        /// </summary>
        [Test]
        public void TestForNodes()
        {
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            IClusterNode first = nodes.ElementAt(0);
            IClusterNode second = nodes.ElementAt(1);

            IClusterGroup singleNodePrj = grid1.Cluster.ForNodeIds(first.Id);
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = grid1.Cluster.ForNodeIds(new List<Guid> { first.Id });
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = grid1.Cluster.ForNodes(first);
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            singleNodePrj = grid1.Cluster.ForNodes(new List<IClusterNode> { first });
            Assert.AreEqual(1, singleNodePrj.Nodes().Count);
            Assert.AreEqual(first.Id, singleNodePrj.Nodes().First().Id);

            IClusterGroup multiNodePrj = grid1.Cluster.ForNodeIds(first.Id, second.Id);
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = grid1.Cluster.ForNodeIds(new[] {first, second}.Select(x => x.Id));
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = grid1.Cluster.ForNodes(first, second);
            Assert.AreEqual(2, multiNodePrj.Nodes().Count);
            Assert.IsTrue(multiNodePrj.Nodes().Contains(first));
            Assert.IsTrue(multiNodePrj.Nodes().Contains(second));

            multiNodePrj = grid1.Cluster.ForNodes(new List<IClusterNode> { first, second });
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
            var nodes = grid1.Cluster.Nodes().Take(2).ToArray();

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

            var projection = grid1.Cluster.ForNodes(nodes.Select(nodeSelector));
            Assert.AreEqual(2, projection.Nodes().Count);
            Assert.AreEqual(2, callCount);
            
            projection = grid1.Cluster.ForNodeIds(nodes.Select(idSelector));
            Assert.AreEqual(2, projection.Nodes().Count);
            Assert.AreEqual(4, callCount);
        }

        /// <summary>
        /// Test for local node projection.
        /// </summary>
        [Test]
        public void TestForLocal()
        {
            IClusterGroup prj = grid1.Cluster.ForLocal();

            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.AreEqual(grid1.Cluster.LocalNode, prj.Nodes().First());
        }

        /// <summary>
        /// Test for remote nodes projection.
        /// </summary>
        [Test]
        public void TestForRemotes()
        {
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            IClusterGroup prj = grid1.Cluster.ForRemotes();

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
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            IClusterGroup prj = grid1.Cluster.ForHost(nodes.First());

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
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            IClusterGroup prj = grid1.Cluster.ForYoungest();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));

            prj = grid1.Cluster.ForOldest();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));

            prj = grid1.Cluster.ForRandom();
            Assert.AreEqual(1, prj.Nodes().Count);
            Assert.IsTrue(nodes.Contains(prj.Node()));
        }

        /// <summary>
        /// Test for attribute projection.
        /// </summary>
        [Test]
        public void TestForAttribute()
        {
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            IClusterGroup prj = grid1.Cluster.ForAttribute("my_attr", "value1");
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
            ICollection<IClusterNode> nodes = grid1.Cluster.Nodes();

            // Cache nodes.
            IClusterGroup prjCache = grid1.Cluster.ForCacheNodes("cache1");

            Assert.AreEqual(2, prjCache.Nodes().Count);

            Assert.IsTrue(nodes.Contains(prjCache.Nodes().ElementAt(0)));
            Assert.IsTrue(nodes.Contains(prjCache.Nodes().ElementAt(1)));
            
            // Data nodes.
            IClusterGroup prjData = grid1.Cluster.ForDataNodes("cache1");

            Assert.AreEqual(2, prjData.Nodes().Count);

            Assert.IsTrue(prjCache.Nodes().Contains(prjData.Nodes().ElementAt(0)));
            Assert.IsTrue(prjCache.Nodes().Contains(prjData.Nodes().ElementAt(1)));

            // Client nodes.
            IClusterGroup prjClient = grid1.Cluster.ForClientNodes("cache1");

            Assert.AreEqual(0, prjClient.Nodes().Count);
        }
        
        /// <summary>
        /// Test for cache predicate.
        /// </summary>
        [Test]
        public void TestForPredicate()
        {
            IClusterGroup prj1 = grid1.Cluster.ForPredicate(new NotAttributePredicate("value1").Apply);
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
            private readonly string attrVal;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="attrVal">Required attribute value.</param>
            public NotAttributePredicate(string attrVal)
            {
                this.attrVal = attrVal;
            }

            /** <inhreitDoc /> */
            public bool Apply(IClusterNode node)
            {
                string val;

                node.TryGetAttribute("my_attr", out val);

                return val == null || !val.Equals(attrVal);
            }
        }

        /// <summary>
        /// Test echo with decimals.
        /// </summary>
        [Test]
        public void TestEchoDecimal()
        {
            decimal val;

            Assert.AreEqual(val = decimal.Zero, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, 1, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MinValue, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 0, int.MaxValue, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, 1, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MinValue, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(0, int.MaxValue, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 0, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MinValue, 0, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(int.MaxValue, 0, 0, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, false, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = new decimal(1, 1, 1, true, 0) + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("65536"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("65536") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-65536") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("4294967296"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("4294967296") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-4294967296") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("281474976710656"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("281474976710656") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-281474976710656") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("18446744073709551616"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("18446744073709551616") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-18446744073709551616") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("1208925819614629174706176") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-1208925819614629174706176") + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.MaxValue, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MaxValue - 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.MinValue + 1, grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            Assert.AreEqual(val = decimal.Parse("11,12"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));
            Assert.AreEqual(val = decimal.Parse("-11,12"), grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { val, val.ToString() }));

            // Test echo with overflow.
            try
            {
                grid1.Compute().ExecuteJavaTask<Object>(DECIMAL_TASK, new object[] { null, decimal.MaxValue.ToString() + 1 });

                Assert.Fail();
            }
            catch (GridException)
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
            Assert.IsNull(grid1.Compute().ExecuteJavaTask<Object>(ECHO_TASK, ECHO_TYPE_NULL));
        }

        /// <summary>
        /// Test echo task returning various primitives.
        /// </summary>
        [Test]
        public void TestEchoTaskPrimitives()
        {
            Assert.AreEqual(1, grid1.Compute().ExecuteJavaTask<byte>(ECHO_TASK, ECHO_TYPE_BYTE));
            Assert.AreEqual(true, grid1.Compute().ExecuteJavaTask<bool>(ECHO_TASK, ECHO_TYPE_BOOL));
            Assert.AreEqual(1, grid1.Compute().ExecuteJavaTask<short>(ECHO_TASK, ECHO_TYPE_SHORT));
            Assert.AreEqual((char)1, grid1.Compute().ExecuteJavaTask<char>(ECHO_TASK, ECHO_TYPE_CHAR));
            Assert.AreEqual(1, grid1.Compute().ExecuteJavaTask<int>(ECHO_TASK, ECHO_TYPE_INT));
            Assert.AreEqual(1, grid1.Compute().ExecuteJavaTask<long>(ECHO_TASK, ECHO_TYPE_LONG));
            Assert.AreEqual((float)1, grid1.Compute().ExecuteJavaTask<float>(ECHO_TASK, ECHO_TYPE_FLOAT));
            Assert.AreEqual((double)1, grid1.Compute().ExecuteJavaTask<double>(ECHO_TASK, ECHO_TYPE_DOUBLE));
        }

        /// <summary>
        /// Test echo task returning compound types.
        /// </summary>
        [Test]
        public void TestEchoTaskCompound()
        {
            int[] res1 = grid1.Compute().ExecuteJavaTask<int[]>(ECHO_TASK, ECHO_TYPE_ARRAY);

            Assert.AreEqual(1, res1.Length);
            Assert.AreEqual(1, res1[0]);

            IList<int> res2 = grid1.Compute().ExecuteJavaTask<IList<int>>(ECHO_TASK, ECHO_TYPE_COLLECTION);

            Assert.AreEqual(1, res2.Count);
            Assert.AreEqual(1, res2[0]);

            IDictionary<int, int> res3 = grid1.Compute().ExecuteJavaTask<IDictionary<int, int>>(ECHO_TASK, ECHO_TYPE_MAP);

            Assert.AreEqual(1, res3.Count);
            Assert.AreEqual(1, res3[1]);
        }

        /// <summary>
        /// Test echo task returning portable object.
        /// </summary>
        [Test]
        public void TestEchoTaskPortable()
        {
            GridInteropComputePortable res = grid1.Compute().ExecuteJavaTask<GridInteropComputePortable>(ECHO_TASK, ECHO_TYPE_PORTABLE);

            Assert.AreEqual(1, res.Field);
        }

        /// <summary>
        /// Test echo task returning portable object with no corresponding class definition.
        /// </summary>
        [Test]
        public void TestEchoTaskPortableNoClass()
        {
            ICompute compute = grid1.Compute();

            compute.WithKeepPortable();

            IPortableObject res = compute.ExecuteJavaTask<IPortableObject>(ECHO_TASK, ECHO_TYPE_PORTABLE_JAVA);

            Assert.AreEqual(1, res.Field<int>("field"));

            // This call must fail because "keepPortable" flag is reset.
            Assert.Catch(typeof(PortableException), () =>
            {
                compute.ExecuteJavaTask<IPortableObject>(ECHO_TASK, ECHO_TYPE_PORTABLE_JAVA);
            });
        }

        /// <summary>
        /// Tests the echo task returning object array.
        /// </summary>
        [Test]
        public void TestEchoTaskObjectArray()
        {
            var res = grid1.Compute().ExecuteJavaTask<string[]>(ECHO_TASK, ECHO_TYPE_OBJ_ARRAY);
            
            Assert.AreEqual(new[] {"foo", "bar", "baz"}, res);
        }

        /// <summary>
        /// Tests the echo task returning portable array.
        /// </summary>
        [Test]
        public void TestEchoTaskPortableArray()
        {
            var res = grid1.Compute().ExecuteJavaTask<GridInteropComputePortable[]>(ECHO_TASK, ECHO_TYPE_PORTABLE_ARRAY);
            
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
            var res = grid1.Compute().ExecuteJavaTask<GridInteropComputeEnum>(ECHO_TASK, ECHO_TYPE_ENUM);

            Assert.AreEqual(GridInteropComputeEnum.BAR, res);
        }

        /// <summary>
        /// Tests the echo task returning enum.
        /// </summary>
        [Test]
        public void TestEchoTaskEnumArray()
        {
            var res = grid1.Compute().ExecuteJavaTask<GridInteropComputeEnum[]>(ECHO_TASK, ECHO_TYPE_ENUM_ARRAY);

            Assert.AreEqual(new[]
            {
                GridInteropComputeEnum.BAR,
                GridInteropComputeEnum.BAZ,
                GridInteropComputeEnum.FOO
            }, res);
        }

        /// <summary>
        /// Test for portable argument in Java.
        /// </summary>
        [Test]
        public void TestPortableArgTask()
        {
            ICompute compute = grid1.Compute();

            compute.WithKeepPortable();

            GridInteropComputeNetPortable arg = new GridInteropComputeNetPortable();

            arg.Field = 100;

            int res = compute.ExecuteJavaTask<int>(PORTABLE_ARG_TASK, arg);

            Assert.AreEqual(arg.Field, res);
        }

        /// <summary>
        /// Test running broadcast task.
        /// </summary>
        [Test]
        public void TestBroadcastTask()
        {
            ICollection<Guid> res = grid1.Compute().ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);

            var prj = grid1.Cluster.ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.Nodes().Count);

            ICollection<Guid> filteredRes = prj.Compute().ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null);

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
            var gridCompute = grid1.Compute().WithAsync();
            Assert.IsNull(gridCompute.ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null));
            ICollection<Guid> res = gridCompute.GetFuture<ICollection<Guid>>().Get();

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);

            var prj = grid1.Cluster.ForPredicate(node => res.Take(2).Contains(node.Id));

            Assert.AreEqual(2, prj.Nodes().Count);

            var compute = prj.Compute().WithAsync();
            Assert.IsNull(compute.ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null));
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
            ComputeAction.invokeCount = 0;
            
            grid1.Compute().Broadcast(new ComputeAction());

            Assert.AreEqual(grid1.Cluster.Nodes().Count, ComputeAction.invokeCount);
        }

        /// <summary>
        /// Tests single action run.
        /// </summary>
        [Test]
        public void TestRunAction()
        {
            ComputeAction.invokeCount = 0;
            
            grid1.Compute().Run(new ComputeAction());

            Assert.AreEqual(1, ComputeAction.invokeCount);
        }

        /// <summary>
        /// Tests multiple actions run.
        /// </summary>
        [Test]
        public void TestRunActions()
        {
            ComputeAction.invokeCount = 0;

            var actions = Enumerable.Range(0, 10).Select(x => new ComputeAction());
            
            grid1.Compute().Run(actions);

            Assert.AreEqual(10, ComputeAction.invokeCount);
        }

        /// <summary>
        /// Tests affinity run.
        /// </summary>
        [Test]
        public void TestAffinityRun()
        {
            const string cacheName = null;

            // Test keys for non-client nodes
            var nodes = new[] {grid1, grid2}.Select(x => x.Cluster.LocalNode);

            var aff = grid1.Affinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = Enumerable.Range(1, int.MaxValue).First(x => aff.IsPrimary(node, x));

                var affinityKey = grid1.Affinity(cacheName).AffinityKey<int, int>(primaryKey);

                grid1.Compute().AffinityRun(cacheName, affinityKey, new ComputeAction());

                Assert.AreEqual(node.Id, ComputeAction.lastNodeId);
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
            var nodes = new[] { grid1, grid2 }.Select(x => x.Cluster.LocalNode);

            var aff = grid1.Affinity(cacheName);

            foreach (var node in nodes)
            {
                var primaryKey = Enumerable.Range(1, int.MaxValue).First(x => aff.IsPrimary(node, x));

                var affinityKey = grid1.Affinity(cacheName).AffinityKey<int, int>(primaryKey);

                var result = grid1.Compute().AffinityCall(cacheName, affinityKey, new ComputeFunc());

                Assert.AreEqual(result, ComputeFunc.invokeCount);

                Assert.AreEqual(node.Id, ComputeFunc.lastNodeId);
            }
        }

        /// <summary>
        /// Test "withNoFailover" feature.
        /// </summary>
        [Test]
        public void TestWithNoFailover()
        {
            ICollection<Guid> res = grid1.Compute().WithNoFailover().ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);
        }

        /// <summary>
        /// Test "withTimeout" feature.
        /// </summary>
        [Test]
        public void TestWithTimeout()
        {
            ICollection<Guid> res = grid1.Compute().WithTimeout(1000).ExecuteJavaTask<ICollection<Guid>>(BROADCAST_TASK, null);

            Assert.AreEqual(3, res.Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(0)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(1)).Nodes().Count);
            Assert.AreEqual(1, grid1.Cluster.ForNodeIds(res.ElementAt(2)).Nodes().Count);
        }

        /// <summary>
        /// Test simple dotNet task execution.
        /// </summary>
        [Test]
        public void TestNetTaskSimple()
        {
            int res = grid1.Compute().Execute<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                    typeof(NetSimpleTask), new NetSimpleJobArgument(1)).res;

            Assert.AreEqual(grid1.Compute().ClusterGroup.Nodes().Count, res);
        }

        /// <summary>
        /// Create configuration.
        /// </summary>
        /// <param name="path">XML config path.</param>
        private GridConfiguration Configuration(string path)
        {
            GridConfiguration cfg = new GridConfiguration();

            PortableConfiguration portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(GridInteropComputePortable)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(GridInteropComputeNetPortable)));
            portTypeCfgs.Add(new PortableTypeConfiguration(JAVA_PORTABLE_CLS));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;

            cfg.JvmClasspath = GridManager.CreateClasspath(cfg, true);

            cfg.JvmOptions = GridTestUtils.TestJavaOptions();

            cfg.SpringConfigUrl = path;

            return cfg;
        }
    }

    class GridInteropComputePortable
    {
        public int Field
        {
            get;
            set;
        }
    }

    class GridInteropComputeNetPortable : GridInteropComputePortable
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
                NetSimpleJob job = new NetSimpleJob {arg = arg};

                jobs[job] = subgrid[i];
            }

            return jobs;
        }

        /** <inheritDoc /> */
        public ComputeJobResultPolicy Result(IComputeJobResult<NetSimpleJobResult> res,
            IList<IComputeJobResult<NetSimpleJobResult>> rcvd)
        {
            return ComputeJobResultPolicy.WAIT;
        }

        /** <inheritDoc /> */
        public NetSimpleTaskResult Reduce(IList<IComputeJobResult<NetSimpleJobResult>> results)
        {
            return new NetSimpleTaskResult(results.Sum(res => res.Data().res));
        }
    }

    [Serializable]
    class NetSimpleJob : IComputeJob<NetSimpleJobResult>
    {
        public NetSimpleJobArgument arg;

        /** <inheritDoc /> */
        public NetSimpleJobResult Execute()
        {
            return new NetSimpleJobResult(arg.arg);
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
        public int arg;

        public NetSimpleJobArgument(int arg)
        {
            this.arg = arg;
        }
    }

    [Serializable]
    class NetSimpleTaskResult
    {
        public int res;

        public NetSimpleTaskResult(int res)
        {
            this.res = res;
        }
    }

    [Serializable]
    class NetSimpleJobResult
    {
        public int res;

        public NetSimpleJobResult(int res)
        {
            this.res = res;
        }
    }

    [Serializable]
    class ComputeAction : IComputeAction
    {
        [InstanceResource]
        #pragma warning disable 649
        private IGrid grid;

        public static int invokeCount;

        public static Guid lastNodeId;

        public void Invoke()
        {
            Interlocked.Increment(ref invokeCount);
            lastNodeId = grid.Cluster.LocalNode.Id;
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
        private IGrid grid;

        public static int invokeCount;

        public static Guid lastNodeId;

        int IComputeFunc<int>.Invoke()
        {
            invokeCount++;
            lastNodeId = grid.Cluster.LocalNode.Id;
            return invokeCount;
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

    public enum GridInteropComputeEnum
    {
        FOO,
        BAR,
        BAZ
    }
}
