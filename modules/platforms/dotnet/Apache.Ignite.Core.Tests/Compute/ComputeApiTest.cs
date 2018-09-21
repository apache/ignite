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

// ReSharper disable UnusedAutoPropertyAccessor.Global
namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for compute.
    /// </summary>
    public partial class ComputeApiTest
    {
        /** Java binary class name. */
        private const string JavaBinaryCls = "PlatformComputeJavaBinarizable";

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
            var configs = GetConfigs();

            _grid1 = Ignition.Start(Configuration(configs.Item1));
            _grid2 = Ignition.Start(Configuration(configs.Item2));
            _grid3 = Ignition.Start(Configuration(configs.Item3));

            // Wait for rebalance.
            var events = _grid1.GetEvents();
            events.EnableLocal(EventType.CacheRebalanceStopped);
            events.WaitForLocal(EventType.CacheRebalanceStopped);
        }

        /// <summary>
        /// Gets the configs.
        /// </summary>
        protected virtual Tuple<string, string, string> GetConfigs()
        {
            return Tuple.Create(
                "Config\\Compute\\compute-grid1.xml",
                "Config\\Compute\\compute-grid2.xml",
                "Config\\Compute\\compute-grid3.xml");
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

                Assert.NotNull(node.Attributes);
                Assert.IsTrue(node.Attributes.Count > 0);
                Assert.Throws<NotSupportedException>(() => node.Attributes.Add("key", "val"));

#pragma warning disable 618
                Assert.AreSame(node.Attributes, node.GetAttributes());
#pragma warning restore 618

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
            Assert.Throws<AggregateException>(() => _grid1.GetCompute().Broadcast(new InvalidComputeAction()));

            Assert.Throws<AggregateException>(
                () => _grid1.GetCompute().Execute<NetSimpleJobArgument, NetSimpleJobResult, NetSimpleTaskResult>(
                    typeof (NetSimpleTask), new NetSimpleJobArgument(-1)));

            // Local.
            var ex = Assert.Throws<AggregateException>(() =>
                _grid1.GetCluster().ForLocal().GetCompute().Broadcast(new ExceptionalComputeAction()));

            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual("Compute job has failed on local node, examine InnerException for details.", 
                ex.InnerException.Message);
            Assert.IsNotNull(ex.InnerException.InnerException);
            Assert.AreEqual(ExceptionalComputeAction.ErrorText, ex.InnerException.InnerException.Message);

            // Remote.
            ex = Assert.Throws<AggregateException>(() =>
                _grid1.GetCluster().ForRemotes().GetCompute().Broadcast(new ExceptionalComputeAction()));

            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual("Compute job has failed on remote node, examine InnerException for details.",
                ex.InnerException.Message);
            Assert.IsNotNull(ex.InnerException.InnerException);
            Assert.AreEqual(ExceptionalComputeAction.ErrorText, ex.InnerException.InnerException.Message);
        }

        /// <summary>
        /// Tests the footer setting.
        /// </summary>
        [Test]
        public void TestFooterSetting()
        {
            Assert.AreEqual(CompactFooter, ((Ignite) _grid1).Marshaller.CompactFooter);

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
                    NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
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
