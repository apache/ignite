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

namespace Apache.Ignite.Core.Tests.Client.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using Apache.Ignite.Core.Tests.Services;
    using NUnit.Framework;

    /// <summary>
    /// Service awareness tests.
    /// </summary>
    public class ServicesAwarenessTest
    {
        /** Name of the test platform service. */
        private const string PlatformSvcName = "PlatformTestService";

        /** All the server nodes. */
        private IIgnite[] _grids;

        /** The thin client. */
        private IIgniteClient _thinClient;
        
        /** */
        private bool _partitionAwareness;

        /** */
        private IList<IIgnite> _serviceNodes;

        /// <summary>
        /// Tests preparations.
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTests()
        {
            _grids = new IIgnite[3];

            for (var i = 0; i < _grids.Length; ++i)
                StartGrid(i);
        }

        /// <summary>
        /// Clears test.
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            _partitionAwareness = true;
            
            _serviceNodes = new List<IIgnite> { _grids[1], _grids[2] };

            DeployJavaService();
            
            DeployPlatformService();
            
            _thinClient = Ignition.StartClient(GetClientConfiguration());
        }
        
        /// <summary>
        /// Clears test.
        /// </summary>
        [TearDown]
        public void AfterTest()
        {
            _thinClient.Dispose();
            
            _grids[0].GetServices().CancelAll();

            TestUtils.WaitForTrueCondition(() => _grids[0].GetServices().GetServiceDescriptors().Count == 0, 20_000);
        }
        
        /// <summary>
        /// Tears down the test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void AfterTests()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration
            {
                Endpoints = new List<string> {IPAddress.Loopback + ":" + IgniteClientConfiguration.DefaultPort},
                SocketTimeout = TimeSpan.FromSeconds(15),
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace}),
                EnablePartitionAwareness = _partitionAwareness
            };
        }

        /// <summary>
        /// Tests service awareness is disabled using several threads.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceAwarenessIsDisabled(string serviceName)
        {
            _thinClient.Dispose();

            _partitionAwareness = false;

            _thinClient = Ignition.StartClient(GetClientConfiguration());

            DoTestServiceAwareness(serviceName, null);
        }

        /// <summary>
        /// Tests service awareness is enabled using several threads.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceAwarenessIsEnabled(string serviceName)
        {
            DoTestServiceAwareness(serviceName, _serviceNodes);
        }
        
        /// <summary>
        /// Tests service topology change is received.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestTopologyChange(string serviceName)
        {
            DoTestServiceAwareness(serviceName, _serviceNodes);

            Ignition.Stop(_grids[2].Name, false);

            TestUtils.WaitForTrueCondition(() => _thinClient.GetCluster().GetNodes().Count == 2, 20_000);

            DoTestServiceAwareness(serviceName, new List<IIgnite> { _grids[1] });

            StartGrid(2);
            
            // Wait for the service redeployment.
            Thread.Sleep(5_000);
            
            DoTestServiceAwareness(serviceName, new List<IIgnite> { _grids[1], _grids[2] });
        }

        /// <summary>
        /// Tests service topology is updated when service is forcibly redeployed.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceRedeploy(string serviceName)
        {
            DoTestServiceAwareness(serviceName, _serviceNodes);

            _grids[0].GetServices().Cancel(serviceName);

            TestUtils.WaitForTrueCondition(() => _grids[0].GetServices().GetServiceDescriptors().Count == 1, 10_000);

            switch (serviceName)
            {
                case TestUtils.JavaServiceName:
                    DeployJavaService(true);
                    break;
                
                case PlatformSvcName:
                    DeployPlatformService(true);
                    break;
                
                default:
                    throw new InvalidOperationException("Unknown service name: " + serviceName);
            }

            // Wait for the update interval.
            Thread.Sleep(10_000);

            DoTestServiceAwareness(serviceName, _grids);
        }

        /// <summary>
        /// Tests service awareness with the cluster group using single node.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName, true)]
        [TestCase(TestUtils.JavaServiceName, false)]
        [TestCase(PlatformSvcName, true)]
        [TestCase(PlatformSvcName, false)]
        public void TestClusterGroupSingleNode(string serviceName, bool correctNode)
        {
            var expectedTopology = correctNode ? new List<IIgnite> { _grids[1] } : new List<IIgnite>();

            // Node 0 has no service instance.
            var clusterGroup = new List<IIgnite> { _grids[correctNode ? 1 : 0] };

            DoTestServiceAwareness(serviceName, expectedTopology, clusterGroup);
        }
        
        /// <summary>
        /// Tests service awareness with the cluster group which intersects the service topology.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName, false)]
        [TestCase(TestUtils.JavaServiceName, true)]        
        [TestCase(PlatformSvcName, false)]
        [TestCase(PlatformSvcName, true)]
        public void TestClusterGroupIntersectsServiceTopology(string serviceName, bool wholeClusterAsGroup)
        {
            var expectedTopology = wholeClusterAsGroup ? _serviceNodes : new List<IIgnite> { _grids[1] };

            // Node 0 has no service instance.
            var clusterGroup = wholeClusterAsGroup ? _grids.ToList() : new List<IIgnite> { _grids[0] , _grids[1] };

            DoTestServiceAwareness(serviceName, expectedTopology, clusterGroup);
        }
        
        /// <summary>
        /// Tests service awareness with the cluster group which is equal the service topology.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestClusterGroupEqualToServiceTopology(string serviceName)
        {
            DoTestServiceAwareness(serviceName, _serviceNodes, _serviceNodes);
        }

        /// <summary>
        /// Checks if service awareness is enabled or disabled.
        /// </summary>
        /// <param name="srcName">Name of the test service.</param>
        /// <param name="expectedTop">Expected nodes to call the service on. If empty, service topology is not expected at all.</param>
        /// <param name="clusterGroup">If not null, filters nodes to call service on.</param>
        private void DoTestServiceAwareness(string srcName, ICollection<IIgnite> expectedTop, ICollection<IIgnite> clusterGroup = null)
        {
            var log = (ListLogger)_thinClient.GetConfiguration().Logger;
            
            log.Clear();

            CallService(ServicesClient(expectedTop, clusterGroup).GetServiceProxy<IJavaService>(srcName), expectedTop?.Count == 0);
            
            var logStr = "Topology of service '" + srcName + "' has been updated. The service instance nodes: ";

            if (expectedTop != null)
            {
                var top = ExtractServiceTopology(log, logStr);

                Assert.AreEqual(expectedTop.Count, top.Count());
                
                // Checks that expected service topology and the received topology are equal.
                Assert.AreEqual(expectedTop.Count, 
                    expectedTop.Select(n => n.GetCluster().GetLocalNode().Id.ToString()).Intersect(top).ToList().Count);
            }
            else
            {
                // Checks that there is no service topology received.
                Assert.AreEqual(0, log.Entries.Count(e => e.Message.Contains(logStr)));
            }
        }

        /// <summary>
        /// Provides proper services client.
        /// </summary>
        private IServicesClient ServicesClient(ICollection<IIgnite> expectedTop, ICollection<IIgnite> clusterGroup)
        {
            if (clusterGroup == null)
                return _thinClient.GetServices();

            return _thinClient.GetCluster()
                .ForPredicate(n => clusterGroup.Any(i => i.GetCluster().GetLocalNode().Id.Equals(n.Id)))
                .GetServices();
        }

        /// <summary>
        /// Calls the service for 5 seconds.
        /// </summary>
        private static void CallService(IJavaService service, bool failureExpected)
        {
            var callLatch = new CountdownEvent(4);

            for (var i = 0; i < 4; ++i)
            {
                Task.Run(() =>
                {
                    for (var i = 0; i < 50; ++i)
                    {
                        if (failureExpected)
                        {
                            Assert.Throws<IgniteClientException>(() => service.test(i));

                            break;
                        }

                        Assert.AreEqual(i + 1, service.test(i));

                        Thread.Sleep(100);
                    }

                    callLatch.Signal();
                });
            }

            callLatch.Wait(20_000);
        }
        
        /// <summary>
        /// Extracts received effective service topology from the client' log.
        /// </summary>
        private static IEnumerable<string> ExtractServiceTopology(ListLogger log, string stringToSearch)
        {
            var logEntries = log.Entries.Where(e => e.Message.Contains(stringToSearch))
                .Select(e => e.Message).ToArray();

            Assert.AreEqual(1, logEntries.Length);

            var nodeIdsIdx = logEntries[0].LastIndexOf(": ") + 2;

            var idsStr = logEntries[0].Substring(nodeIdsIdx, logEntries[0].Length - nodeIdsIdx - 1);

            if (idsStr.Length == 0)
                return new List<string>();

            return logEntries[0].Substring(nodeIdsIdx, logEntries[0].Length - nodeIdsIdx - 1)
                .Replace(stringToSearch, "").Split(", ").ToList();
        }

        /// <summary>
        /// Starts server node.
        /// </summary>
        private void StartGrid(int nodeIdx)
        {
            var cfg = TestUtils.GetTestConfiguration(false, "Node" + nodeIdx);
            
            cfg.ConsistentId = cfg.IgniteInstanceName;
            
            _grids[nodeIdx] = Ignition.Start(cfg);
        }

        /// <summary>
        /// Deploys test Java service.
        /// </summary>
        private void DeployJavaService(bool allNodes = false)
        {
            TestUtils.DeployJavaService(_grids[0], allNodes ? null : ServiceNodesConsistentIds());
        }
        
        /// <summary>
        /// Deploys test platform service.
        /// </summary>
        private void DeployPlatformService(bool allNodes = false)
        {
            var cfg = new ServiceConfiguration
            {
                Name = PlatformSvcName,
                MaxPerNodeCount = 1,
                Service = new PlatformTestService(),
            };

            if (!allNodes)
                cfg.NodeFilter = new NodeConsistentIdFilter(ServiceNodesConsistentIds());

            _grids[0].GetServices().Deploy(cfg);
        }

        /// <summary>
        /// Provides consistent ids of the service nodes.
        /// </summary>
        private ICollection<object> ServiceNodesConsistentIds()
        {
            return _serviceNodes.Select(g => g.GetCluster().GetLocalNode().ConsistentId).ToList();
        }

        /// <summary>
        /// Test node filter.
        /// </summary>
        [Serializable]
        private class NodeConsistentIdFilter : IClusterNodeFilter
        {
            /** */
            private readonly IEnumerable<object> ids;

            /** */
            internal NodeConsistentIdFilter(IEnumerable<object> ids)
            {
                this.ids = ids;
            }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return ids.Contains(node.ConsistentId);
            }
        }
    }
}
