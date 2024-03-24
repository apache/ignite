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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using Apache.Ignite.Core.Tests.Services;
    using NUnit.Framework;

    /// <summary>
    /// Service awareness tests.
    /// </summary>
    public class ServicesAwarenessTests : ClientTestBase
    {
        /** Name of the test platform service. */
        private const string PlatformSvcName = "PlatformTestService";
        
        /** Service call request in the server logs. */
        private const string ServiceCallRequestPrefix = "service.ClientService";

        /** Number of service load threads. */
        private const int ServiceCallThreads = 4;

        /** Consistent ids of the nodes with the service instances. */
        private IList<object> _topConsistentIds;
        
        /// <summary>
        /// Creates test.
        /// </summary>
        public ServicesAwarenessTests() : base(gridCount: 3, enableServerListLogging: true)
        {
            // No-op.
        }

        /// <summary>
        /// Prepares each test.
        /// </summary>
        [SetUp]
        public override void TestSetUp()
        {
            // By default we deploy service on the second and third node.
            _topConsistentIds = new List<object> { GetConsistentId(1), GetConsistentId(2) };
            RedeployServices();

            StartClient(enablePartitionAwareness: true);
        }
        
        /// <summary>
        /// Clears each test.
        /// </summary>
        [TearDown]
        public void TestTearDown()
        {
            Client.Dispose();
            
            GetIgnite().GetServices().CancelAll();

            TestUtils.WaitForTrueCondition(() => GetIgnite().GetServices().GetServiceDescriptors().Count == 0, 20_000);
        }

        /// <summary>
        /// Tests service awareness is disabled.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceAwarenessIsDisabled(string serviceName)
        {
            StartClient(enablePartitionAwareness: false);
            DoTestServiceAwareness(serviceName, null);
        }

        /// <summary>
        /// Tests service awareness is enabled.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceAwarenessIsEnabled(string serviceName)
        {
            DoTestServiceAwareness(serviceName, FilterGridsNodes());
        }

        /// <summary>
        /// Tests service topology is updated when the cluster topology changes.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestClusterTopologyChanges(string serviceName)
        {
            var prevServiceNodes = _topConsistentIds;

            // New nodes filter includes additional node.
            var newNodeConsistentId = "newNode";
            _topConsistentIds = new List<object> { GetConsistentId(1), GetConsistentId(2), newNodeConsistentId };

            RedeployServices();

            // Additional node is not started. Service topology must be the same.
            DoTestServiceAwareness(serviceName, FilterGridsNodes(prevServiceNodes));

            var newNodeCfg = new IgniteConfiguration(GetIgniteConfiguration())
            {
                ConsistentId = newNodeConsistentId,
                IgniteInstanceName = newNodeConsistentId
            };

            using (Ignition.Start(newNodeCfg))
            {
                WaitForClientConnectionsNumber(4);

                // Additional node is started. Service topology must include the new node.
                DoTestServiceAwareness(serviceName, FilterGridsNodes());
            }

            WaitForClientConnectionsNumber(3);

            // Additional node stopped. Service topology must be as at the beginning.
            DoTestServiceAwareness(serviceName, FilterGridsNodes(prevServiceNodes));
        }

        /// <summary>
        /// Tests service topology is updated when service is forcibly redeployed.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestServiceRedeploy(string serviceName)
        {
            DoTestServiceAwareness(serviceName, FilterGridsNodes());

            RedeployServices(true);

            // Wait for the update interval.
            Thread.Sleep(Impl.Client.Services.ServicesClient.SrvTopUpdatePeriod);

            DoTestServiceAwareness(serviceName, Ignition.GetAll());
        }

        /// <summary>
        /// Tests service awareness with a intersecting cluster group of a single node.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName, true)]
        [TestCase(TestUtils.JavaServiceName, false)]
        [TestCase(PlatformSvcName, true)]
        [TestCase(PlatformSvcName, false)]
        public void TestClusterGroupSingleNode(string serviceName, bool correctNode)
        {
            var expectedTopology = correctNode ? new List<IIgnite> { GetIgnite(1) } : new List<IIgnite>();

            // Node 0 has no service instance.
            var clusterGroup = new List<IIgnite> { correctNode ? GetIgnite(1) : GetIgnite() };

            DoTestServiceAwareness(serviceName, expectedTopology, clusterGroup);
        }
        
        /// <summary>
        /// Tests service awareness with a cluster group intersecting the service topology.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName, false)]
        [TestCase(TestUtils.JavaServiceName, true)]        
        [TestCase(PlatformSvcName, false)]
        [TestCase(PlatformSvcName, true)]
        public void TestClusterGroupIntersectsServiceTopology(string serviceName, bool wholeClusterAsGroup)
        {
            var expectedTopology = wholeClusterAsGroup ? FilterGridsNodes() : new List<IIgnite> { GetIgnite(1) };

            // Node 0 has no service instance.
            var clusterGroup = wholeClusterAsGroup ? Ignition.GetAll() : new List<IIgnite> { GetIgnite() , GetIgnite(1) };

            DoTestServiceAwareness(serviceName, expectedTopology, clusterGroup);
        }
        
        /// <summary>
        /// Tests service awareness with a cluster group equal the service topology.
        /// </summary>
        [Test]
        [TestCase(TestUtils.JavaServiceName)]
        [TestCase(PlatformSvcName)]
        public void TestClusterGroupEqualToServiceTopology(string serviceName)
        {
            DoTestServiceAwareness(serviceName, FilterGridsNodes(), 
                Ignition.GetAll().Where(g=>_topConsistentIds.Contains(GetConsistentId(g))).ToList());
        }

        /// <summary>
        /// Checks if service awareness is enabled or disabled.
        /// </summary>
        /// <param name="srcName">Name of the test service.</param>
        /// <param name="expectedTop">Expected nodes to call the service on. If empty, service topology is not expected at all.</param>
        /// <param name="clusterGroup">If not null, filters nodes to call service on.</param>
        private void DoTestServiceAwareness(
            string srcName,
            ICollection<IIgnite> expectedTop,
            ICollection<IIgnite> clusterGroup = null)
        {
            var log = (ListLogger)Client.GetConfiguration().Logger;
            
            log.Clear();

            var expCnt = expectedTop?.Count;
            
            CallService(ServicesClient(clusterGroup).GetServiceProxy<IJavaService>(srcName), expCnt == 0);
            
            var clientLogStr = "Topology of service '" + srcName + "' has been updated. The service instance nodes: ";

            if (expectedTop != null)
            {
                var top = ExtractServiceTopology(log, clientLogStr);

                Assert.AreEqual(expCnt, top.Count());

                // Checks that expected service topology and the received topology are equal.
                Assert.AreEqual(expCnt,
                    expectedTop.Select(g => g.GetCluster().GetLocalNode().Id.ToString()).Intersect(top).Count());
                
                // Check server logs. On the target nodes we must see service invocation. On the others not.
                foreach (var ignite in Ignition.GetAll())
                {
                    var requests = GetServerRequestNames((ListLogger)ignite.Logger, ServiceCallRequestPrefix);

                    if(expectedTop.Contains(ignite))
                        Assert.IsTrue(requests.Any());
                    else
                        Assert.IsEmpty(requests);
                }
            }
            else
            {
                // Check the client logs. There must be no service topology. 
                Assert.AreEqual(0, log.Entries.Count(e => e.Message.Contains(clientLogStr)));
                
                // Check server logs. Without service awareness we must see service invocation only on the first node.
                foreach (var ignite in Ignition.GetAll())
                {
                    var requests = GetServerRequestNames((ListLogger)ignite.Logger, ServiceCallRequestPrefix);

                    if (ignite.Equals(GetIgnite()))
                        Assert.IsTrue(requests.Any());
                    else
                        Assert.IsFalse(requests.Any());
                }
            }
        }
        
        /// <summary>
        /// Provides proper services client.
        /// </summary>
        private IServicesClient ServicesClient(ICollection<IIgnite> clusterGroup)
        {
            if (clusterGroup == null)
                return Client.GetServices();

            return Client.GetCluster()
                .ForPredicate(n => clusterGroup.Any(i => i.GetCluster().GetLocalNode().Id.Equals(n.Id)))
                .GetServices();
        }

        /// <summary>
        /// Calls the service asynchronously 2 times. First time, keeps calling for 3 seconds with some call interval.
        /// After, clears all the server logs and calls service again many times without any interval.
        /// </summary>
        private static void CallService(IJavaService service, bool failureExpected)
        {
            var topologyLatch = new CountdownEvent(ServiceCallThreads);

            for (var t = 0; t < ServiceCallThreads; ++t)
            {
                Task.Run(() =>
                {
                    // Cal service for 3 seconds. If service awareness is enabled, service topology must be received.
                    for (var c = 0; c < 30; ++c)
                    {
                        if (failureExpected)
                        {
                            Assert.Throws<IgniteClientException>(() => service.test(c));

                            break;
                        }
                        
                        service.test(c);
                        
                        Thread.Sleep(100);
                    }

                    topologyLatch.Signal();
                });
            }

            topologyLatch.Wait(20_000);

            ClearLoggers();

            if (failureExpected)
                return;
            
            var finishLatch = new CountdownEvent(ServiceCallThreads);

            for (var t = 0; t < ServiceCallThreads; ++t)
            {
                Task.Run(() =>
                {
                    for (var c = 0; c < 100; ++c)
                        service.test(c);

                    finishLatch.Signal();
                });
            }

            finishLatch.Wait(5_000);
        }
        
        /// <summary>
        /// Extracts received effective service topology from the client' log. Expects strictly one record. Ensures
        /// that service topology is updated once.
        /// </summary>
        private static IList<string> ExtractServiceTopology(ListLogger log, string stringToSearch)
        {
            var logEntry = log.Entries
                .Where(e => e.Message.Contains(stringToSearch))
                .Select(e => e.Message)
                .Single();
            
            var nodeIdsIdx = logEntry.LastIndexOf(": ", StringComparison.Ordinal) + 2;
            var idsStr = logEntry.Substring(nodeIdsIdx, logEntry.Length - nodeIdsIdx - 1);

            return idsStr.Split(", ".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
        }

        /// <summary>
        /// Redeploys the platform and Java services.
        /// </summary>
        private void RedeployServices(bool allNodes = false)
        {
            GetIgnite().GetServices().CancelAll();

            TestUtils.WaitForTrueCondition(() => GetIgnite().GetServices().GetServiceDescriptors().Count == 0, 10_000);
            
            TestUtils.DeployJavaService(GetIgnite(), allNodes ? null : _topConsistentIds);
            
            var cfg = new ServiceConfiguration
            {
                Name = PlatformSvcName,
                MaxPerNodeCount = 1,
                Service = new PlatformTestService(),
            };

            if (!allNodes)
                cfg.NodeFilter = new NodeConsistentIdFilter(_topConsistentIds);

            GetIgnite().GetServices().Deploy(cfg);
            
            TestUtils.WaitForTrueCondition(() => GetIgnite().GetServices().GetServiceDescriptors().Count == 2, 10_000);
        }

        /// <summary>
        /// Waits until certain number of client connections.
        /// </summary>
        private void WaitForClientConnectionsNumber(int cnt)
        {
            TestUtils.WaitForTrueCondition(() =>
            {
                // Force update of the client's topology.
                Client.GetCacheNames();
                
                return Client.GetConnections().Count() == cnt;
            }, 20_000);
        }
        
        private static object GetConsistentId(IIgnite ignite) => ignite.GetCluster().GetLocalNode().ConsistentId;
        
        private static object GetConsistentId(int? idx) => GetConsistentId(GetIgnite(idx));

        /// <summary>
        /// Filters grids by consistent ids. By default uses the service topology ids. 
        /// </summary>
        private ICollection<IIgnite> FilterGridsNodes(IEnumerable<object> consistentIds = null)
        {
            consistentIds ??= _topConsistentIds;
            
            return Ignition.GetAll().Where(g => consistentIds.Contains(GetConsistentId(g))).ToList();
        }

        private void StartClient(bool enablePartitionAwareness)
        {
            Client?.Dispose();
            Client = Ignition.StartClient(new IgniteClientConfiguration(GetClientConfiguration())
            {
                EnablePartitionAwareness = enablePartitionAwareness
            });
        }

        /// <summary>
        /// Test node filter.
        /// </summary>
        [Serializable]
        private class NodeConsistentIdFilter : IClusterNodeFilter
        {
            /** */
            private readonly IList<object> _ids;

            /** */
            internal NodeConsistentIdFilter(IEnumerable<object> ids)
            {
                // To be serialized.
                _ids = ids.ToList();
            }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return _ids.Contains(node.ConsistentId);
            }
        }
    }
}
