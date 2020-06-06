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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client with old server versions.
    /// Differs from <see cref="ClientProtocolCompatibilityTest"/>:
    /// here we actually download and run old Ignite versions instead of changing the protocol version in handshake.
    /// </summary>
    [TestFixture(JavaServer.GroupIdIgnite, "2.4.0", 0)]
    [TestFixture(JavaServer.GroupIdIgnite, "2.5.0", 1)]
    [TestFixture(JavaServer.GroupIdIgnite, "2.6.0", 1)]
    [TestFixture(JavaServer.GroupIdIgnite, "2.7.6", 2)]
    [TestFixture(JavaServer.GroupIdIgnite, "2.8.0", 6)]
    [Category(TestUtils.CategoryIntensive)]
    public class ClientServerCompatibilityTest
    {
        /** */
        private readonly string _groupId;
        
        /** */
        private readonly string _serverVersion;
        
        /** */
        private readonly ClientProtocolVersion _clientProtocolVersion;

        /** Server node holder. */
        private IDisposable _server;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientServerCompatibilityTest"/>.
        /// </summary>
        public ClientServerCompatibilityTest(string groupId, string serverVersion, int clientProtocolVersion)
        {
            _groupId = groupId;
            _serverVersion = serverVersion;
            _clientProtocolVersion = new ClientProtocolVersion(1, (short) clientProtocolVersion, 0);
        }

        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _server = JavaServer.Start(_groupId, _serverVersion);
        }

        /// <summary>
        /// Tears down the test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            _server.Dispose();
        }

        /// <summary>
        /// Tests that basic cache operations work on all versions.
        /// </summary>
        [Test]
        public void TestCacheOperationsAreSupportedOnAllVersions()
        {
            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                cache.Put(1, 10);
                Assert.AreEqual(10, cache.Get(1));
            }
        }

        /// <summary>
        /// Tests that cluster operations throw proper exception on older server versions.
        /// </summary>
        [Test]
        public void TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150()
        {
            if (_clientProtocolVersion >= ClientSocket.Ver150)
            {
                return;
            }
            
            using (var client = StartClient())
            {
                ClientProtocolCompatibilityTest.TestClusterOperationsThrowCorrectExceptionOnVersionsOlderThan150(
                    client, _clientProtocolVersion.ToString());
            }
        }
        
        /// <summary>
        /// Tests that cluster group operations throw proper exception on older server versions.
        /// </summary>
        [Test]
        public void TestClusterGroupOperationsThrowCorrectExceptionWhenFeatureIsMissing()
        {
            using (var client = StartClient())
            {
                // ReSharper disable once AccessToDisposedClosure
                ClientProtocolCompatibilityTest.AssertNotSupportedFeatureOperation(
                    () => client.GetCluster().ForServers().GetNodes(), 
                    ClientBitmaskFeature.ClusterGroups, 
                    ClientOp.ClusterGroupGetNodeIds);
            }
        }
        
        /// <summary>
        /// Tests that compute operations throw proper exception on older server versions.
        /// </summary>
        [Test]
        public void TestComputeOperationsThrowCorrectExceptionWhenFeatureIsMissing()
        {
            using (var client = StartClient())
            {
                // ReSharper disable once AccessToDisposedClosure
                ClientProtocolCompatibilityTest.AssertNotSupportedFeatureOperation(
                    () => client.GetCompute().ExecuteJavaTask<int>("t", null), 
                    ClientBitmaskFeature.ExecuteTaskByName, 
                    ClientOp.ComputeTaskExecute);
            }
        }
        
        /// <summary>
        /// Tests that partition awareness disables automatically on older server versions.
        /// </summary>
        [Test]
        public void TestPartitionAwarenessDisablesAutomaticallyOnVersionsOlderThan140()
        {
            using (var client = StartClient())
            {
                var expectedPartitionAwareness = _clientProtocolVersion >= ClientSocket.Ver140;
                Assert.AreEqual(expectedPartitionAwareness, client.GetConfiguration().EnablePartitionAwareness);
                
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                cache.Put(1, 2);
                Assert.AreEqual(2, cache.Get(1));
            }
        }
        
        /// <summary>
        /// Tests that WithExpiryPolicy throws proper exception on older server versions.
        /// </summary>
        [Test]
        public void TestWithExpiryPolicyThrowCorrectExceptionOnVersionsOlderThan150()
        {
            if (_clientProtocolVersion >= ClientSocket.Ver150)
            {
                return;
            }
            
            using (var client = StartClient())
            {
                var cache = client.GetOrCreateCache<int, int>(TestContext.CurrentContext.Test.Name);
                var cacheWithExpiry = cache.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromSeconds(1), null, null));

                ClientProtocolCompatibilityTest.AssertNotSupportedOperation(
                    () => cacheWithExpiry.Put(1, 2), _clientProtocolVersion.ToString(), "WithExpiryPolicy");
            }
        }

        /// <summary>
        /// Tests that server-side configured expiry policy works on all client versions.
        /// </summary>
        [Test]
        public void TestServerSideExpiryPolicyWorksOnAllVersions()
        {
            using (var client = StartClient())
            {
                var cache = client.GetCache<int, int>("twoSecondCache");
                
                cache.Put(1, 2);
                Assert.True(cache.ContainsKey(1));
                
                Thread.Sleep(TimeSpan.FromSeconds(2.1));
                Assert.False(cache.ContainsKey(1));
            }
        }

        /// <summary>
        /// Tests that CreateCache with all config properties customized works on all versions. 
        /// </summary>
        [Test]
        public void TestCreateCacheWithFullConfigWorksOnAllVersions()
        {
            using (var client = StartClient())
            {
                var cache = client.CreateCache<int, Person>(GetFullCacheConfiguration());
                
                cache.Put(1, new Person(2));
                
                Assert.AreEqual(2, cache.Get(1).Id);
                Assert.AreEqual("Person 2", cache[1].Name);
            }
        }

        /// <summary>
        /// Starts the client.
        /// </summary>
        private static IIgniteClient StartClient()
        {
            var cfg = new IgniteClientConfiguration(JavaServer.GetClientConfiguration())
            {
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace}),
                EnablePartitionAwareness = true
            };
            
            return Ignition.StartClient(cfg);
        }

        /// <summary>
        /// Gets the cache config.
        /// </summary>
        private static CacheClientConfiguration GetFullCacheConfiguration()
        {
            return new CacheClientConfiguration
            {
                Name = Guid.NewGuid().ToString(),
                Backups = 3,
                AtomicityMode = CacheAtomicityMode.Transactional,
                CacheMode = CacheMode.Partitioned,
                EagerTtl = false,
                EnableStatistics = true,
                GroupName = Guid.NewGuid().ToString(),
                KeyConfiguration = new[]
                {
                    new CacheKeyConfiguration
                    {
                        TypeName = typeof(Person).FullName,
                        AffinityKeyFieldName = "Name"
                    }
                },
                LockTimeout =TimeSpan.FromSeconds(5),
                QueryEntities = new[]
                {
                    new QueryEntity(typeof(int), typeof(Person))
                    {
                        Aliases = new[]
                        {
                            new QueryAlias("Person.Name", "PName") 
                        }
                    }
                },
                QueryParallelism = 7,
                RebalanceDelay = TimeSpan.FromSeconds(1.5),
                RebalanceMode = CacheRebalanceMode.Sync,
                RebalanceOrder = 25,
                RebalanceThrottle = TimeSpan.FromSeconds(2.3),
                RebalanceTimeout = TimeSpan.FromSeconds(42),
                SqlSchema = Guid.NewGuid().ToString(),
                CopyOnRead = false,
                DataRegionName = DataStorageConfiguration.DefaultDataRegionName,
                ExpiryPolicyFactory = new TestExpiryPolicyFactory(),
                OnheapCacheEnabled = true,
                PartitionLossPolicy = PartitionLossPolicy.ReadWriteAll,
                ReadFromBackup = false,
                RebalanceBatchSize = 100000,
                SqlEscapeAll = true,
                WriteSynchronizationMode = CacheWriteSynchronizationMode.FullAsync,
                MaxConcurrentAsyncOperations = 123,
                MaxQueryIteratorsCount = 17,
                QueryDetailMetricsSize = 50,
                RebalanceBatchesPrefetchCount = 4,
                SqlIndexMaxInlineSize = 200000
            };
        }
        
        /** */
        private class TestExpiryPolicyFactory : IFactory<IExpiryPolicy>
        {
            /** */
            public IExpiryPolicy CreateInstance()
            {
                return new ExpiryPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
            }
        }
    }
}