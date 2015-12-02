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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Resource;
    using Apache.Ignite.Core.Services;
    using NUnit.Framework;

    /// <summary>
    /// Services tests.
    /// </summary>
    public class ServicesTest
    {
        /** */
        private const string SvcName = "Service1";

        /** */
        private const string CacheName = "cache1";

        /** */
        private const int AffKey = 25;

        /** */
        protected IIgnite Grid1;

        /** */
        protected IIgnite Grid2;

        /** */
        protected IIgnite Grid3;

        /** */
        protected IIgnite[] Grids;

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();
            EventsTestHelper.ListenResult = true;
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                Services.Cancel(SvcName);

                TestUtils.AssertHandleRegistryIsEmpty(1000, Grid1, Grid2, Grid3);
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
            finally
            {
                EventsTestHelper.AssertFailures();

                if (TestContext.CurrentContext.Test.Name.StartsWith("TestEventTypes"))
                    StopGrids(); // clean events for other tests
            }
        }

        /// <summary>
        /// Tests deployment.
        /// </summary>
        [Test]
        public void TestDeploy([Values(true, false)] bool binarizable)
        {
            var cfg = new ServiceConfiguration
            {
                Name = SvcName,
                MaxPerNodeCount = 3,
                TotalCount = 3,
                NodeFilter = new NodeFilter {NodeId = Grid1.GetCluster().GetLocalNode().Id},
                Service = binarizable ? new TestIgniteServiceBinarizable() : new TestIgniteServiceSerializable()
            };

            Services.Deploy(cfg);

            CheckServiceStarted(Grid1, 3);
        }

        /// <summary>
        /// Tests cluster singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployClusterSingleton()
        {
            var svc = new TestIgniteServiceSerializable();

            Services.DeployClusterSingleton(SvcName, svc);

            var svc0 = Services.GetServiceProxy<ITestIgniteService>(SvcName);

            // Check that only one node has the service.
            foreach (var grid in Grids)
            {
                if (grid.GetCluster().GetLocalNode().Id == svc0.NodeId)
                    CheckServiceStarted(grid);
                else
                    Assert.IsNull(grid.GetServices().GetService<TestIgniteServiceSerializable>(SvcName));
            }
        }

        /// <summary>
        /// Tests node singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployNodeSingleton()
        {
            var svc = new TestIgniteServiceSerializable();

            Services.DeployNodeSingleton(SvcName, svc);

            Assert.AreEqual(1, Grid1.GetServices().GetServices<ITestIgniteService>(SvcName).Count);
            Assert.AreEqual(1, Grid2.GetServices().GetServices<ITestIgniteService>(SvcName).Count);
            Assert.AreEqual(1, Grid3.GetServices().GetServices<ITestIgniteService>(SvcName).Count);
        }

        /// <summary>
        /// Tests key affinity singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployKeyAffinitySingleton()
        {
            var svc = new TestIgniteServiceBinarizable();

            Services.DeployKeyAffinitySingleton(SvcName, svc, CacheName, AffKey);

            var affNode = Grid1.GetAffinity(CacheName).MapKeyToNode(AffKey);

            var prx = Services.GetServiceProxy<ITestIgniteService>(SvcName);

            Assert.AreEqual(affNode.Id, prx.NodeId);
        }

        /// <summary>
        /// Tests key affinity singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployKeyAffinitySingletonBinarizable()
        {
            var services = Services.WithKeepBinary();

            var svc = new TestIgniteServiceBinarizable();

            var affKey = new BinarizableObject {Val = AffKey};

            services.DeployKeyAffinitySingleton(SvcName, svc, CacheName, affKey);

            var prx = services.GetServiceProxy<ITestIgniteService>(SvcName);

            Assert.IsTrue(prx.Initialized);
        }

        /// <summary>
        /// Tests multiple deployment.
        /// </summary>
        [Test]
        public void TestDeployMultiple()
        {
            var svc = new TestIgniteServiceSerializable();

            Services.DeployMultiple(SvcName, svc, Grids.Length * 5, 5);

            foreach (var grid in Grids)
                CheckServiceStarted(grid, 5);
        }

        /// <summary>
        /// Tests cancellation.
        /// </summary>
        [Test]
        public void TestCancel()
        {
            for (var i = 0; i < 10; i++)
            {
                Services.DeployNodeSingleton(SvcName + i, new TestIgniteServiceBinarizable());
                Assert.IsNotNull(Services.GetService<ITestIgniteService>(SvcName + i));
            }

            Services.Cancel(SvcName + 0);
            Services.Cancel(SvcName + 1);

            Assert.IsNull(Services.GetService<ITestIgniteService>(SvcName + 0));
            Assert.IsNull(Services.GetService<ITestIgniteService>(SvcName + 1));

            for (var i = 2; i < 10; i++)
                Assert.IsNotNull(Services.GetService<ITestIgniteService>(SvcName + i));

            Services.CancelAll();

            for (var i = 0; i < 10; i++)
                Assert.IsNull(Services.GetService<ITestIgniteService>(SvcName + i));
        }

        /// <summary>
        /// Tests service proxy.
        /// </summary>
        [Test]
        public void TestGetServiceProxy([Values(true, false)] bool binarizable)
        {
            // Test proxy without a service
            var prx = Services.GetServiceProxy<ITestIgniteService>(SvcName);

            Assert.IsTrue(prx != null);

            var ex = Assert.Throws<ServiceInvocationException>(() => Assert.IsTrue(prx.Initialized)).InnerException;
            Assert.AreEqual("Failed to find deployed service: " + SvcName, ex.Message);

            // Deploy to grid2 & grid3
            var svc = binarizable
                ? new TestIgniteServiceBinarizable {TestProperty = 17}
                : new TestIgniteServiceSerializable {TestProperty = 17};

            Grid1.GetCluster().ForNodeIds(Grid2.GetCluster().GetLocalNode().Id, Grid3.GetCluster().GetLocalNode().Id).GetServices()
                .DeployNodeSingleton(SvcName,
                    svc);

            // Make sure there is no local instance on grid1
            Assert.IsNull(Services.GetService<ITestIgniteService>(SvcName));

            // Get proxy
            prx = Services.GetServiceProxy<ITestIgniteService>(SvcName);

            // Check proxy properties
            Assert.IsNotNull(prx);
            Assert.AreEqual(prx.GetType(), svc.GetType());
            Assert.AreEqual(prx.ToString(), svc.ToString());
            Assert.AreEqual(17, prx.TestProperty);
            Assert.IsTrue(prx.Initialized);
            Assert.IsTrue(prx.Executed);
            Assert.IsFalse(prx.Cancelled);
            Assert.AreEqual(SvcName, prx.LastCallContextName);

            // Check err method
            Assert.Throws<ServiceInvocationException>(() => prx.ErrMethod(123));

            // Check local scenario (proxy should not be created for local instance)
            Assert.IsTrue(ReferenceEquals(Grid2.GetServices().GetService<ITestIgniteService>(SvcName),
                Grid2.GetServices().GetServiceProxy<ITestIgniteService>(SvcName)));

            // Check sticky = false: call multiple times, check that different nodes get invoked
            var invokedIds = Enumerable.Range(1, 100).Select(x => prx.NodeId).Distinct().ToList();
            Assert.AreEqual(2, invokedIds.Count);

            // Check sticky = true: all calls should be to the same node
            prx = Services.GetServiceProxy<ITestIgniteService>(SvcName, true);
            invokedIds = Enumerable.Range(1, 100).Select(x => prx.NodeId).Distinct().ToList();
            Assert.AreEqual(1, invokedIds.Count);

            // Proxy does not work for cancelled service.
            Services.CancelAll();

            Assert.Throws<ServiceInvocationException>(() => { Assert.IsTrue(prx.Cancelled); });
        }

        /// <summary>
        /// Tests the duck typing: proxy interface can be different from actual service interface, 
        /// only called method signature should be compatible.
        /// </summary>
        [Test]
        public void TestDuckTyping([Values(true, false)] bool local)
        {
            var svc = new TestIgniteServiceBinarizable {TestProperty = 33};

            // Deploy locally or to the remote node
            var nodeId = (local ? Grid1 : Grid2).GetCluster().GetLocalNode().Id;
            
            var cluster = Grid1.GetCluster().ForNodeIds(nodeId);

            cluster.GetServices().DeployNodeSingleton(SvcName, svc);

            // Get proxy
            var prx = Services.GetServiceProxy<ITestIgniteServiceProxyInterface>(SvcName);

            // NodeId signature is the same as in service
            Assert.AreEqual(nodeId, prx.NodeId);
            
            // Method signature is different from service signature (object -> object), but is compatible.
            Assert.AreEqual(15, prx.Method(15));

            // TestProperty is object in proxy and int in service, getter works..
            Assert.AreEqual(33, prx.TestProperty);

            // .. but setter does not
            var ex = Assert.Throws<ServiceInvocationException>(() => { prx.TestProperty = new object(); });
            Assert.AreEqual("Object of type 'System.Object' cannot be converted to type 'System.Int32'.",
                ex.InnerException.Message);
        }

        /// <summary>
        /// Tests service descriptors.
        /// </summary>
        [Test]
        public void TestServiceDescriptors()
        {
            Services.DeployKeyAffinitySingleton(SvcName, new TestIgniteServiceSerializable(), CacheName, 1);

            var descriptors = Services.GetServiceDescriptors();

            Assert.AreEqual(1, descriptors.Count);

            var desc = descriptors.Single();

            Assert.AreEqual(SvcName, desc.Name);
            Assert.AreEqual(CacheName, desc.CacheName);
            Assert.AreEqual(1, desc.AffinityKey);
            Assert.AreEqual(1, desc.MaxPerNodeCount);
            Assert.AreEqual(1, desc.TotalCount);
            Assert.AreEqual(typeof(TestIgniteServiceSerializable), desc.Type);
            Assert.AreEqual(Grid1.GetCluster().GetLocalNode().Id, desc.OriginNodeId);

            var top = desc.TopologySnapshot;
            var prx = Services.GetServiceProxy<ITestIgniteService>(SvcName);
            
            Assert.AreEqual(1, top.Count);
            Assert.AreEqual(prx.NodeId, top.Keys.Single());
            Assert.AreEqual(1, top.Values.Single());
        }

        /// <summary>
        /// Tests the client binary flag.
        /// </summary>
        [Test]
        public void TestWithKeepBinaryClient()
        {
            var svc = new TestIgniteServiceBinarizable();

            // Deploy to grid2
            Grid1.GetCluster().ForNodeIds(Grid2.GetCluster().GetLocalNode().Id).GetServices().WithKeepBinary()
                .DeployNodeSingleton(SvcName, svc);

            // Get proxy
            var prx = Services.WithKeepBinary().GetServiceProxy<ITestIgniteService>(SvcName);

            var obj = new BinarizableObject {Val = 11};

            var res = (IBinaryObject) prx.Method(obj);
            Assert.AreEqual(11, res.Deserialize<BinarizableObject>().Val);

            res = (IBinaryObject) prx.Method(Grid1.GetBinary().ToBinary<IBinaryObject>(obj));
            Assert.AreEqual(11, res.Deserialize<BinarizableObject>().Val);
        }
        
        /// <summary>
        /// Tests the server binary flag.
        /// </summary>
        [Test]
        public void TestWithKeepBinaryServer()
        {
            var svc = new TestIgniteServiceBinarizable();

            // Deploy to grid2
            Grid1.GetCluster().ForNodeIds(Grid2.GetCluster().GetLocalNode().Id).GetServices().WithServerKeepBinary()
                .DeployNodeSingleton(SvcName, svc);

            // Get proxy
            var prx = Services.WithServerKeepBinary().GetServiceProxy<ITestIgniteService>(SvcName);

            var obj = new BinarizableObject { Val = 11 };

            var res = (BinarizableObject) prx.Method(obj);
            Assert.AreEqual(11, res.Val);

            res = (BinarizableObject)prx.Method(Grid1.GetBinary().ToBinary<IBinaryObject>(obj));
            Assert.AreEqual(11, res.Val);
        }

        /// <summary>
        /// Tests server and client binary flag.
        /// </summary>
        [Test]
        public void TestWithKeepBinaryBoth()
        {
            var svc = new TestIgniteServiceBinarizable();

            // Deploy to grid2
            Grid1.GetCluster().ForNodeIds(Grid2.GetCluster().GetLocalNode().Id).GetServices().WithKeepBinary().WithServerKeepBinary()
                .DeployNodeSingleton(SvcName, svc);

            // Get proxy
            var prx = Services.WithKeepBinary().WithServerKeepBinary().GetServiceProxy<ITestIgniteService>(SvcName);

            var obj = new BinarizableObject { Val = 11 };

            var res = (IBinaryObject)prx.Method(obj);
            Assert.AreEqual(11, res.Deserialize<BinarizableObject>().Val);

            res = (IBinaryObject)prx.Method(Grid1.GetBinary().ToBinary<IBinaryObject>(obj));
            Assert.AreEqual(11, res.Deserialize<BinarizableObject>().Val);
        }

        /// <summary>
        /// Tests exception in Initialize.
        /// </summary>
        [Test]
        public void TestInitException()
        {
            var svc = new TestIgniteServiceSerializable { ThrowInit = true };

            var ex = Assert.Throws<IgniteException>(() => Services.DeployMultiple(SvcName, svc, Grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestIgniteServiceSerializable>(SvcName);

            Assert.IsNull(svc0);
        }

        /// <summary>
        /// Tests exception in Execute.
        /// </summary>
        [Test]
        public void TestExecuteException()
        {
            var svc = new TestIgniteServiceSerializable { ThrowExecute = true };

            Services.DeployMultiple(SvcName, svc, Grids.Length, 1);

            var svc0 = Services.GetService<TestIgniteServiceSerializable>(SvcName);

            // Execution failed, but service exists.
            Assert.IsNotNull(svc0);
            Assert.IsFalse(svc0.Executed);
        }

        /// <summary>
        /// Tests exception in Cancel.
        /// </summary>
        [Test]
        public void TestCancelException()
        {
            var svc = new TestIgniteServiceSerializable { ThrowCancel = true };

            Services.DeployMultiple(SvcName, svc, Grids.Length, 1);

            CheckServiceStarted(Grid1);

            Services.CancelAll();

            // Cancellation failed, but service is removed.
            foreach (var grid in Grids)
                Assert.IsNull(grid.GetServices().GetService<ITestIgniteService>(SvcName));
        }

        [Test]
        public void TestMarshalExceptionOnRead()
        {
            var svc = new TestIgniteServiceBinarizableErr();

            var ex = Assert.Throws<IgniteException>(() => Services.DeployMultiple(SvcName, svc, Grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestIgniteServiceSerializable>(SvcName);

            Assert.IsNull(svc0);
        }

        [Test]
        public void TestMarshalExceptionOnWrite()
        {
            var svc = new TestIgniteServiceBinarizableErr {ThrowOnWrite = true};

            var ex = Assert.Throws<Exception>(() => Services.DeployMultiple(SvcName, svc, Grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestIgniteServiceSerializable>(SvcName);

            Assert.IsNull(svc0);
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (Grid1 != null)
                return;

            Grid1 = Ignition.Start(Configuration("config\\compute\\compute-grid1.xml"));
            Grid2 = Ignition.Start(Configuration("config\\compute\\compute-grid2.xml"));
            Grid3 = Ignition.Start(Configuration("config\\compute\\compute-grid3.xml"));

            Grids = new[] { Grid1, Grid2, Grid3 };
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            Grid1 = Grid2 = Grid3 = null;
            Grids = null;

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Checks that service has started on specified grid.
        /// </summary>
        private static void CheckServiceStarted(IIgnite grid, int count = 1)
        {
            var services = grid.GetServices().GetServices<TestIgniteServiceSerializable>(SvcName);

            Assert.AreEqual(count, services.Count);

            var svc = services.First();

            Assert.IsNotNull(svc);

            Assert.IsTrue(svc.Initialized);

            Thread.Sleep(100);  // Service runs in a separate thread, wait for it to execute.

            Assert.IsTrue(svc.Executed);
            Assert.IsFalse(svc.Cancelled);

            Assert.AreEqual(grid.GetCluster().GetLocalNode().Id, svc.NodeId);
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private static IgniteConfiguration Configuration(string springConfigUrl)
        {
            return new IgniteConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(TestIgniteServiceBinarizable)),
                        new BinaryTypeConfiguration(typeof(TestIgniteServiceBinarizableErr)),
                        new BinaryTypeConfiguration(typeof(BinarizableObject))
                    }
                }
            };
        }

        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return Grid1.GetServices(); }
        }

        /// <summary>
        /// Test service interface for proxying.
        /// </summary>
        private interface ITestIgniteService
        {
            int TestProperty { get; set; }

            /** */
            bool Initialized { get; }

            /** */
            bool Cancelled { get; }

            /** */
            bool Executed { get; }

            /** */
            Guid NodeId { get; }

            /** */
            string LastCallContextName { get; }

            /** */
            object Method(object arg);

            /** */
            object ErrMethod(object arg);
        }

        /// <summary>
        /// Test service interface for proxy usage.
        /// Has some of the original interface members with different signatures.
        /// </summary>
        private interface ITestIgniteServiceProxyInterface
        {
            /** */
            Guid NodeId { get; }

            /** */
            object TestProperty { get; set; }

            /** */
            int Method(int arg);
        }

        #pragma warning disable 649

        /// <summary>
        /// Test serializable service.
        /// </summary>
        [Serializable]
        private class TestIgniteServiceSerializable : IService, ITestIgniteService
        {
            /** */
            [InstanceResource]
            private IIgnite _grid;

            /** <inheritdoc /> */
            public int TestProperty { get; set; }

            /** <inheritdoc /> */
            public bool Initialized { get; private set; }

            /** <inheritdoc /> */
            public bool Cancelled { get; private set; }

            /** <inheritdoc /> */
            public bool Executed { get; private set; }

            /** <inheritdoc /> */
            public Guid NodeId
            {
                get { return _grid.GetCluster().GetLocalNode().Id; }
            }

            /** <inheritdoc /> */
            public string LastCallContextName { get; private set; }

            /** */
            public bool ThrowInit { get; set; }

            /** */
            public bool ThrowExecute { get; set; }

            /** */
            public bool ThrowCancel { get; set; }

            /** */
            public object Method(object arg)
            {
                return arg;
            }

            /** */
            public object ErrMethod(object arg)
            {
                throw new ArgumentNullException("arg", "ExpectedException");
            }

            /** <inheritdoc /> */
            public void Init(IServiceContext context)
            {
                if (ThrowInit) 
                    throw new Exception("Expected exception");

                CheckContext(context);

                Assert.IsFalse(context.IsCancelled);
                Initialized = true;
            }

            /** <inheritdoc /> */
            public void Execute(IServiceContext context)
            {
                if (ThrowExecute)
                    throw new Exception("Expected exception");

                CheckContext(context);

                Assert.IsFalse(context.IsCancelled);
                Assert.IsTrue(Initialized);
                Assert.IsFalse(Cancelled);

                Executed = true;
            }

            /** <inheritdoc /> */
            public void Cancel(IServiceContext context)
            {
                if (ThrowCancel)
                    throw new Exception("Expected exception");

                CheckContext(context);

                Assert.IsTrue(context.IsCancelled);

                Cancelled = true;
            }

            /// <summary>
            /// Checks the service context.
            /// </summary>
            private void CheckContext(IServiceContext context)
            {
                LastCallContextName = context.Name;

                if (context.AffinityKey != null && !(context.AffinityKey is int))
                {
                    var binaryObj = context.AffinityKey as IBinaryObject;
                    
                    var key = binaryObj != null
                        ? binaryObj.Deserialize<BinarizableObject>()
                        : (BinarizableObject) context.AffinityKey;

                    Assert.AreEqual(AffKey, key.Val);
                }

                Assert.IsNotNull(_grid);

                Assert.IsTrue(context.Name.StartsWith(SvcName));
                Assert.AreNotEqual(Guid.Empty, context.ExecutionId);
            }
        }

        /// <summary>
        /// Test binary service.
        /// </summary>
        private class TestIgniteServiceBinarizable : TestIgniteServiceSerializable, IBinarizable
        {
            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("TestProp", TestProperty);
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                TestProperty = reader.ReadInt("TestProp");
            }
        }

        /// <summary>
        /// Test binary service with exceptions in marshalling.
        /// </summary>
        private class TestIgniteServiceBinarizableErr : TestIgniteServiceSerializable, IBinarizable
        {
            /** */
            public bool ThrowOnWrite { get; set; }

            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("TestProp", TestProperty);
                
                if (ThrowOnWrite)
                    throw new Exception("Expected exception");
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                TestProperty = reader.ReadInt("TestProp");
                
                throw new Exception("Expected exception");
            }
        }

        /// <summary>
        /// Test node filter.
        /// </summary>
        [Serializable]
        private class NodeFilter : IClusterNodeFilter
        {
            /// <summary>
            /// Gets or sets the node identifier.
            /// </summary>
            public Guid NodeId { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return node.Id == NodeId;
            }
        }

        /// <summary>
        /// Binary object.
        /// </summary>
        private class BinarizableObject
        {
            public int Val { get; set; }
        }
    }
}
