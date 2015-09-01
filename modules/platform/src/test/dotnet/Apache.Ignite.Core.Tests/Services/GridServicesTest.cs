/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Portable;
    using GridGain.Resource;
    using GridGain.Services;

    using NUnit.Framework;

    /// <summary>
    /// Services tests.
    /// </summary>
    public class GridServicesTest
    {
        /** */
        private const string SVC_NAME = "Service1";

        /** */
        private const string CACHE_NAME = "cache1";

        /** */
        private const int AFF_KEY = 25;

        /** */
        protected IGrid grid1;

        /** */
        protected IGrid grid2;

        /** */
        protected IGrid grid3;

        /** */
        protected IGrid[] grids;

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
            EventsTestHelper.listenResult = true;
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                Services.Cancel(SVC_NAME);

                GridTestUtils.AssertHandleRegistryIsEmpty(1000, grid1, grid2, grid3);
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
        public void TestDeploy([Values(true, false)] bool portable)
        {
            var cfg = new ServiceConfiguration
            {
                Name = SVC_NAME,
                MaxPerNodeCount = 3,
                TotalCount = 3,
                NodeFilter = new NodeFilter {NodeId = grid1.Cluster.LocalNode.Id},
                Service = portable ? new TestGridServicePortable() : new TestGridServiceSerializable()
            };

            Services.Deploy(cfg);

            CheckServiceStarted(grid1, 3);
        }

        /// <summary>
        /// Tests cluster singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployClusterSingleton()
        {
            var svc = new TestGridServiceSerializable();

            Services.DeployClusterSingleton(SVC_NAME, svc);

            var svc0 = Services.GetServiceProxy<ITestGridService>(SVC_NAME);

            // Check that only one node has the service.
            foreach (var grid in grids)
            {
                if (grid.Cluster.LocalNode.Id == svc0.NodeId)
                    CheckServiceStarted(grid);
                else
                    Assert.IsNull(grid.Services().GetService<TestGridServiceSerializable>(SVC_NAME));
            }
        }

        /// <summary>
        /// Tests node singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployNodeSingleton()
        {
            var svc = new TestGridServiceSerializable();

            Services.DeployNodeSingleton(SVC_NAME, svc);

            Assert.AreEqual(1, grid1.Services().GetServices<ITestGridService>(SVC_NAME).Count);
            Assert.AreEqual(1, grid2.Services().GetServices<ITestGridService>(SVC_NAME).Count);
            Assert.AreEqual(1, grid3.Services().GetServices<ITestGridService>(SVC_NAME).Count);
        }

        /// <summary>
        /// Tests key affinity singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployKeyAffinitySingleton()
        {
            var svc = new TestGridServicePortable();

            Services.DeployKeyAffinitySingleton(SVC_NAME, svc, CACHE_NAME, AFF_KEY);

            var affNode = grid1.Affinity(CACHE_NAME).MapKeyToNode(AFF_KEY);

            var prx = Services.GetServiceProxy<ITestGridService>(SVC_NAME);

            Assert.AreEqual(affNode.Id, prx.NodeId);
        }

        /// <summary>
        /// Tests key affinity singleton deployment.
        /// </summary>
        [Test]
        public void TestDeployKeyAffinitySingletonPortable()
        {
            var services = Services.WithKeepPortable();

            var svc = new TestGridServicePortable();

            var affKey = new PortableObject {Val = AFF_KEY};

            services.DeployKeyAffinitySingleton(SVC_NAME, svc, CACHE_NAME, affKey);

            var prx = services.GetServiceProxy<ITestGridService>(SVC_NAME);

            Assert.IsTrue(prx.Initialized);
        }

        /// <summary>
        /// Tests multiple deployment.
        /// </summary>
        [Test]
        public void TestDeployMultiple()
        {
            var svc = new TestGridServiceSerializable();

            Services.DeployMultiple(SVC_NAME, svc, grids.Length * 5, 5);

            foreach (var grid in grids)
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
                Services.DeployNodeSingleton(SVC_NAME + i, new TestGridServicePortable());
                Assert.IsNotNull(Services.GetService<ITestGridService>(SVC_NAME + i));
            }

            Services.Cancel(SVC_NAME + 0);
            Services.Cancel(SVC_NAME + 1);

            Assert.IsNull(Services.GetService<ITestGridService>(SVC_NAME + 0));
            Assert.IsNull(Services.GetService<ITestGridService>(SVC_NAME + 1));

            for (var i = 2; i < 10; i++)
                Assert.IsNotNull(Services.GetService<ITestGridService>(SVC_NAME + i));

            Services.CancelAll();

            for (var i = 0; i < 10; i++)
                Assert.IsNull(Services.GetService<ITestGridService>(SVC_NAME + i));
        }

        /// <summary>
        /// Tests service proxy.
        /// </summary>
        [Test]
        public void TestGetServiceProxy([Values(true, false)] bool portable)
        {
            // Test proxy without a service
            var prx = Services.GetServiceProxy<ITestGridService>(SVC_NAME);

            Assert.IsTrue(prx != null);

            var ex = Assert.Throws<ServiceInvocationException>(() => Assert.IsTrue(prx.Initialized)).InnerException;
            Assert.AreEqual("Failed to find deployed service: " + SVC_NAME, ex.Message);

            // Deploy to grid2 & grid3
            var svc = portable
                ? new TestGridServicePortable {TestProperty = 17}
                : new TestGridServiceSerializable {TestProperty = 17};

            grid1.Cluster.ForNodeIds(grid2.Cluster.LocalNode.Id, grid3.Cluster.LocalNode.Id).Services()
                .DeployNodeSingleton(SVC_NAME,
                    svc);

            // Make sure there is no local instance on grid1
            Assert.IsNull(Services.GetService<ITestGridService>(SVC_NAME));

            // Get proxy
            prx = Services.GetServiceProxy<ITestGridService>(SVC_NAME);

            // Check proxy properties
            Assert.IsNotNull(prx);
            Assert.AreEqual(prx.GetType(), svc.GetType());
            Assert.AreEqual(prx.ToString(), svc.ToString());
            Assert.AreEqual(17, prx.TestProperty);
            Assert.IsTrue(prx.Initialized);
            Assert.IsTrue(prx.Executed);
            Assert.IsFalse(prx.Cancelled);
            Assert.AreEqual(SVC_NAME, prx.LastCallContextName);

            // Check err method
            Assert.Throws<ServiceInvocationException>(() => prx.ErrMethod(123));

            // Check local scenario (proxy should not be created for local instance)
            Assert.IsTrue(ReferenceEquals(grid2.Services().GetService<ITestGridService>(SVC_NAME),
                grid2.Services().GetServiceProxy<ITestGridService>(SVC_NAME)));

            // Check sticky = false: call multiple times, check that different nodes get invoked
            var invokedIds = Enumerable.Range(1, 100).Select(x => prx.NodeId).Distinct().ToList();
            Assert.AreEqual(2, invokedIds.Count);

            // Check sticky = true: all calls should be to the same node
            prx = Services.GetServiceProxy<ITestGridService>(SVC_NAME, true);
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
            var svc = new TestGridServicePortable {TestProperty = 33};

            // Deploy locally or to the remote node
            var nodeId = (local ? grid1 : grid2).Cluster.LocalNode.Id;
            
            var cluster = grid1.Cluster.ForNodeIds(nodeId);

            cluster.Services().DeployNodeSingleton(SVC_NAME, svc);

            // Get proxy
            var prx = Services.GetServiceProxy<ITestGridServiceProxyInterface>(SVC_NAME);

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
            Services.DeployKeyAffinitySingleton(SVC_NAME, new TestGridServiceSerializable(), CACHE_NAME, 1);

            var descriptors = Services.GetServiceDescriptors();

            Assert.AreEqual(1, descriptors.Count);

            var desc = descriptors.Single();

            Assert.AreEqual(SVC_NAME, desc.Name);
            Assert.AreEqual(CACHE_NAME, desc.CacheName);
            Assert.AreEqual(1, desc.AffinityKey);
            Assert.AreEqual(1, desc.MaxPerNodeCount);
            Assert.AreEqual(1, desc.TotalCount);
            Assert.AreEqual(typeof(TestGridServiceSerializable), desc.Type);
            Assert.AreEqual(grid1.Cluster.LocalNode.Id, desc.OriginNodeId);

            var top = desc.TopologySnapshot;
            var prx = Services.GetServiceProxy<ITestGridService>(SVC_NAME);
            
            Assert.AreEqual(1, top.Count);
            Assert.AreEqual(prx.NodeId, top.Keys.Single());
            Assert.AreEqual(1, top.Values.Single());
        }

        /// <summary>
        /// Tests the client portable flag.
        /// </summary>
        [Test]
        public void TestWithKeepPortableClient()
        {
            var svc = new TestGridServicePortable();

            // Deploy to grid2
            grid1.Cluster.ForNodeIds(grid2.Cluster.LocalNode.Id).Services().WithKeepPortable()
                .DeployNodeSingleton(SVC_NAME, svc);

            // Get proxy
            var prx = Services.WithKeepPortable().GetServiceProxy<ITestGridService>(SVC_NAME);

            var obj = new PortableObject {Val = 11};

            var res = (IPortableObject) prx.Method(obj);
            Assert.AreEqual(11, res.Deserialize<PortableObject>().Val);

            res = (IPortableObject) prx.Method(grid1.Portables().ToPortable<IPortableObject>(obj));
            Assert.AreEqual(11, res.Deserialize<PortableObject>().Val);
        }
        
        /// <summary>
        /// Tests the server portable flag.
        /// </summary>
        [Test]
        public void TestWithKeepPortableServer()
        {
            var svc = new TestGridServicePortable();

            // Deploy to grid2
            grid1.Cluster.ForNodeIds(grid2.Cluster.LocalNode.Id).Services().WithServerKeepPortable()
                .DeployNodeSingleton(SVC_NAME, svc);

            // Get proxy
            var prx = Services.WithServerKeepPortable().GetServiceProxy<ITestGridService>(SVC_NAME);

            var obj = new PortableObject { Val = 11 };

            var res = (PortableObject) prx.Method(obj);
            Assert.AreEqual(11, res.Val);

            res = (PortableObject)prx.Method(grid1.Portables().ToPortable<IPortableObject>(obj));
            Assert.AreEqual(11, res.Val);
        }

        /// <summary>
        /// Tests server and client portable flag.
        /// </summary>
        [Test]
        public void TestWithKeepPortableBoth()
        {
            var svc = new TestGridServicePortable();

            // Deploy to grid2
            grid1.Cluster.ForNodeIds(grid2.Cluster.LocalNode.Id).Services().WithKeepPortable().WithServerKeepPortable()
                .DeployNodeSingleton(SVC_NAME, svc);

            // Get proxy
            var prx = Services.WithKeepPortable().WithServerKeepPortable().GetServiceProxy<ITestGridService>(SVC_NAME);

            var obj = new PortableObject { Val = 11 };

            var res = (IPortableObject)prx.Method(obj);
            Assert.AreEqual(11, res.Deserialize<PortableObject>().Val);

            res = (IPortableObject)prx.Method(grid1.Portables().ToPortable<IPortableObject>(obj));
            Assert.AreEqual(11, res.Deserialize<PortableObject>().Val);
        }

        /// <summary>
        /// Tests exception in Initialize.
        /// </summary>
        [Test]
        public void TestInitException()
        {
            var svc = new TestGridServiceSerializable { ThrowInit = true };

            var ex = Assert.Throws<IgniteException>(() => Services.DeployMultiple(SVC_NAME, svc, grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestGridServiceSerializable>(SVC_NAME);

            Assert.IsNull(svc0);
        }

        /// <summary>
        /// Tests exception in Execute.
        /// </summary>
        [Test]
        public void TestExecuteException()
        {
            var svc = new TestGridServiceSerializable { ThrowExecute = true };

            Services.DeployMultiple(SVC_NAME, svc, grids.Length, 1);

            var svc0 = Services.GetService<TestGridServiceSerializable>(SVC_NAME);

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
            var svc = new TestGridServiceSerializable { ThrowCancel = true };

            Services.DeployMultiple(SVC_NAME, svc, grids.Length, 1);

            CheckServiceStarted(grid1);

            Services.CancelAll();

            // Cancellation failed, but service is removed.
            foreach (var grid in grids)
                Assert.IsNull(grid.Services().GetService<ITestGridService>(SVC_NAME));
        }

        [Test]
        public void TestMarshalExceptionOnRead()
        {
            var svc = new TestGridServicePortableErr();

            var ex = Assert.Throws<IgniteException>(() => Services.DeployMultiple(SVC_NAME, svc, grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestGridServiceSerializable>(SVC_NAME);

            Assert.IsNull(svc0);
        }

        [Test]
        public void TestMarshalExceptionOnWrite()
        {
            var svc = new TestGridServicePortableErr {ThrowOnWrite = true};

            var ex = Assert.Throws<Exception>(() => Services.DeployMultiple(SVC_NAME, svc, grids.Length, 1));
            Assert.AreEqual("Expected exception", ex.Message);

            var svc0 = Services.GetService<TestGridServiceSerializable>(SVC_NAME);

            Assert.IsNull(svc0);
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (grid1 != null)
                return;

            grid1 = GridFactory.Start(Configuration("config\\compute\\compute-grid1.xml"));
            grid2 = GridFactory.Start(Configuration("config\\compute\\compute-grid2.xml"));
            grid3 = GridFactory.Start(Configuration("config\\compute\\compute-grid3.xml"));

            grids = new[] { grid1, grid2, grid3 };
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            grid1 = grid2 = grid3 = null;
            grids = null;

            GridFactory.StopAll(true);
        }

        /// <summary>
        /// Checks that service has started on specified grid.
        /// </summary>
        private static void CheckServiceStarted(IGrid grid, int count = 1)
        {
            var services = grid.Services().GetServices<TestGridServiceSerializable>(SVC_NAME);

            Assert.AreEqual(count, services.Count);

            var svc = services.First();

            Assert.IsNotNull(svc);

            Assert.IsTrue(svc.Initialized);

            Thread.Sleep(100);  // Service runs in a separate thread, wait for it to execute.

            Assert.IsTrue(svc.Executed);
            Assert.IsFalse(svc.Cancelled);

            Assert.AreEqual(grid.Cluster.LocalNode.Id, svc.NodeId);
        }

        /// <summary>
        /// Gets the grid configuration.
        /// </summary>
        private static GridConfiguration Configuration(string springConfigUrl)
        {
            return new GridConfiguration
            {
                SpringConfigUrl = springConfigUrl,
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new List<PortableTypeConfiguration>
                    {
                        new PortableTypeConfiguration(typeof(TestGridServicePortable)),
                        new PortableTypeConfiguration(typeof(TestGridServicePortableErr)),
                        new PortableTypeConfiguration(typeof(PortableObject))
                    }
                }
            };
        }

        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return grid1.Services(); }
        }

        /// <summary>
        /// Test service interface for proxying.
        /// </summary>
        private interface ITestGridService
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
        private interface ITestGridServiceProxyInterface
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
        private class TestGridServiceSerializable : IService, ITestGridService
        {
            /** */
            [InstanceResource]
            private IGrid grid;

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
                get { return grid.Cluster.LocalNode.Id; }
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
                    var portableObject = context.AffinityKey as IPortableObject;
                    
                    var key = portableObject != null
                        ? portableObject.Deserialize<PortableObject>()
                        : (PortableObject) context.AffinityKey;

                    Assert.AreEqual(AFF_KEY, key.Val);
                }

                Assert.IsNotNull(grid);

                Assert.IsTrue(context.Name.StartsWith(SVC_NAME));
                Assert.AreNotEqual(Guid.Empty, context.ExecutionId);
            }
        }

        /// <summary>
        /// Test portable service.
        /// </summary>
        private class TestGridServicePortable : TestGridServiceSerializable, IPortableMarshalAware
        {
            /** <inheritdoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteInt("TestProp", TestProperty);
            }

            /** <inheritdoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                TestProperty = reader.ReadInt("TestProp");
            }
        }

        /// <summary>
        /// Test portable service with exceptions in marshalling.
        /// </summary>
        private class TestGridServicePortableErr : TestGridServiceSerializable, IPortableMarshalAware
        {
            /** */
            public bool ThrowOnWrite { get; set; }

            /** <inheritdoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteInt("TestProp", TestProperty);
                
                if (ThrowOnWrite)
                    throw new Exception("Expected exception");
            }

            /** <inheritdoc /> */
            public void ReadPortable(IPortableReader reader)
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
        /// Portable object.
        /// </summary>
        private class PortableObject
        {
            public int Val { get; set; }
        }
    }
}
