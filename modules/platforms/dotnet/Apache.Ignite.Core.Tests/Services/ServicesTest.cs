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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
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
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                Services.CancelAll();

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
        /// Tests several services deployment via DeployAll() method.
        /// </summary>
        [Test]
        public void TestDeployAll([Values(true, false)] bool binarizable)
        {
            const int num = 10;

            var cfgs = new List<ServiceConfiguration>();
            for (var i = 0; i < num; i++)
            {
                cfgs.Add(new ServiceConfiguration
                {
                    Name = MakeServiceName(i),
                    MaxPerNodeCount = 3,
                    TotalCount = 3,
                    NodeFilter = new NodeFilter {NodeId = Grid1.GetCluster().GetLocalNode().Id},
                    Service = binarizable ? new TestIgniteServiceBinarizable() : new TestIgniteServiceSerializable()
                });
            }

            Services.DeployAll(cfgs);

            for (var i = 0; i < num; i++)
            {
                CheckServiceStarted(Grid1, 3, MakeServiceName(i));
            }
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
            Assert.AreEqual(0, Grid3.GetServices().GetServices<ITestIgniteService>(SvcName).Count);
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

            foreach (var grid in Grids.Where(x => !x.GetConfiguration().ClientMode))
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
            AssertNoService(SvcName + 0);

            Services.Cancel(SvcName + 1);
            AssertNoService(SvcName + 1);

            for (var i = 2; i < 10; i++)
                Assert.IsNotNull(Services.GetService<ITestIgniteService>(SvcName + i));

            Services.CancelAll();

            for (var i = 0; i < 10; i++)
                AssertNoService(SvcName + i);
        }

        /// <summary>
        /// Tests service proxy.
        /// </summary>
        [Test]
        public void TestGetServiceProxy([Values(true, false)] bool binarizable)
        {
            // Test proxy without a service
            var ex = Assert.Throws<IgniteException>(()=> Services.GetServiceProxy<ITestIgniteService>(SvcName));
            Assert.AreEqual("Failed to find deployed service: " + SvcName, ex.Message);

            // Deploy to grid1 & grid2
            var svc = binarizable
                ? new TestIgniteServiceBinarizable {TestProperty = 17}
                : new TestIgniteServiceSerializable {TestProperty = 17};

            Grid3.GetCluster().ForNodeIds(Grid2.GetCluster().GetLocalNode().Id, Grid1.GetCluster().GetLocalNode().Id)
                .GetServices().DeployNodeSingleton(SvcName, svc);

            // Make sure there is no local instance on grid3
            Assert.IsNull(Grid3.GetServices().GetService<ITestIgniteService>(SvcName));

            // Get proxy
            var prx = Grid3.GetServices().GetServiceProxy<ITestIgniteService>(SvcName);

            // Check proxy properties
            Assert.IsNotNull(prx);
            Assert.AreEqual(prx.ToString(), svc.ToString());
            Assert.AreEqual(17, prx.TestProperty);
            Assert.IsTrue(prx.Initialized);
            // ReSharper disable once AccessToModifiedClosure
            Assert.IsTrue(TestUtils.WaitForCondition(() => prx.Executed, 5000));
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
            prx = Grid3.GetServices().GetServiceProxy<ITestIgniteService>(SvcName, true);
            invokedIds = Enumerable.Range(1, 100).Select(x => prx.NodeId).Distinct().ToList();
            Assert.AreEqual(1, invokedIds.Count);

            // Proxy does not work for cancelled service.
            Services.CancelAll();

            Assert.Throws<ServiceInvocationException>(() => { Assert.IsTrue(prx.Cancelled); });
        }

        /// <summary>
        /// Tests dynamic service proxies.
        /// </summary>
        [Test]
        public void TestGetDynamicServiceProxy()
        {
            // Deploy to remotes.
            var svc = new TestIgniteServiceSerializable { TestProperty = 37 };
            Grid3.GetCluster().ForRemotes().GetServices().DeployNodeSingleton(SvcName, svc);

            // Make sure there is no local instance on grid3
            Assert.IsNull(Grid3.GetServices().GetService<ITestIgniteService>(SvcName));

            // Get proxy.
            dynamic prx = Grid3.GetServices().GetDynamicServiceProxy(SvcName, true);

            // Property getter.
            Assert.AreEqual(37, prx.TestProperty);
            Assert.IsTrue(prx.Initialized);
            Assert.IsTrue(TestUtils.WaitForCondition(() => prx.Executed, 5000));
            Assert.IsFalse(prx.Cancelled);
            Assert.AreEqual(SvcName, prx.LastCallContextName);

            // Property setter.
            prx.TestProperty = 42;
            Assert.AreEqual(42, prx.TestProperty);

            // Method invoke.
            Assert.AreEqual(prx.ToString(), svc.ToString());
            Assert.AreEqual("baz", prx.Method("baz"));

            // Non-existent member.
            var ex = Assert.Throws<ServiceInvocationException>(() => prx.FooBar(1));
            Assert.AreEqual(
                string.Format("Failed to invoke proxy: there is no method 'FooBar' in type '{0}' with 1 arguments",
                    typeof(TestIgniteServiceSerializable)), (ex.InnerException ?? ex).Message);

            // Exception in service.
            ex = Assert.Throws<ServiceInvocationException>(() => prx.ErrMethod(123));
            Assert.AreEqual("ExpectedException", (ex.InnerException ?? ex).Message.Substring(0, 17));
        }

        /// <summary>
        /// Tests dynamic service proxies with local service instance.
        /// </summary>
        [Test]
        public void TestGetDynamicServiceProxyLocal()
        {
            // Deploy to all nodes.
            var svc = new TestIgniteServiceSerializable { TestProperty = 37 };
            Grid1.GetServices().DeployNodeSingleton(SvcName, svc);

            // Make sure there is an instance on grid1.
            var svcInst = Grid1.GetServices().GetService<ITestIgniteService>(SvcName);
            Assert.IsNotNull(svcInst);

            // Get dynamic proxy that simply wraps the service instance.
            var prx = Grid1.GetServices().GetDynamicServiceProxy(SvcName);
            Assert.AreSame(prx, svcInst);
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
            Assert.IsInstanceOf<InvalidCastException>(ex.InnerException);
        }

        /// <summary>
        /// Test call service proxy from remote node with a methods having an array of user types and objects.
        /// </summary>
        [Test]
        public void TestCallServiceProxyWithTypedArrayParameters()
        {
            // Deploy to the remote node.
            var nodeId = Grid2.GetCluster().GetLocalNode().Id;

            var cluster = Grid1.GetCluster().ForNodeIds(nodeId);

            cluster.GetServices().DeployNodeSingleton(SvcName, new TestIgniteServiceArraySerializable());

            var typedArray = new[] {10, 11, 12}
                .Select(x => new PlatformComputeBinarizable {Field = x}).ToArray();

            var objArray = typedArray.ToArray<object>();

            // object[]
            var prx = Services.GetServiceProxy<ITestIgniteServiceArray>(SvcName);

            Assert.AreEqual(new[] {11, 12, 13}, prx.TestBinarizableArrayOfObjects(objArray)
                .OfType<PlatformComputeBinarizable>().Select(x => x.Field).ToArray());

            Assert.IsNull(prx.TestBinarizableArrayOfObjects(null));

            Assert.IsEmpty(prx.TestBinarizableArrayOfObjects(new object[0]));

            // T[]
            Assert.AreEqual(new[] {11, 12, 13}, prx.TestBinarizableArray(typedArray)
                  .Select(x => x.Field).ToArray());

            Assert.IsEmpty(prx.TestBinarizableArray(new PlatformComputeBinarizable[0]));

            Assert.IsNull(prx.TestBinarizableArray(null));

            // BinaryObject[]
            var binPrx = cluster.GetServices()
                .WithKeepBinary()
                .WithServerKeepBinary()
                .GetServiceProxy<ITestIgniteServiceArray>(SvcName);

            var res = binPrx.TestBinaryObjectArray(
                typedArray.Select(Grid1.GetBinary().ToBinary<IBinaryObject>).ToArray());

            Assert.AreEqual(new[] {11, 12, 13}, res.Select(b => b.GetField<int>("Field")));

            // TestBinarizableArray2 has no corresponding class in Java.
            var typedArray2 = new[] {10, 11, 12}
                .Select(x => new PlatformComputeBinarizable2 {Field = x}).ToArray();

            var actual = prx.TestBinarizableArray2(typedArray2)
                .Select(x => x.Field).ToArray();

            Assert.AreEqual(new[] {11, 12, 13}, actual);
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
        public void TestDeployMultipleException([Values(true, false)] bool keepBinary)
        {
            VerifyDeploymentException((services, svc) =>
                services.DeployMultiple(SvcName, svc, Grids.Length, 1), keepBinary);
        }

        /// <summary>
        /// Tests exception in Initialize.
        /// </summary>
        [Test]
        public void TestDeployException([Values(true, false)] bool keepBinary)
        {
            VerifyDeploymentException((services, svc) =>
                services.Deploy(new ServiceConfiguration
                {
                    Name = SvcName,
                    Service = svc,
                    TotalCount = Grids.Length,
                    MaxPerNodeCount = 1
                }), keepBinary);
        }

        /// <summary>
        /// Tests ServiceDeploymentException result via DeployAll() method.
        /// </summary>
        [Test]
        public void TestDeployAllException([Values(true, false)] bool binarizable)
        {
            const int num = 10;
            const int firstFailedIdx = 1;
            const int secondFailedIdx = 9;

            var cfgs = new List<ServiceConfiguration>();
            for (var i = 0; i < num; i++)
            {
                var throwInit = (i == firstFailedIdx || i == secondFailedIdx);
                cfgs.Add(new ServiceConfiguration
                {
                    Name = MakeServiceName(i),
                    MaxPerNodeCount = 2,
                    TotalCount = 2,
                    NodeFilter = new NodeFilter { NodeId = Grid1.GetCluster().GetLocalNode().Id },
                    Service = binarizable ? new TestIgniteServiceBinarizable { TestProperty = i, ThrowInit = throwInit }
                        : new TestIgniteServiceSerializable { TestProperty = i, ThrowInit = throwInit }
                });
            }

            var deploymentException = Assert.Throws<ServiceDeploymentException>(() => Services.DeployAll(cfgs));

            var failedCfgs = deploymentException.FailedConfigurations;
            Assert.IsNotNull(failedCfgs);
            Assert.AreEqual(2, failedCfgs.Count);

            var firstFailedSvc = binarizable ? failedCfgs.ElementAt(0).Service as TestIgniteServiceBinarizable :
                failedCfgs.ElementAt(0).Service as TestIgniteServiceSerializable;
            var secondFailedSvc = binarizable ? failedCfgs.ElementAt(1).Service as TestIgniteServiceBinarizable :
                failedCfgs.ElementAt(1).Service as TestIgniteServiceSerializable;

            Assert.IsNotNull(firstFailedSvc);
            Assert.IsNotNull(secondFailedSvc);

            int[] properties = { firstFailedSvc.TestProperty, secondFailedSvc.TestProperty };

            Assert.IsTrue(properties.Contains(firstFailedIdx));
            Assert.IsTrue(properties.Contains(secondFailedIdx));

            for (var i = 0; i < num; i++)
            {
                if (i != firstFailedIdx && i != secondFailedIdx)
                {
                    CheckServiceStarted(Grid1, 2, MakeServiceName(i));
                }
            }
        }

        /// <summary>
        /// Tests input errors for DeployAll() method.
        /// </summary>
        [Test]
        public void TestDeployAllInputErrors()
        {
            var nullException = Assert.Throws<ArgumentNullException>(() => Services.DeployAll(null));
            Assert.IsTrue(nullException.Message.Contains("configurations"));

            var argException = Assert.Throws<ArgumentException>(() => Services.DeployAll(new List<ServiceConfiguration>()));
            Assert.IsTrue(argException.Message.Contains("empty collection"));

            nullException = Assert.Throws<ArgumentNullException>(() => Services.DeployAll(new List<ServiceConfiguration> { null }));
            Assert.IsTrue(nullException.Message.Contains("configurations[0]"));

            nullException = Assert.Throws<ArgumentNullException>(() => Services.DeployAll(new List<ServiceConfiguration>
            {
                new ServiceConfiguration { Name = SvcName }
            }));
            Assert.IsTrue(nullException.Message.Contains("configurations[0].Service"));

            argException = Assert.Throws<ArgumentException>(() => Services.DeployAll(new List<ServiceConfiguration>
            {
                new ServiceConfiguration { Service = new TestIgniteServiceSerializable() }
            }));
            Assert.IsTrue(argException.Message.Contains("configurations[0].Name"));

            argException = Assert.Throws<ArgumentException>(() => Services.DeployAll(new List<ServiceConfiguration>
            {
                new ServiceConfiguration { Service = new TestIgniteServiceSerializable(), Name = string.Empty }
            }));
            Assert.IsTrue(argException.Message.Contains("configurations[0].Name"));
        }

        /// <summary>
        /// Tests [Serializable] usage of ServiceDeploymentException.
        /// </summary>
        [Test]
        public void TestDeploymentExceptionSerializable()
        {
            var cfg = new ServiceConfiguration
            {
                Name = "foo",
                CacheName = "cacheName",
                AffinityKey = 1,
                MaxPerNodeCount = 2,
                Service = new TestIgniteServiceSerializable(),
                NodeFilter = new NodeFilter(),
                TotalCount = 3
            };

            var ex = new ServiceDeploymentException("msg", new Exception("in"), new[] {cfg});

            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, ex);
            stream.Seek(0, SeekOrigin.Begin);

            var res = (ServiceDeploymentException) formatter.Deserialize(stream);

            Assert.AreEqual(ex.Message, res.Message);
            Assert.IsNotNull(res.InnerException);
            Assert.AreEqual("in", res.InnerException.Message);

            var resCfg = res.FailedConfigurations.Single();

            Assert.AreEqual(cfg.Name, resCfg.Name);
            Assert.AreEqual(cfg.CacheName, resCfg.CacheName);
            Assert.AreEqual(cfg.AffinityKey, resCfg.AffinityKey);
            Assert.AreEqual(cfg.MaxPerNodeCount, resCfg.MaxPerNodeCount);
            Assert.AreEqual(cfg.TotalCount, resCfg.TotalCount);
            Assert.IsInstanceOf<TestIgniteServiceSerializable>(cfg.Service);
            Assert.IsInstanceOf<NodeFilter>(cfg.NodeFilter);
        }

        /// <summary>
        /// Verifies the deployment exception.
        /// </summary>
        private void VerifyDeploymentException(Action<IServices, IService> deploy, bool keepBinary)
        {
            var svc = new TestIgniteServiceSerializable { ThrowInit = true };

            var services = Services;

            if (keepBinary)
            {
                services = services.WithKeepBinary();
            }

            var deploymentException = Assert.Throws<ServiceDeploymentException>(() => deploy(services, svc));

            var text = keepBinary
                ? "Service deployment failed with a binary error. Examine BinaryCause for details."
                : "Service deployment failed with an exception. Examine InnerException for details.";

            Assert.AreEqual(text, deploymentException.Message);

            Exception ex;

            if (keepBinary)
            {
                Assert.IsNull(deploymentException.InnerException);

                ex = deploymentException.BinaryCause.Deserialize<Exception>();
            }
            else
            {
                Assert.IsNull(deploymentException.BinaryCause);

                ex = deploymentException.InnerException;
            }

            Assert.IsNotNull(ex);
            Assert.AreEqual("Expected exception", ex.Message);
            Assert.IsTrue(ex.StackTrace.Trim().StartsWith(
                "at Apache.Ignite.Core.Tests.Services.ServicesTest.TestIgniteServiceSerializable.Init"));

            var failedCfgs = deploymentException.FailedConfigurations;
            Assert.IsNotNull(failedCfgs);
            Assert.AreEqual(1, failedCfgs.Count);

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

            Services.DeployMultiple(SvcName, svc, 2, 1);

            CheckServiceStarted(Grid1);

            Services.CancelAll();

            // Cancellation failed, but service is removed.
            AssertNoService();
        }

        /// <summary>
        /// Tests exception in binarizable implementation.
        /// </summary>
        [Test]
        public void TestMarshalExceptionOnRead()
        {
            var svc = new TestIgniteServiceBinarizableErr();

            var ex = Assert.Throws<ServiceDeploymentException>(() =>
                Services.DeployMultiple(SvcName, svc, Grids.Length, 1));

            Assert.IsNotNull(ex.InnerException);
            Assert.AreEqual("Expected exception", ex.InnerException.Message);

            var svc0 = Services.GetService<TestIgniteServiceSerializable>(SvcName);

            Assert.IsNull(svc0);
        }

        /// <summary>
        /// Tests exception in binarizable implementation.
        /// </summary>
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
        /// Tests Java service invocation.
        /// </summary>
        [Test]
        public void TestCallJavaService()
        {
            // Deploy Java service
            const string javaSvcName = "javaService";
            DeployJavaService(javaSvcName);

            // Verify decriptor
            var descriptor = Services.GetServiceDescriptors().Single(x => x.Name == javaSvcName);
            Assert.AreEqual(javaSvcName, descriptor.Name);

            var svc = Services.GetServiceProxy<IJavaService>(javaSvcName, false);
            var binSvc = Services.WithKeepBinary().WithServerKeepBinary()
                .GetServiceProxy<IJavaService>(javaSvcName, false);

            // Basics
            Assert.IsTrue(svc.isInitialized());
            Assert.IsTrue(TestUtils.WaitForCondition(() => svc.isExecuted(), 500));
            Assert.IsFalse(svc.isCancelled());

            // Primitives
            Assert.AreEqual(4, svc.test((byte) 3));
            Assert.AreEqual(5, svc.test((short) 4));
            Assert.AreEqual(6, svc.test(5));
            Assert.AreEqual(6, svc.test((long) 5));
            Assert.AreEqual(3.8f, svc.test(2.3f));
            Assert.AreEqual(5.8, svc.test(3.3));
            Assert.IsFalse(svc.test(true));
            Assert.AreEqual('b', svc.test('a'));
            Assert.AreEqual("Foo!", svc.test("Foo"));

            // Nullables (Java wrapper types)
            Assert.AreEqual(4, svc.testWrapper(3));
            Assert.AreEqual(5, svc.testWrapper((short?) 4));
            Assert.AreEqual(6, svc.testWrapper((int?)5));
            Assert.AreEqual(6, svc.testWrapper((long?) 5));
            Assert.AreEqual(3.8f, svc.testWrapper(2.3f));
            Assert.AreEqual(5.8, svc.testWrapper(3.3));
            Assert.AreEqual(false, svc.testWrapper(true));
            Assert.AreEqual('b', svc.testWrapper('a'));

            // Arrays
            Assert.AreEqual(new byte[] {2, 3, 4}, svc.testArray(new byte[] {1, 2, 3}));
            Assert.AreEqual(new short[] {2, 3, 4}, svc.testArray(new short[] {1, 2, 3}));
            Assert.AreEqual(new[] {2, 3, 4}, svc.testArray(new[] {1, 2, 3}));
            Assert.AreEqual(new long[] {2, 3, 4}, svc.testArray(new long[] {1, 2, 3}));
            Assert.AreEqual(new float[] {2, 3, 4}, svc.testArray(new float[] {1, 2, 3}));
            Assert.AreEqual(new double[] {2, 3, 4}, svc.testArray(new double[] {1, 2, 3}));
            Assert.AreEqual(new[] {"a1", "b1"}, svc.testArray(new [] {"a", "b"}));
            Assert.AreEqual(new[] {'c', 'd'}, svc.testArray(new[] {'b', 'c'}));
            Assert.AreEqual(new[] {false, true, false}, svc.testArray(new[] {true, false, true}));

            // Nulls
            Assert.AreEqual(9, svc.testNull(8));
            Assert.IsNull(svc.testNull(null));

            // params / varargs
            Assert.AreEqual(5, svc.testParams(1, 2, 3, 4, "5"));
            Assert.AreEqual(0, svc.testParams());

            // Overloads
            Assert.AreEqual(3, svc.test(2, "1"));
            Assert.AreEqual(3, svc.test("1", 2));

            // Binary
            Assert.AreEqual(7, svc.testBinarizable(new PlatformComputeBinarizable {Field = 6}).Field);

            // Binary collections
            var arr  = new[] {10, 11, 12}.Select(
                x => new PlatformComputeBinarizable {Field = x}).ToArray();
            var arrOfObj = arr.ToArray<object>();

            Assert.AreEqual(new[] {11, 12, 13}, svc.testBinarizableCollection(arr)
                .OfType<PlatformComputeBinarizable>().Select(x => x.Field));

            Assert.AreEqual(new[] {11, 12, 13}, svc.testBinarizableArrayOfObjects(arrOfObj)
                .OfType<PlatformComputeBinarizable>().Select(x => x.Field));

            Assert.IsNull(svc.testBinarizableArrayOfObjects(null));

            Assert.AreEqual(new[] {11, 12, 13}, svc.testBinarizableArray(arr)
                .Select(x => x.Field));

            Assert.IsNull(svc.testBinarizableArray(null));

            // Binary object
            Assert.AreEqual(15,
                binSvc.testBinaryObject(
                    Grid1.GetBinary().ToBinary<IBinaryObject>(new PlatformComputeBinarizable {Field = 6}))
                    .GetField<int>("Field"));

            DateTime dt = new DateTime(1992, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            Assert.AreEqual(dt, svc.test(dt));
            Assert.AreEqual(dt, svc.testNullTimestamp(dt));
            Assert.IsNull(svc.testNullTimestamp(null));
            Assert.AreEqual(dt, svc.testArray(new DateTime?[] {dt})[0]);

            Guid guid = Guid.NewGuid();

            Assert.AreEqual(guid, svc.test(guid));
            Assert.AreEqual(guid, svc.testNullUUID(guid));
            Assert.IsNull(svc.testNullUUID(null));
            Assert.AreEqual(guid, svc.testArray(new Guid?[] {guid})[0]);

            // Binary object array.
            var binArr = arr.Select(Grid1.GetBinary().ToBinary<IBinaryObject>).ToArray();

            Assert.AreEqual(new[] {11, 12, 13}, binSvc.testBinaryObjectArray(binArr)
                .Select(x => x.GetField<int>("Field")));

            Services.Cancel(javaSvcName);
        }

        /// <summary>
        /// Tests Java service invocation with dynamic proxy.
        /// </summary>
        [Test]
        public void TestCallJavaServiceDynamicProxy()
        {
            const string javaSvcName = "javaService";
            DeployJavaService(javaSvcName);

            var svc = Grid1.GetServices().GetDynamicServiceProxy(javaSvcName, true);

            // Basics
            Assert.IsTrue(svc.isInitialized());
            Assert.IsTrue(TestUtils.WaitForCondition(() => svc.isExecuted(), 500));
            Assert.IsFalse(svc.isCancelled());

            // Primitives
            Assert.AreEqual(4, svc.test((byte)3));
            Assert.AreEqual(5, svc.test((short)4));
            Assert.AreEqual(6, svc.test(5));
            Assert.AreEqual(6, svc.test((long)5));
            Assert.AreEqual(3.8f, svc.test(2.3f));
            Assert.AreEqual(5.8, svc.test(3.3));
            Assert.IsFalse(svc.test(true));
            Assert.AreEqual('b', svc.test('a'));
            Assert.AreEqual("Foo!", svc.test("Foo"));

            // Nullables (Java wrapper types)
            Assert.AreEqual(4, svc.testWrapper(3));
            Assert.AreEqual(5, svc.testWrapper((short?)4));
            Assert.AreEqual(6, svc.testWrapper((int?)5));
            Assert.AreEqual(6, svc.testWrapper((long?)5));
            Assert.AreEqual(3.8f, svc.testWrapper(2.3f));
            Assert.AreEqual(5.8, svc.testWrapper(3.3));
            Assert.AreEqual(false, svc.testWrapper(true));
            Assert.AreEqual('b', svc.testWrapper('a'));

            // Arrays
            Assert.AreEqual(new byte[] { 2, 3, 4 }, svc.testArray(new byte[] { 1, 2, 3 }));
            Assert.AreEqual(new short[] { 2, 3, 4 }, svc.testArray(new short[] { 1, 2, 3 }));
            Assert.AreEqual(new[] { 2, 3, 4 }, svc.testArray(new[] { 1, 2, 3 }));
            Assert.AreEqual(new long[] { 2, 3, 4 }, svc.testArray(new long[] { 1, 2, 3 }));
            Assert.AreEqual(new float[] { 2, 3, 4 }, svc.testArray(new float[] { 1, 2, 3 }));
            Assert.AreEqual(new double[] { 2, 3, 4 }, svc.testArray(new double[] { 1, 2, 3 }));
            Assert.AreEqual(new[] { "a1", "b1" }, svc.testArray(new[] { "a", "b" }));
            Assert.AreEqual(new[] { 'c', 'd' }, svc.testArray(new[] { 'b', 'c' }));
            Assert.AreEqual(new[] { false, true, false }, svc.testArray(new[] { true, false, true }));

            // Nulls
            Assert.AreEqual(9, svc.testNull(8));
            Assert.IsNull(svc.testNull(null));

            // Overloads
            Assert.AreEqual(3, svc.test(2, "1"));
            Assert.AreEqual(3, svc.test("1", 2));

            // Binary
            Assert.AreEqual(7, svc.testBinarizable(new PlatformComputeBinarizable { Field = 6 }).Field);

            // Binary object
            var binSvc = Services.WithKeepBinary().WithServerKeepBinary().GetDynamicServiceProxy(javaSvcName);

            Assert.AreEqual(15,
                binSvc.testBinaryObject(
                    Grid1.GetBinary().ToBinary<IBinaryObject>(new PlatformComputeBinarizable { Field = 6 }))
                    .GetField<int>("Field"));

            DateTime dt = new DateTime(1992, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            Assert.AreEqual(dt, svc.test(dt));
            Assert.AreEqual(dt, svc.testNullTimestamp(dt));
            Assert.IsNull(svc.testNullTimestamp(null));
            Assert.AreEqual(dt, svc.testArray(new DateTime?[] { dt })[0]);

            Guid guid = Guid.NewGuid();

            Assert.AreEqual(guid, svc.test(guid));
            Assert.AreEqual(guid, svc.testNullUUID(guid));
            Assert.IsNull(svc.testNullUUID(null));
            Assert.AreEqual(guid, svc.testArray(new Guid?[] { guid })[0]);
        }

        /// <summary>
        /// Deploys the java service.
        /// </summary>
        private void DeployJavaService(string javaSvcName)
        {
            Grid1.GetCompute()
                .ExecuteJavaTask<object>("org.apache.ignite.platform.PlatformDeployServiceTask", javaSvcName);

            TestUtils.WaitForCondition(() => Services.GetServiceDescriptors().Any(x => x.Name == javaSvcName), 1000);
        }

        /// <summary>
        /// Tests the footer setting.
        /// </summary>
        [Test]
        public void TestFooterSetting()
        {
            foreach (var grid in Grids)
            {
                Assert.AreEqual(CompactFooter, ((Ignite) grid).Marshaller.CompactFooter);
                Assert.AreEqual(CompactFooter, grid.GetConfiguration().BinaryConfiguration.CompactFooter);
            }
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (Grid1 != null)
                return;

            Grid1 = Ignition.Start(GetConfiguration("Config\\Compute\\compute-grid1.xml"));
            Grid2 = Ignition.Start(GetConfiguration("Config\\Compute\\compute-grid2.xml"));
            Grid3 = Ignition.Start(GetConfiguration("Config\\Compute\\compute-grid3.xml"));

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
        private static void CheckServiceStarted(IIgnite grid, int count = 1, string svcName = SvcName)
        {
            Func<ICollection<TestIgniteServiceSerializable>> getServices = () =>
                grid.GetServices().GetServices<TestIgniteServiceSerializable>(svcName);

            Assert.IsTrue(TestUtils.WaitForCondition(() => count == getServices().Count, 5000));

            var svc = getServices().First();

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
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0
            if (!CompactFooter)
            {
                springConfigUrl = Compute.ComputeApiTestFullFooter.ReplaceFooterSetting(springConfigUrl);
            }
#endif

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl,
                BinaryConfiguration = new BinaryConfiguration(
                    typeof (TestIgniteServiceBinarizable),
                    typeof (TestIgniteServiceBinarizableErr),
                    typeof (PlatformComputeBinarizable),
                    typeof (BinarizableObject))
                {
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                }
            };
        }

        /// <summary>
        /// Asserts that there is no service on any grid with given name.
        /// </summary>
        /// <param name="name">The name.</param>
        private void AssertNoService(string name = SvcName)
        {
            foreach (var grid in Grids)
                Assert.IsTrue(
                    // ReSharper disable once AccessToForEachVariableInClosure
                    TestUtils.WaitForCondition(() => grid.GetServices()
                        .GetService<ITestIgniteService>(name) == null, 5000));
        }

        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return Grid1.GetServices(); }
        }

        /// <summary>
        /// Gets a value indicating whether compact footers should be used.
        /// </summary>
        protected virtual bool CompactFooter { get { return true; } }

        /// <summary>
        /// Makes Service1-{i} names for services.
        /// </summary>
        private static string MakeServiceName(int i)
        {
            // Please note that CheckContext() validates Name.StartsWith(SvcName).
            return string.Format("{0}-{1}", SvcName, i);
        }

        /// <summary>
        /// Test base service.
        /// </summary>
        public interface ITestIgniteServiceBase
        {
            /** */
            int TestProperty { get; set; }

            /** */
            object Method(object arg);
        }

        /// <summary>
        /// Test serializable service with a methods having an array of user types and objects.
        /// </summary>
        public interface ITestIgniteServiceArray
        {
            /** */
            object[] TestBinarizableArrayOfObjects(object[] x);

            /** */
            PlatformComputeBinarizable[] TestBinarizableArray(PlatformComputeBinarizable[] x);

            /** */
            IBinaryObject[] TestBinaryObjectArray(IBinaryObject[] x);

            /** Class TestBinarizableArray2 has no an equals class in Java. */
            PlatformComputeBinarizable2[] TestBinarizableArray2(PlatformComputeBinarizable2[] x);
        }

        /// <summary>
        /// Test serializable service with a methods having an array of user types and objects.
        /// </summary>
        [Serializable]
        private class TestIgniteServiceArraySerializable : TestIgniteServiceSerializable, ITestIgniteServiceArray
        {
            /** */
            public object[] TestBinarizableArrayOfObjects(object[] arg)
            {
                if (arg == null)
                    return null;

                for (var i = 0; i < arg.Length; i++)
                    if (arg[i] != null)
                        if (arg[i].GetType() == typeof(PlatformComputeBinarizable))
                            arg[i] = new PlatformComputeBinarizable()
                                {Field = ((PlatformComputeBinarizable) arg[i]).Field + 1};
                        else
                            arg[i] = new PlatformComputeBinarizable2()
                                {Field = ((PlatformComputeBinarizable2) arg[i]).Field + 1};

                return arg;
            }

            /** */
            public PlatformComputeBinarizable[] TestBinarizableArray(PlatformComputeBinarizable[] arg)
            {
                // ReSharper disable once CoVariantArrayConversion
                return (PlatformComputeBinarizable[])TestBinarizableArrayOfObjects(arg);
            }

            /** */
            public IBinaryObject[] TestBinaryObjectArray(IBinaryObject[] x)
            {
                for (var i = 0; i < x.Length; i++)
                {
                    var binaryObject = x[i];

                    var fieldVal = binaryObject.GetField<int>("Field");

                    x[i] = binaryObject.ToBuilder().SetField("Field", fieldVal + 1).Build();
                }

                return x;
            }

            /** */
            public PlatformComputeBinarizable2[] TestBinarizableArray2(PlatformComputeBinarizable2[] arg)
            {
                // ReSharper disable once CoVariantArrayConversion
                return (PlatformComputeBinarizable2[])TestBinarizableArrayOfObjects(arg);
            }
        }

        /// <summary>
        /// Test service interface for proxying.
        /// </summary>
        public interface ITestIgniteService : IService, ITestIgniteServiceBase
        {
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
            object ErrMethod(object arg);
        }

        /// <summary>
        /// Test service interface for proxy usage.
        /// Has some of the original interface members with different signatures.
        /// </summary>
        public interface ITestIgniteServiceProxyInterface
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
        private class TestIgniteServiceSerializable : ITestIgniteService
        {
            /** */
            [InstanceResource]
            // ReSharper disable once UnassignedField.Local
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
                // ReSharper disable once InconsistentlySynchronizedField
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
                lock (this)
                {
                    if (ThrowInit)
                        throw new Exception("Expected exception");

                    CheckContext(context);

                    Assert.IsFalse(context.IsCancelled);
                    Initialized = true;
                }
            }

            /** <inheritdoc /> */
            public void Execute(IServiceContext context)
            {
                lock (this)
                {
                    if (ThrowExecute)
                        throw new Exception("Expected exception");

                    CheckContext(context);

                    Assert.IsFalse(context.IsCancelled);
                    Assert.IsTrue(Initialized);
                    Assert.IsFalse(Cancelled);

                    Executed = true;
                }
            }

            /** <inheritdoc /> */
            public void Cancel(IServiceContext context)
            {
                lock (this)
                {
                    if (ThrowCancel)
                        throw new Exception("Expected exception");

                    CheckContext(context);

                    Assert.IsTrue(context.IsCancelled);

                    Cancelled = true;
                }
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
                writer.WriteBoolean("ThrowInit", ThrowInit);
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                ThrowInit = reader.ReadBoolean("ThrowInit");
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

        /// <summary>
        /// Java service proxy interface.
        /// </summary>
        [SuppressMessage("ReSharper", "InconsistentNaming")]
        public interface IJavaService
        {
            /** */
            bool isCancelled();

            /** */
            bool isInitialized();

            /** */
            bool isExecuted();

            /** */
            byte test(byte x);

            /** */
            short test(short x);

            /** */
            int test(int x);

            /** */
            long test(long x);

            /** */
            float test(float x);

            /** */
            double test(double x);

            /** */
            char test(char x);

            /** */
            string test(string x);

            /** */
            bool test(bool x);

            /** */
            DateTime test(DateTime x);

            /** */
            Guid test(Guid x);

            /** */
            byte? testWrapper(byte? x);

            /** */
            short? testWrapper(short? x);

            /** */
            int? testWrapper(int? x);

            /** */
            long? testWrapper(long? x);

            /** */
            float? testWrapper(float? x);

            /** */
            double? testWrapper(double? x);

            /** */
            char? testWrapper(char? x);

            /** */
            bool? testWrapper(bool? x);

            /** */
            byte[] testArray(byte[] x);

            /** */
            short[] testArray(short[] x);

            /** */
            int[] testArray(int[] x);

            /** */
            long[] testArray(long[] x);

            /** */
            float[] testArray(float[] x);

            /** */
            double[] testArray(double[] x);

            /** */
            char[] testArray(char[] x);

            /** */
            string[] testArray(string[] x);

            /** */
            bool[] testArray(bool[] x);

            /** */
            DateTime?[] testArray(DateTime?[] x);

            /** */
            Guid?[] testArray(Guid?[] x);

            /** */
            int test(int x, string y);

            /** */
            int test(string x, int y);

            /** */
            int? testNull(int? x);

            /** */
            DateTime? testNullTimestamp(DateTime? x);

            /** */
            Guid? testNullUUID(Guid? x);

            /** */
            int testParams(params object[] args);

            /** */
            PlatformComputeBinarizable testBinarizable(PlatformComputeBinarizable x);

            /** */
            object[] testBinarizableArrayOfObjects(object[] x);

            /** */
            IBinaryObject[] testBinaryObjectArray(IBinaryObject[] x);

            /** */
            PlatformComputeBinarizable[] testBinarizableArray(PlatformComputeBinarizable[] x);

            /** */
            ICollection testBinarizableCollection(ICollection x);

            /** */
            IBinaryObject testBinaryObject(IBinaryObject x);
        }

        /// <summary>
        /// Interop class.
        /// </summary>
        public class PlatformComputeBinarizable
        {
            /** */
            public int Field { get; set; }
        }

        /// <summary>
        /// Class has no an equals class in Java.
        /// </summary>
        public class PlatformComputeBinarizable2
        {
            /** */
            public int Field { get; set; }
        }
    }
}
