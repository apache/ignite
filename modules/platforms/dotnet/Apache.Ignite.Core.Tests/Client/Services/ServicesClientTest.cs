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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IServicesClient"/>.
    /// </summary>
    public class ServicesClientTest : ClientTestBase
    {
        /** */
        private const string ServiceName = "SVC_NAME";

        [TearDown]
        public void TestTearDown()
        {
            ServerServices.CancelAll();

            TestUtils.AssertHandleRegistryIsEmpty(1000, Ignition.GetAll().ToArray());
        }

        /// <summary>
        /// Tests the basic success path.
        /// </summary>
        [Test]
        public void TestBasicServiceCall()
        {
            var svc = DeployAndGetTestService();

            var res = svc.IntMethod();

            Assert.AreEqual(42, res);
        }

        /// <summary>
        /// Tests that void method can be called and the lack of result is handled correctly.  
        /// </summary>
        [Test]
        public void TestVoidMethodCall()
        {
            var svc = DeployAndGetTestService();
            var expectedCallCount = TestService.CallCount + 1;
            
            svc.VoidMethod();
            
            Assert.AreEqual(expectedCallCount, TestService.CallCount);
        }

        /// <summary>
        /// Tests that objects can be passed to and from service methods.
        /// </summary>
        [Test]
        public void TestObjectMethodCall()
        {
            var svc = DeployAndGetTestService();

            var res = svc.PersonMethod(new Person(1));
            
            Assert.AreEqual(2, res.Id);
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithKeepBinary"/> call causes service invocation results to be
        /// returned in serialized form.
        /// </summary>
        [Test]
        public void TestClientKeepBinaryReturnsServiceInvocationResultInBinaryMode()
        {
            var svc = DeployAndGetTestService<ITestServiceClient>(s => s.WithKeepBinary());

            var person = Client.GetBinary().ToBinary<IBinaryObject>(new Person(5));
            var res = svc.PersonMethod(person);
            
            Assert.AreEqual(6, res.GetField<int>("Id"));
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithServerKeepBinary"/> call causes service invocation arguments
        /// to be passed to the service (on server node) in serialized form.
        /// </summary>
        [Test]
        public void TestServerKeepBinaryPassesServerSideArgumentsInBinaryMode()
        {
            var svc = DeployAndGetTestService<ITestServiceClient>(s => s.WithServerKeepBinary());

            var res = svc.PersonMethodBinary(new Person(1));
            
            Assert.AreEqual(2, res.Id);
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithServerKeepBinary"/> combined with
        /// <see cref="IServicesClient.WithKeepBinary"/> uses binary objects both on client and server sides.
        /// </summary>
        [Test]
        public void TestServerAndClientKeepBinaryPassesBinaryObjectsOnServerAndClient()
        {
            // TODO
        }

        /// <summary>
        /// Tests various basic argument types passing.
        /// </summary>
        [Test]
        public void TestAllArgumentTypes()
        {
            // TODO

        }

        /// <summary>
        /// Tests async method calls.
        /// </summary>
        [Test]
        [Ignore("IGNITE-13343")]
        public void TestAsyncServiceCalls()
        {
            var svc = DeployAndGetTestService();

            var task = svc.AsyncMethod();
            task.Wait();
            
            Assert.AreEqual(1, task.Result);
        }

        /// <summary>
        /// Tests that thin client can call Java services.
        /// </summary>
        [Test]
        public void TestJavaServiceCall()
        {
            // TODO
        }

        /// <summary>
        /// Tests that specifying custom cluster group causes service calls to be routed to selected servers.
        /// </summary>
        [Test]
        public void TestServicesWithCustomClusterGroupInvokeOnSpecifiedNodes()
        {
            // TODO
        }

        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public void TestEmptyClusterGroupThrowsError()
        {
            // TODO
        }

        /// <summary>
        /// Tests that a custom cluster group that does not have any server nodes with specified service produces
        /// a correct exception.
        ///
        /// - Deploy the service to node X
        /// - Create client cluster group with node Y
        /// - Call service, verify exception
        /// </summary>
        [Test]
        public void TestClusterGroupWithoutMatchingServiceNodesCausesError()
        {
            // TODO
        }
        
        /// <summary>
        /// Tests that exception in service is propagated to the client and service is still operational.
        /// </summary>
        [Test]
        public void TestExceptionInServiceIsPropagatedToClient()
        {
            var svc = DeployAndGetTestService();

            var ex = Assert.Throws<IgniteClientException>(() => svc.ExceptionalMethod());

            Assert.AreEqual("Failed to invoke platform service, see server logs for details", ex.Message);
        }
        
        /// <summary>
        /// Tests that invoking a service that does not exist causes a correct exception.
        /// </summary>
        [Test]
        public void TestNonExistentServiceNameCausesClientException()
        {
            var svc = Client.GetServices().GetServiceProxy<ITestService>(ServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.VoidMethod());
            Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);
        }
        
        // TODO: Binary mode
        // TODO: All argument types
        // TODO: Overloads
        // TODO: Async calls
        // TODO: Timeout
        // TODO: Cluster group
        // TODO: Call Java services

        /// <summary>
        /// Deploys test service and returns client-side proxy.
        /// </summary>
        private ITestService DeployAndGetTestService(Func<IServicesClient, IServicesClient> transform = null)
        {
            return DeployAndGetTestService<ITestService>(transform);
        }

        /// <summary>
        /// Deploys test service and returns client-side proxy.
        /// </summary>
        private T DeployAndGetTestService<T>(Func<IServicesClient, IServicesClient> transform = null) where T : class
        {
            ServerServices.DeployNodeSingleton(ServiceName, new TestService());

            var services = Client.GetServices();

            if (transform != null)
            {
                services = transform(services);
            }
            
            return services.GetServiceProxy<T>(ServiceName);
        }

        /// <summary>
        /// Gets server-side Services.
        /// </summary>
        private static IServices ServerServices
        {
            get { return Ignition.GetIgnite().GetServices(); }
        }
    }
}
