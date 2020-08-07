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
    using System.Linq;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Services;
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
        /// Tests that invoking a service that does not exist causes a correct exception.
        /// </summary>
        [Test]
        public void TestNonExistentServiceNameCausesClientException()
        {
            var svc = Client.GetServices().GetServiceProxy<ITestService>(ServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.VoidMethod());
            Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);
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
        /// Tests that <see cref="IServicesClient.WithKeepBinary"/> call causes service invocation results to be
        /// returned in serialized form.
        /// </summary>
        [Test]
        public void TestClientKeepBinaryReturnsServiceInvocationResultInBinaryMode()
        {
            // TODO
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithServerKeepBinary"/> call causes service invocation arguments
        /// to be passed to the service (on server node) in serialized form.
        /// </summary>
        [Test]
        public void TestServerKeepBinaryPassesServerSideArgumentsInBinaryMode()
        {
            // TODO
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
        public void TestAsyncServiceCalls()
        {
            // TODO: Async methods (returning Task) must be handled asynchronously internally.
        }

        /// <summary>
        /// Tests that thin client can call Java services.
        /// </summary>
        [Test]
        public void TestJavaServiceCall()
        {
            // TODO
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
        private ITestService DeployAndGetTestService()
        {
            ServerServices.DeployNodeSingleton(ServiceName, new TestService());

            return Client.GetServices().GetServiceProxy<ITestService>(ServiceName);
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
