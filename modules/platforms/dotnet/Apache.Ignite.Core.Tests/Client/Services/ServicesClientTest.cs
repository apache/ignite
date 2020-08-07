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
            var svc = Client.GetServices().GetServiceProxy<ITestService1>(ServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.VoidMethod());
            Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);
        }

        /// <summary>
        /// Tests the basic success path.
        /// </summary>
        [Test]
        public void TestBasicServiceCall()
        {
            ServerServices.DeployNodeSingleton(ServiceName, new TestService1());

            var svc = Client.GetServices().GetServiceProxy<ITestService1>(ServiceName);
            var res = svc.IntMethod();

            Assert.AreEqual(42, res);
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
