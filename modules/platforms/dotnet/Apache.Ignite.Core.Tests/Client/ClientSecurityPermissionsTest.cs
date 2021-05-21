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
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client security permissions.
    /// </summary>
    public class ClientSecurityPermissionsTest
    {
        /** */
        private const string Login = "CLIENT";

        /** */
        private const string AllowAllLogin = "CLIENT_";

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            TestUtils.EnsureJvmCreated();
            TestUtilsJni.StartIgnite("s");
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            TestUtilsJni.StopIgnite("s");
        }

        [Test]
        public void TestCreateCacheNoPermissionThrowsSecurityViolationClientException()
        {
            using (var client = StartClient())
            {
                var ex = Assert.Throws<IgniteClientException>(() => client.CreateCache<int, int>("FORBIDDEN_CACHE"));

                Assert.AreEqual(ClientStatusCode.SecurityViolation, ex.StatusCode);
            }
        }

        private static IIgniteClient StartClient(string login = Login)
        {
            return Ignition.StartClient(GetClientConfiguration(login));
        }

        private static IgniteClientConfiguration GetClientConfiguration(string login)
        {
            return new IgniteClientConfiguration("127.0.0.1")
            {
                UserName = login,
                Password = "pass1"
            };
        }
    }
}
