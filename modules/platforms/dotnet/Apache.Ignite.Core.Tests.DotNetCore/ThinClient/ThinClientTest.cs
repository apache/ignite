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

namespace Apache.Ignite.Core.Tests.DotNetCore.ThinClient
{
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Tests.DotNetCore.Common;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Thin client connection test.
    /// </summary>
    [TestClass]
    public class ThinClientTest : TestBase
    {
        /// <summary>
        /// Tests the thin client connection.
        /// </summary>
        [TestMethod]
        public void TestThinClientConnection()
        {
            var clientCfg = new IgniteClientConfiguration
            {
                Host = "127.0.0.1"
            };

            using (var ignite = Start())
            using (var client = Ignition.StartClient(clientCfg))
            {
                var clientCache = client.CreateCache<int, Person>("p");
                clientCache[1] = new Person("Abc", 123);
                
                Assert.AreEqual(123, clientCache.Get(1).Id);
                Assert.AreEqual("Abc", ignite.GetCache<int, Person>(clientCache.Name)[1].Name);
            }
        }
    }
}
