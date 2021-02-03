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

namespace Apache.Ignite.Core.Tests.Client.Binary
{
    using System.Net;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests automatic binary configuration retrieval.
    /// </summary>
    public class BinaryConfigurationRetrievalTest
    {
        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that <see cref="BinaryConfiguration.CompactFooter"/> sets to false on the client when it is false
        /// on the server.
        /// </summary>
        [Test]
        public void TestCompactFooterDisablesOnClientWhenDisabledOnServer()
        {
            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false
                }
            };

            var logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace});
            var clientCfg = new IgniteClientConfiguration(IPAddress.Loopback.ToString());

            Ignition.Start(serverCfg);

            using (var client = Ignition.StartClient(clientCfg))
            {
                var resCfg = client.GetConfiguration();

                Assert.IsNotNull(resCfg.BinaryConfiguration);
                Assert.IsFalse(resCfg.BinaryConfiguration.CompactFooter);

                // TODO: Create a binary object to verify the behavior.
                // TODO: Check log message.
            }
        }
    }
}
