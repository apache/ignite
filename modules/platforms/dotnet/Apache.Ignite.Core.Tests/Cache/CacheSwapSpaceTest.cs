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

namespace Apache.Ignite.Core.Tests.Cache
{
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests the swap space.
    /// </summary>
    public class CacheSwapSpaceTest
    {
        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that swap space is disabled by default and cache can't have EnableSwap.
        /// </summary>
        [Test]
        public void TestDisabledSwapSpace()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration());

            Assert.IsNull(cfg.SwapSpaceSpi);

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.IsNull(ignite.GetConfiguration().SwapSpaceSpi);

                ignite.CreateCache<int, int>(new CacheConfiguration {EnableSwap = true});
            }

        }

        /// <summary>
        /// Tests the swap space.
        /// </summary>
        [Test]
        public void TestSwapSpace()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                // TODO
            };

            using (var ignite = Ignition.Start(cfg))
            {
                
            }
        }
    }
}
