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

namespace Apache.Ignite.Core.Tests.DotNetCore
{
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Configuration;


    /// <summary>
    /// Tests Ignite startup.
    /// 
    /// MsTest is currently the most viable option on .NET Core (especially because of console output handling).
    /// </summary>
    [TestClass]
    public class IgnitionStartTest
    {
        /// <summary>
        /// Tests that Ignite starts with default configuration.
        /// </summary>
        [TestMethod]
        public void TestIgniteStartsWithDefaultConfig()
        {
            var cfg = TestUtils.GetTestConfiguration();

            var ignite = Ignition.Start(cfg);
            Assert.IsNotNull(ignite);

            var cache = ignite.CreateCache<int, int>("foo");
            cache[1] = 1;
            Assert.AreEqual(1, cache[1]);

            // Second node.
            var ignite2 = Ignition.Start(cfg);
            Assert.AreEqual(2, ignite2.GetCluster().GetNodes().Count);

            // Stop node.
            Ignition.Stop(ignite.Name, true);
            Assert.AreEqual(1, ignite2.GetCluster().GetNodes().Count);
        }

        /// <summary>
        /// Tests the ignite starts from application configuration.
        /// </summary>
        [TestMethod]
        public void TestIgniteStartsFromAppConfig()
        {
            //IgniteConfigurationSection section = ConfigurationManager.GetSection("dd") 
            //    as IgniteConfigurationSection;

            var ignite = Ignition.StartFromApplicationConfiguration();
            var cache = ignite.GetCache<int, int>(ignite.GetCacheNames().Single());

            Assert.AreEqual("cacheFromConfig", cache.Name);
            Assert.AreEqual(CacheMode.Replicated, cache.GetConfiguration().CacheMode);
        }

        /// <summary>
        /// Fixture cleanup.
        /// </summary>
        [ClassCleanup]
        public static void ClassCleanup()
        {
            Ignition.StopAll(true);
        }
    }
}