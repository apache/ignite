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

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Tests the EntityFramework integration.
    /// </summary>
    public class EntityFrameworkCacheTest
    {
        /// <summary>
        /// Tests cache startup and basic operation.
        /// </summary>
        [Test]
        public void TestStartupPutGet()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                GridName = "myGrid"
            };
            
            // ReSharper disable once ObjectCreationAsStatement
            new IgniteDbConfiguration(cfg, "efCache", null);

            var ignite = Ignition.GetIgnite(cfg.GridName);
            var cache = ignite.GetCache<string, object>("efCache");

            var efCache = new IgniteEntityFrameworkCache(cache);

            object val;
            Assert.IsFalse(efCache.GetItem("1", out val));

            efCache.PutItem("1", "val", new [] {"streets"}, TimeSpan.MaxValue, DateTimeOffset.MaxValue);
            Assert.IsTrue(efCache.GetItem("1", out val));
            Assert.AreEqual("val", val);
        }

        /// <summary>
        /// Test teardown.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }
    }
}
