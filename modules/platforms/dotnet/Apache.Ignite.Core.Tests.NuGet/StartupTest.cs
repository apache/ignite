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
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Cache test.
    /// </summary>
    public class StartupTest
    {
        /// <summary>
        /// Tests code configuration.
        /// </summary>
        [Test]
        public void TestCodeConfig()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                CacheConfiguration = new[] {new CacheConfiguration("testcache")}
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetCache<int, int>("testcache");

                cache[1] = 5;

                Assert.AreEqual(5, cache[1]);
            }
        }

        /// <summary>
        /// Tests code configuration.
        /// </summary>
        [Test]
        public void TestSpringConfig()
        {
            using (var ignite = Ignition.Start("config\\ignite-config.xml"))
            {
                var cache = ignite.GetCache<int, int>("testcache");

                cache[1] = 5;

                Assert.AreEqual(5, cache[1]);
            }
        }
    }
}
