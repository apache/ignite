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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using System.Collections.Specialized;
    using Apache.Ignite.AspNet;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// ASP.NET test.
    /// </summary>
    public class AspNetTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                CacheConfiguration = new [] {new CacheConfiguration("aspNetCache") }
            };

            Ignition.Start(cfg);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests cache put/get.
        /// </summary>
        [Test]
        public void TestSetGet()
        {
            var cacheProvider = new IgniteOutputCacheProvider();

            cacheProvider.Initialize("myProvider", new NameValueCollection
            {
                {"cacheName", "aspNetCache"}
            });

            cacheProvider.Set("1", 3, DateTime.MaxValue);
            Assert.AreEqual(3, cacheProvider.Get("1"));
        }
    }
}
