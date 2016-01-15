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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System;
    using System.Collections.Specialized;
    using Apache.Ignite.AspNet;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteOutputCacheProvider"/>
    /// </summary>
    public class IgniteOutputCacheProviderTest
    {
        /** Grid name XML config attribute. */
        private const string GridNameAttr = "gridName";

        /** Cache name XML config attribute. */
        private const string CacheNameAttr = "cacheName";

        /// <summary>
        /// Tests output caching.
        /// </summary>
        [Test]
        public void TestCaching()
        {
            const string gridName = "myGrid";
            const string cacheName = "myCache";

            var cacheProvider = new IgniteOutputCacheProvider();

            // Not initialized
            Assert.Throws<InvalidOperationException>(() => cacheProvider.Get("1"));

            // Grid not started
            Assert.Throws<IgniteException>(() =>
                cacheProvider.Initialize("testName", new NameValueCollection
                {
                    {GridNameAttr, gridName},
                    {CacheNameAttr, cacheName}
                }));

            // Start grid
            Ignition.Start(new IgniteConfigurationEx
            {
                GridName = gridName,
                SpringConfigUrl = "config\\compute\\compute-grid1.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            });

            cacheProvider.Initialize("testName", new NameValueCollection
            {
                {GridNameAttr, gridName},
                {CacheNameAttr, cacheName}
            });

            // Test cache operations

        }
    }
}
