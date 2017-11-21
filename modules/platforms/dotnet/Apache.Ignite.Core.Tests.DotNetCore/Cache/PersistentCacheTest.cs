﻿/*
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

namespace Apache.Ignite.Core.Tests.DotNetCore.Cache
{
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Tests.DotNetCore.Common;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests persistent cache.
    /// </summary>
    [TestClass]
    public class PersistentCacheTest : TestBase
    {
        /** */
        private const string CacheName = "persistentCache";

        /// <summary>
        /// Tests that cache data survives node restart.
        /// </summary>
        [TestMethod]
        public void TestCacheDataSurvivesNodeRestart()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "def",
                        PersistenceEnabled = true
                    }
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                ignite.SetActive(true);

                var cache = ignite.GetOrCreateCache<int, Person>(CacheName);
                cache.RemoveAll();
                
                Assert.AreEqual(0, cache.GetSize());
                cache[1] = new Person("Petya", 25);
            }

            using (var ignite = Ignition.Start(cfg))
            {
                ignite.SetActive(true);

                var cache = ignite.GetCache<int, Person>(CacheName);
                
                Assert.AreEqual(1, cache.GetSize());
                Assert.AreEqual("Petya", cache[1].Name);
            }
        }
    }
}
