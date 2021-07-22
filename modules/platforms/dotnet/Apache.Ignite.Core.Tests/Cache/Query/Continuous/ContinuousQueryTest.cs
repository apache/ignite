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


namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests continuous queries.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ContinuousQueryTest
    {
        /** */
        private bool _enableSecurity;
        
        /** */
        private readonly ListLogger _logger = new ListLogger(new ConsoleLogger())
        {
            EnabledLevels = new[] {LogLevel.Error}
        };

        /// <summary>
        /// Tests same query on multiple nodes.
        /// This tests verifies that there are no exception on Java side during event delivery.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestSameQueryMultipleNodes()
        {
            CheckSameQueryMultipleNodes();
        }
        
        /// <summary>
        /// Tests same query on multiple nodes with enabled security.
        /// This test verifies that on the Java side the security wrapper for the event filter does not produce errors  
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestSameQueryMultipleNodesSecurityEnabled()
        {
            _enableSecurity = true;

            CheckSameQueryMultipleNodes();
            
            var errs = _logger.Entries.FindAll(e => e.Message.Contains("CacheEntryEventFilter failed"));

            Assert.AreEqual(0, errs.Count);
        }

        /// <summary>
        /// Tests same query on multiple nodes.
        /// </summary>
        private void CheckSameQueryMultipleNodes()
        {
            using (var ignite = StartIgnite())
            {
                ignite.GetCluster().SetActive(true);

                var cache = ignite.GetOrCreateCache<Guid, Data>("data");
                cache.QueryContinuous(new ContinuousQuery<Guid, Data>(new Listener()));

                using (var ignite2 = StartIgnite())
                {
                    var cache2 = ignite2.GetOrCreateCache<Guid, Data>("data");
                    cache2.QueryContinuous(new ContinuousQuery<Guid, Data>(new Listener()));

                    for (var i = 0; i < 100; i++)
                    {
                        PutEntry(cache2);
                        PutEntry(cache);
                    }
                }
            }
        }

        /// <summary>
        /// Puts the entry and verifies events.
        /// </summary>
        private static void PutEntry(ICache<Guid, Data> cache)
        {
            // Put new entry.
            var entry = new Data {Id = Guid.NewGuid()};
            cache.Put(entry.Id, entry);

            // Wait for events.
            TestUtils.WaitForTrueCondition(() => Listener.Events.Count == 2);

            ICacheEntryEvent<Guid, Data> e;

            // Two listeners  - two events.
            Assert.IsTrue(Listener.Events.TryPop(out e));
            Assert.AreEqual(entry.Id, e.Key);

            Assert.IsTrue(Listener.Events.TryPop(out e));
            Assert.AreEqual(entry.Id, e.Key);
        }

        /// <summary>
        /// Starts the ignite.
        /// </summary>
        private IIgnite StartIgnite()
        {
            var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Data)),
                AutoGenerateIgniteInstanceName = true,
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        PersistenceEnabled = _enableSecurity,
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                    }
                },
                AuthenticationEnabled = _enableSecurity,
                WorkDirectory = PathUtils.GetTempDirectoryName(),
                Logger = _logger,
                IsActiveOnStart = false
            });
            
            ignite.GetCluster().SetBaselineAutoAdjustEnabledFlag(true);

            return ignite;
        }

        private class Data
        {
            public Guid Id;
        }

        private class Listener : ICacheEntryEventListener<Guid, Data>
        {
            public static readonly ConcurrentStack<ICacheEntryEvent<Guid, Data>> Events
                = new ConcurrentStack<ICacheEntryEvent<Guid, Data>>();

            public void OnEvent(IEnumerable<ICacheEntryEvent<Guid, Data>> evts)
            {
                foreach (var e in evts)
                {
                    Events.Push(e);
                }
            }
        }
        
        [TearDown]
        public void TearDown()
        {
            _logger.Clear();
            Directory.Delete(PathUtils.GetTempDirectoryName(), true);
        }
    }
}
