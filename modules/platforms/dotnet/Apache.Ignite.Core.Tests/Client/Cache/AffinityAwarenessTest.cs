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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests affinity awareness functionality.
    /// </summary>
    public class AffinityAwarenessTest : ClientTestBase
    {
        // TODO:
        // * Test disabled/enabled
        // * Test request routing (using local cache events)
        // * Test hash code for all primitives
        // * Test hash code for complex key
        // * Test hash code for complex key with AffinityKeyMapped
        // * Test topology update

        /** */
        private readonly List<ListLogger> _loggers = new List<ListLogger>();

        /** */
        private ICacheClient<int, int> _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityAwarenessTest"/> class.
        /// </summary>
        public AffinityAwarenessTest() : base(3)
        {
            // No-op.
        }

        /// <summary>
        /// Fixture set up.
        /// </summary>
        public override void FixtureSetUp()
        {
            base.FixtureSetUp();

            _cache = Client.CreateCache<int, int>("c");
            _cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));

            // Warm up client partition data.
            _cache.Get(1);
            _cache.Get(2);
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            ClearLoggers();
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            var cfg = base.GetIgniteConfiguration();

            var logger = new ListLogger();
            cfg.Logger = logger;
            _loggers.Add(logger);

            return cfg;
        }

        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            var cfg = base.GetClientConfiguration();

            cfg.EnableAffinityAwareness = true;
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 1));
            cfg.Endpoints.Add(string.Format("{0}:{1}", IPAddress.Loopback, IgniteClientConfiguration.DefaultPort + 2));

            return cfg;
        }

        private int GetClientRequestGridIndex()
        {
            try
            {
                for (var i = 0; i < _loggers.Count; i++)
                {
                    var logger = _loggers[i];

                    if (logger.Messages.Any(m => m.Contains("ClientCacheGetRequest")))
                    {
                        return i;
                    }
                }

                return -1;
            }
            finally
            {
                ClearLoggers();
            }
        }


        private void ClearLoggers()
        {
            foreach (var logger in _loggers)
            {
                logger.Clear();
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        [TestCase(5, 1)]
        [TestCase(6, 2)]
        public void CacheGet_PrimitiveKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.Get(key);

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 0)]
        [TestCase(3, 0)]
        [TestCase(4, 0)]
        [TestCase(5, 0)]
        [TestCase(6, 1)]
        public void CacheGet_UserDefinedKeyType_RequestIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var cache = Client.GetOrCreateCache<TestKey, int>("c_custom_key");
            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => new TestKey(x, x.ToString()), x => x));
            cache.Get(new TestKey(1, "1")); // Warm up;

            var res = cache.Get(new TestKey(key, key.ToString()));

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetClientRequestGridIndex());
        }

        [Test]
        public void CachePut_UserDefinedTypeWithAffinityKey_ThrowsIgniteException()
        {
            // TODO: This is broken in Java, we don't get any data on annotation-configured types.
            var cache = Client.GetOrCreateCache<TestKeyWithAffinity, int>("c_custom_key_aff");

            var ex = Assert.Throws<IgniteException>(() => cache.Put(new TestKeyWithAffinity(1, "1"), 1));
            Assert.AreEqual("TODO", ex.Message);
        }

        [Test]
        public void CacheGet_NewNodeEnteredTopology_RequestIsRoutedToPrimaryNode()
        {
            // Warm-up.
            Assert.AreEqual(1, _cache.Get(1));

            // Before topology change.
            Assert.AreEqual(12, _cache.Get(1));
            Assert.AreEqual(1, GetClientRequestGridIndex());

            Assert.AreEqual(14, _cache.Get(1));
            Assert.AreEqual(2, GetClientRequestGridIndex());

            // After topology change.
            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                // Warm-up.
                Assert.AreEqual(1, _cache.Get(1));

                // TODO: Wait for rebalance event - how?
                Thread.Sleep(5000);

                Assert.AreEqual(12, _cache.Get(1));
                Assert.AreEqual(1, GetClientRequestGridIndex());

                Assert.AreEqual(14, _cache.Get(1));
                Assert.AreEqual(2, GetClientRequestGridIndex());
            }
        }
    }
}
