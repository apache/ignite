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
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Events;
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
        private readonly List<CacheTestEventListener> _listeners = new List<CacheTestEventListener>();

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

            var grids = Ignition.GetAll();
            foreach (var grid in grids)
            {
                var events = grid.GetEvents();
                events.EnableLocal(EventType.CacheObjectRead);

                var listener = new CacheTestEventListener(grid);
                events.LocalListen(listener, EventType.CacheObjectRead);

                _listeners.Add(listener);

                var consistentId = grid.GetCluster().GetLocalNode().ConsistentId;
                Console.WriteLine(consistentId);
            }

            _cache = GetClient().CreateCache<int, int>("c");
            _cache.PutAll(Enumerable.Range(1, 100).ToDictionary(x => x, x => x));
        }

        public override void TestSetUp()
        {
            base.TestSetUp();

            foreach (var listener in _listeners)
            {
                listener.Events.Clear();
            }
        }

        [Test]
        [TestCase(1, 1)]
        [TestCase(2, 2)]
        [TestCase(3, 0)]
        [TestCase(4, 1)]
        public void TestGetIsRoutedToPrimaryNode(int key, int gridIdx)
        {
            var res = _cache.Get(key);

            Assert.AreEqual(key, res);
            Assert.AreEqual(gridIdx, GetCacheEventGridIndex());
        }

        private int GetCacheEventGridIndex()
        {
            for (var i = 0; i < _listeners.Count; i++)
            {
                var listener = _listeners[i];

                if (listener.Events.Count > 0)
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
