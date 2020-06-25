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
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using NUnit.Framework;

    /// <summary>
    /// Tests for thin client continuous queries.
    /// </summary>
    public class ContinuousQueryTest : ClientTestBase
    {
        /// <summary>
        /// Basic continuous query test.
        /// </summary>
        [Test]
        public void TestContinuousQueryCallsLocalListenerWithCorrectEvent()
        {
            var cache = Client.GetOrCreateCache<int, int>(TestUtils.TestName);

            var events = new List<ICacheEntryEvent<int, int>>();
            var qry = new ContinuousQuery<int, int>(new DelegateListener<int, int>(events.Add));
            using (cache.QueryContinuous(qry))
            {
                // Create.
                cache.Put(1, 1);
                TestUtils.WaitForTrueCondition(() => events.Count == 1);

                var evt = events.Single();
                Assert.AreEqual(CacheEntryEventType.Created, evt.EventType);
                Assert.IsFalse(evt.HasOldValue);
                Assert.IsTrue(evt.HasValue);
                Assert.AreEqual(1, evt.Key);
                Assert.AreEqual(1, evt.Value);

                // Update.
                cache.Put(1, 2);
                TestUtils.WaitForTrueCondition(() => events.Count == 2);

                evt = events.Last();
                Assert.AreEqual(CacheEntryEventType.Updated, evt.EventType);
                Assert.IsTrue(evt.HasOldValue);
                Assert.IsTrue(evt.HasValue);
                Assert.AreEqual(1, evt.Key);
                Assert.AreEqual(2, evt.Value);
                Assert.AreEqual(1, evt.OldValue);

                // Remove.
                cache.Remove(1);
                TestUtils.WaitForTrueCondition(() => events.Count == 3);

                evt = events.Last();
                Assert.AreEqual(CacheEntryEventType.Removed, evt.EventType);
                Assert.IsTrue(evt.HasOldValue);
                Assert.IsTrue(evt.HasValue);
                Assert.AreEqual(1, evt.Key);
                Assert.AreEqual(2, evt.Value);
                Assert.AreEqual(2, evt.OldValue);
            }
        }

        /// <summary>
        /// Tests that Compute notifications and Continuous Query notifications work together correctly.
        /// </summary>
        [Test]
        public void TestComputeWorksWhenContinuousQueryIsActive()
        {
            // TODO: Start multiple queries with different filters,
            // do cache updates and compute calls in multiple threads.
        }

        /// <summary>
        /// Tests that continuous query with filter receives only matching events.
        /// </summary>
        [Test]
        public void TestContinuousQueryWithFilterReceivesOnlyMatchingEvents()
        {
            // TODO:
        }

        /// <summary>
        /// Tests that server starts sending notifications only when client is ready to receive them.
        /// There is a brief moment when server starts the continuous query,
        /// but client has not received the response with query ID yet - server should not send notifications.
        /// </summary>
        [Test]
        public void TestContinuousQueryStartWithBackgroundCacheUpdatesReceivesEventsCorrectly()
        {
            // TODO:
        }

        [Test]
        public void TestInitialSqlQuery()
        {
            // TODO
        }

        [Test]
        public void TestInitialScanQuery()
        {
            // TODO
        }

        /** */
        private class DelegateListener<TK, TV> : ICacheEntryEventListener<TK, TV>
        {
            /** */
            private Action<ICacheEntryEvent<TK, TV>> _action;

            /** */
            public DelegateListener(Action<ICacheEntryEvent<TK, TV>> action)
            {
                _action = action;
            }

            /** */
            public void OnEvent(IEnumerable<ICacheEntryEvent<TK, TV>> evts)
            {
                foreach (var evt in evts)
                {
                    _action(evt);
                }
            }
        }
    }
}
