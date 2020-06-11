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

#pragma warning disable 618
namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cache.Event;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for continuous query.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
    [SuppressMessage("ReSharper", "StaticMemberInGenericType")]
    public abstract class ContinuousQueryAbstractTest
    {
        /** Cache name: ATOMIC, backup. */
        protected const string CACHE_ATOMIC_BACKUP = "atomic_backup";

        /** Cache name: ATOMIC, no backup. */
        protected const string CACHE_ATOMIC_NO_BACKUP = "atomic_no_backup";

        /** Cache name: TRANSACTIONAL, backup. */
        protected const string CACHE_TX_BACKUP = "transactional_backup";

        /** Cache name: TRANSACTIONAL, no backup. */
        protected const string CACHE_TX_NO_BACKUP = "transactional_no_backup";

        /** Listener events. */
        public static BlockingCollection<CallbackEvent> CB_EVTS = new BlockingCollection<CallbackEvent>();

        /** Listener events. */
        public static BlockingCollection<FilterEvent> FILTER_EVTS = new BlockingCollection<FilterEvent>();

        /** First node. */
        private IIgnite grid1;

        /** Second node. */
        private IIgnite grid2;

        /** Cache on the first node. */
        private ICache<int, BinarizableEntry> cache1;

        /** Cache on the second node. */
        private ICache<int, BinarizableEntry> cache2;

        /** Cache name. */
        private readonly string cacheName;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        protected ContinuousQueryAbstractTest(string cacheName)
        {
            this.cacheName = cacheName;
        }

        /// <summary>
        /// Set-up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void SetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new List<BinaryTypeConfiguration>
                    {
                        new BinaryTypeConfiguration(typeof(BinarizableEntry)),
                        new BinaryTypeConfiguration(typeof(BinarizableFilter)),
                        new BinaryTypeConfiguration(typeof(KeepBinaryFilter))
                    }
                },
                SpringConfigUrl = Path.Combine("Config", "cache-query-continuous.xml"),
                IgniteInstanceName = "grid-1"
            };

            grid1 = Ignition.Start(cfg);
            cache1 = grid1.GetCache<int, BinarizableEntry>(cacheName);

            cfg.IgniteInstanceName = "grid-2";
            grid2 = Ignition.Start(cfg);
            cache2 = grid2.GetCache<int, BinarizableEntry>(cacheName);
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Before-test routine.
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            CB_EVTS = new BlockingCollection<CallbackEvent>();
            FILTER_EVTS = new BlockingCollection<FilterEvent>();

            AbstractFilter<BinarizableEntry>.res = true;
            AbstractFilter<BinarizableEntry>.err = false;
            AbstractFilter<BinarizableEntry>.marshErr = false;
            AbstractFilter<BinarizableEntry>.unmarshErr = false;

            cache1.Remove(PrimaryKey(cache1));
            cache1.Remove(PrimaryKey(cache2));

            Assert.AreEqual(0, cache1.GetSize());
            Assert.AreEqual(0, cache2.GetSize());

            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        /// Test arguments validation.
        /// </summary>
        [Test]
        public void TestValidation()
        {
            Assert.Throws<ArgumentException>(() => { cache1.QueryContinuous(new ContinuousQuery<int, BinarizableEntry>(null)); });
        }

        /// <summary>
        /// Test multiple closes.
        /// </summary>
        [Test]
        public void TestMultipleClose()
        {
            int key1 = PrimaryKey(cache1);
            int key2 = PrimaryKey(cache2);

            Assert.AreNotEqual(key1, key2);

            ContinuousQuery<int, BinarizableEntry> qry =
                new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            IDisposable qryHnd;

            using (qryHnd = cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1), CacheEntryEventType.Created);

                // Put from remote node.
                cache2.GetAndPut(key2, Entry(key2));
                CheckCallbackSingle(key2, null, Entry(key2), CacheEntryEventType.Created);
            }

            qryHnd.Dispose();
        }

        /// <summary>
        /// Test regular callback operations.
        /// </summary>
        [Test]
        public void TestCallback()
        {
            CheckCallback(false);
        }

        /// <summary>
        /// Check regular callback execution.
        /// </summary>
        /// <param name="loc"></param>
        protected void CheckCallback(bool loc)
        {
            int key1 = PrimaryKey(cache1);
            int key2 = PrimaryKey(cache2);

            ContinuousQuery<int, BinarizableEntry> qry = loc ?
                new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>(), true) :
                new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1), CacheEntryEventType.Created);

                cache1.GetAndPut(key1, Entry(key1 + 1));
                CheckCallbackSingle(key1, Entry(key1), Entry(key1 + 1), CacheEntryEventType.Updated);

                cache1.Remove(key1);
                CheckCallbackSingle(key1, Entry(key1 + 1), Entry(key1 + 1), CacheEntryEventType.Removed);

                // Put from remote node.
                cache2.GetAndPut(key2, Entry(key2));

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, null, Entry(key2), CacheEntryEventType.Created);

                cache1.GetAndPut(key2, Entry(key2 + 1));

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, Entry(key2), Entry(key2 + 1), CacheEntryEventType.Updated);

                cache1.Remove(key2);

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, Entry(key2 + 1), Entry(key2 + 1), CacheEntryEventType.Removed);
            }

            cache1.Put(key1, Entry(key1));
            CheckNoCallback(100);

            cache1.Put(key2, Entry(key2));
            CheckNoCallback(100);
        }

        /// <summary>
        /// Test Ignite injection into callback.
        /// </summary>
        [Test]
        public void TestCallbackInjection()
        {
            Listener<BinarizableEntry> cb = new Listener<BinarizableEntry>();

            Assert.IsNull(cb.ignite);

            using (cache1.QueryContinuous(new ContinuousQuery<int, BinarizableEntry>(cb)))
            {
                Assert.IsNotNull(cb.ignite);
            }
        }

        /// <summary>
        /// Test binarizable filter logic.
        /// </summary>
        [Test]
        public void TestFilterBinarizable()
        {
            CheckFilter(true, false);
        }

        /// <summary>
        /// Test serializable filter logic.
        /// </summary>
        [Test]
        public void TestFilterSerializable()
        {
            CheckFilter(false, false);
        }

        /// <summary>
        /// Tests the defaults.
        /// </summary>
        [Test]
        public void TestDefaults()
        {
            var qry = new ContinuousQuery<int, int>(null);

            Assert.AreEqual(ContinuousQuery.DefaultAutoUnsubscribe, qry.AutoUnsubscribe);
            Assert.AreEqual(ContinuousQuery.DefaultBufferSize, qry.BufferSize);
            Assert.AreEqual(ContinuousQuery.DefaultTimeInterval, qry.TimeInterval);
            Assert.IsFalse(qry.Local);
        }

        /// <summary>
        /// Check filter.
        /// </summary>
        /// <param name="binarizable">Binarizable.</param>
        /// <param name="loc">Local cache flag.</param>
        protected void CheckFilter(bool binarizable, bool loc)
        {
            ICacheEntryEventListener<int, BinarizableEntry> lsnr = new Listener<BinarizableEntry>();
            ICacheEntryEventFilter<int, BinarizableEntry> filter =
                binarizable ? (AbstractFilter<BinarizableEntry>) new BinarizableFilter() : new SerializableFilter();

            ContinuousQuery<int, BinarizableEntry> qry = loc ?
                new ContinuousQuery<int, BinarizableEntry>(lsnr, filter, true) :
                new ContinuousQuery<int, BinarizableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                int key1 = PrimaryKey(cache1);
                cache1.GetAndPut(key1, Entry(key1));
                CheckFilterSingle(key1, null, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1), CacheEntryEventType.Created);

                // Put from remote node.
                int key2 = PrimaryKey(cache2);
                cache1.GetAndPut(key2, Entry(key2));

                if (loc)
                {
                    CheckNoFilter(key2);
                    CheckNoCallback(key2);
                }
                else
                {
                    CheckFilterSingle(key2, null, Entry(key2));
                    CheckCallbackSingle(key2, null, Entry(key2), CacheEntryEventType.Created);
                }

                AbstractFilter<BinarizableEntry>.res = false;

                // Ignored put from local node.
                cache1.GetAndPut(key1, Entry(key1 + 1));
                CheckFilterSingle(key1, Entry(key1), Entry(key1 + 1));
                CheckNoCallback(100);

                // Ignored put from remote node.
                cache1.GetAndPut(key2, Entry(key2 + 1));

                if (loc)
                    CheckNoFilter(100);
                else
                    CheckFilterSingle(key2, Entry(key2), Entry(key2 + 1));

                CheckNoCallback(100);
            }
        }

        /// <summary>
        /// Test binarizable filter error during invoke.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterInvokeErrorBinarizable()
        {
            CheckFilterInvokeError(true);
        }

        /// <summary>
        /// Test serializable filter error during invoke.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterInvokeErrorSerializable()
        {
            CheckFilterInvokeError(false);
        }

        /// <summary>
        /// Check filter error handling logic during invoke.
        /// </summary>
        private void CheckFilterInvokeError(bool binarizable)
        {
            AbstractFilter<BinarizableEntry>.err = true;

            ICacheEntryEventListener<int, BinarizableEntry> lsnr = new Listener<BinarizableEntry>();
            ICacheEntryEventFilter<int, BinarizableEntry> filter =
                binarizable ? (AbstractFilter<BinarizableEntry>) new BinarizableFilter() : new SerializableFilter();

            ContinuousQuery<int, BinarizableEntry> qry = new ContinuousQuery<int, BinarizableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                Assert.Throws<IgniteException>(() => cache1.GetAndPut(PrimaryKey(cache1), Entry(1)));

                // Put from remote node.
                Assert.Throws<IgniteException>(() => cache1.GetAndPut(PrimaryKey(cache2), Entry(1)));
            }
        }

        /// <summary>
        /// Test binarizable filter marshalling error.
        /// </summary>
        [Test]
        public void TestFilterMarshalErrorBinarizable()
        {
            CheckFilterMarshalError(true);
        }

        /// <summary>
        /// Test serializable filter marshalling error.
        /// </summary>
        [Test]
        public void TestFilterMarshalErrorSerializable()
        {
            CheckFilterMarshalError(false);
        }

        /// <summary>
        /// Check filter marshal error handling.
        /// </summary>
        /// <param name="binarizable">Binarizable flag.</param>
        private void CheckFilterMarshalError(bool binarizable)
        {
            AbstractFilter<BinarizableEntry>.marshErr = true;

            ICacheEntryEventListener<int, BinarizableEntry> lsnr = new Listener<BinarizableEntry>();
            ICacheEntryEventFilter<int, BinarizableEntry> filter =
                binarizable ? (AbstractFilter<BinarizableEntry>)new BinarizableFilter() : new SerializableFilter();

            ContinuousQuery<int, BinarizableEntry> qry = new ContinuousQuery<int, BinarizableEntry>(lsnr, filter);

            Assert.Throws<Exception>(() =>
            {
                using (cache1.QueryContinuous(qry))
                {
                    // No-op.
                }
            });
        }

        /// <summary>
        /// Test non-serializable filter error.
        /// </summary>
        [Test]
        public void TestFilterNonSerializable()
        {
            CheckFilterNonSerializable(false);
        }

        /// <summary>
        /// Test non-serializable filter behavior.
        /// </summary>
        /// <param name="loc"></param>
        protected void CheckFilterNonSerializable(bool loc)
        {
            AbstractFilter<BinarizableEntry>.unmarshErr = true;

            ICacheEntryEventListener<int, BinarizableEntry> lsnr = new Listener<BinarizableEntry>();
            ICacheEntryEventFilter<int, BinarizableEntry> filter = new LocalFilter();

            ContinuousQuery<int, BinarizableEntry> qry = loc
                ? new ContinuousQuery<int, BinarizableEntry>(lsnr, filter, true)
                : new ContinuousQuery<int, BinarizableEntry>(lsnr, filter);

            if (loc)
            {
                using (cache1.QueryContinuous(qry))
                {
                    // Local put must be fine.
                    int key1 = PrimaryKey(cache1);
                    cache1.GetAndPut(key1, Entry(key1));
                    CheckFilterSingle(key1, null, Entry(key1));
                }
            }
            else
            {
                Assert.Throws<BinaryObjectException>(() =>
                {
                    using (cache1.QueryContinuous(qry))
                    {
                        // No-op.
                    }
                });
            }
        }

        /// <summary>
        /// Test binarizable filter unmarshalling error.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterUnmarshalErrorBinarizable()
        {
            CheckFilterUnmarshalError(true);
        }

        /// <summary>
        /// Test serializable filter unmarshalling error.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterUnmarshalErrorSerializable()
        {
            CheckFilterUnmarshalError(false);
        }

        /// <summary>
        /// Check filter unmarshal error handling.
        /// </summary>
        /// <param name="binarizable">Binarizable flag.</param>
        private void CheckFilterUnmarshalError(bool binarizable)
        {
            AbstractFilter<BinarizableEntry>.unmarshErr = true;

            ICacheEntryEventListener<int, BinarizableEntry> lsnr = new Listener<BinarizableEntry>();
            ICacheEntryEventFilter<int, BinarizableEntry> filter =
                binarizable ? (AbstractFilter<BinarizableEntry>) new BinarizableFilter() : new SerializableFilter();

            ContinuousQuery<int, BinarizableEntry> qry = new ContinuousQuery<int, BinarizableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Local put must be fine.
                int key1 = PrimaryKey(cache1);
                cache1.GetAndPut(key1, Entry(key1));
                CheckFilterSingle(key1, null, Entry(key1));

                // Remote put must fail.
                Assert.Throws<IgniteException>(() => cache1.GetAndPut(PrimaryKey(cache2), Entry(1)));
            }
        }

        /// <summary>
        /// Test Ignite injection into filters.
        /// </summary>
        [Test]
        public void TestFilterInjection()
        {
            Listener<BinarizableEntry> cb = new Listener<BinarizableEntry>();
            BinarizableFilter filter = new BinarizableFilter();

            Assert.IsNull(filter.ignite);

            using (cache1.QueryContinuous(new ContinuousQuery<int, BinarizableEntry>(cb, filter)))
            {
                // Local injection.
                Assert.IsNotNull(filter.ignite);

                // Remote injection.
                cache1.GetAndPut(PrimaryKey(cache2), Entry(1));

                FilterEvent evt;

                Assert.IsTrue(FILTER_EVTS.TryTake(out evt, 500));

                Assert.IsNotNull(evt.ignite);
            }
        }


        /// <summary>
        /// Test "keep-binary" scenario.
        /// </summary>
        [Test]
        public void TestKeepBinary()
        {
            var cache = cache1.WithKeepBinary<int, IBinaryObject>();

            ContinuousQuery<int, IBinaryObject> qry = new ContinuousQuery<int, IBinaryObject>(
                    new Listener<IBinaryObject>(), new KeepBinaryFilter());

            using (cache.QueryContinuous(qry))
            {
                // 1. Local put.
                cache1.GetAndPut(PrimaryKey(cache1), Entry(1));

                CallbackEvent cbEvt;
                FilterEvent filterEvt;

                Assert.IsTrue(FILTER_EVTS.TryTake(out filterEvt, 500));
                Assert.AreEqual(PrimaryKey(cache1), filterEvt.entry.Key);
                Assert.AreEqual(null, filterEvt.entry.OldValue);
                Assert.AreEqual(Entry(1), (filterEvt.entry.Value as IBinaryObject)
                    .Deserialize<BinarizableEntry>());

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                Assert.AreEqual(1, cbEvt.entries.Count);
                Assert.AreEqual(PrimaryKey(cache1), cbEvt.entries.First().Key);
                Assert.AreEqual(null, cbEvt.entries.First().OldValue);
                Assert.AreEqual(Entry(1), (cbEvt.entries.First().Value as IBinaryObject)
                    .Deserialize<BinarizableEntry>());

                // 2. Remote put.
                ClearEvents();
                cache1.GetAndPut(PrimaryKey(cache2), Entry(2));

                Assert.IsTrue(FILTER_EVTS.TryTake(out filterEvt, 500));
                Assert.AreEqual(PrimaryKey(cache2), filterEvt.entry.Key);
                Assert.AreEqual(null, filterEvt.entry.OldValue);
                Assert.AreEqual(Entry(2), (filterEvt.entry.Value as IBinaryObject)
                    .Deserialize<BinarizableEntry>());

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                Assert.AreEqual(1, cbEvt.entries.Count);
                Assert.AreEqual(PrimaryKey(cache2), cbEvt.entries.First().Key);
                Assert.AreEqual(null, cbEvt.entries.First().OldValue);
                Assert.AreEqual(Entry(2),
                    (cbEvt.entries.First().Value as IBinaryObject).Deserialize<BinarizableEntry>());
            }
        }
        /// <summary>
        /// Test value types (special handling is required for nulls).
        /// </summary>
        [Test]
        public void TestValueTypes()
        {
            var cache = grid1.GetCache<int, int>(cacheName);

            var qry = new ContinuousQuery<int, int>(new Listener<int>());

            var key = PrimaryKey(cache);

            using (cache.QueryContinuous(qry))
            {
                // First update
                cache.Put(key, 1);

                CallbackEvent cbEvt;

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                var cbEntry = cbEvt.entries.Single();
                Assert.IsFalse(cbEntry.HasOldValue);
                Assert.IsTrue(cbEntry.HasValue);
                Assert.AreEqual(key, cbEntry.Key);
                Assert.AreEqual(null, cbEntry.OldValue);
                Assert.AreEqual(1, cbEntry.Value);

                // Second update
                cache.Put(key, 2);

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                cbEntry = cbEvt.entries.Single();
                Assert.IsTrue(cbEntry.HasOldValue);
                Assert.IsTrue(cbEntry.HasValue);
                Assert.AreEqual(key, cbEntry.Key);
                Assert.AreEqual(1, cbEntry.OldValue);
                Assert.AreEqual(2, cbEntry.Value);

                // Remove
                cache.Remove(key);

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                cbEntry = cbEvt.entries.Single();
                Assert.IsTrue(cbEntry.HasOldValue);
                Assert.IsTrue(cbEntry.HasValue);
                Assert.AreEqual(key, cbEntry.Key);
                Assert.AreEqual(2, cbEntry.OldValue);
                Assert.AreEqual(2, cbEntry.Value);
            }
        }

        /// <summary>
        /// Test whether buffer size works fine.
        /// </summary>
        [Test]
        public void TestBufferSize()
        {
            // Put two remote keys in advance.
            var rmtKeys = TestUtils.GetPrimaryKeys(cache2.Ignite, cache2.Name).Take(2).ToList();

            ContinuousQuery<int, BinarizableEntry> qry = new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            qry.BufferSize = 2;
            qry.TimeInterval = TimeSpan.FromMilliseconds(1000000);

            using (cache1.QueryContinuous(qry))
            {
                qry.BufferSize = 2;

                cache1.GetAndPut(rmtKeys[0], Entry(rmtKeys[0]));

                CheckNoCallback(100);

                cache1.GetAndPut(rmtKeys[1], Entry(rmtKeys[1]));

                CallbackEvent evt;

                Assert.IsTrue(CB_EVTS.TryTake(out evt, 1000));

                Assert.AreEqual(2, evt.entries.Count);

                var entryRmt0 = evt.entries.Single(entry => { return entry.Key.Equals(rmtKeys[0]); });
                var entryRmt1 = evt.entries.Single(entry => { return entry.Key.Equals(rmtKeys[1]); });

                Assert.AreEqual(rmtKeys[0], entryRmt0.Key);
                Assert.IsNull(entryRmt0.OldValue);
                Assert.AreEqual(Entry(rmtKeys[0]), entryRmt0.Value);

                Assert.AreEqual(rmtKeys[1], entryRmt1.Key);
                Assert.IsNull(entryRmt1.OldValue);
                Assert.AreEqual(Entry(rmtKeys[1]), entryRmt1.Value);
            }

            cache1.Remove(rmtKeys[0]);
            cache1.Remove(rmtKeys[1]);
        }

        /// <summary>
        /// Test whether timeout works fine.
        /// </summary>
        [Test]
        public void TestTimeout()
        {
            int key1 = PrimaryKey(cache1);
            int key2 = PrimaryKey(cache2);

            ContinuousQuery<int, BinarizableEntry> qry =
                new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            qry.BufferSize = 2;
            qry.TimeInterval = TimeSpan.FromMilliseconds(500);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1), CacheEntryEventType.Created);

                // Put from remote node.
                cache1.GetAndPut(key2, Entry(key2));
                CheckNoCallback(100);
                CheckCallbackSingle(key2, null, Entry(key2), CacheEntryEventType.Created);
            }
        }

        /// <summary>
        /// Test whether nested Ignite API call from callback works fine.
        /// </summary>
        [Test]
        public void TestNestedCallFromCallback()
        {
            var cache = cache1.WithKeepBinary<int, IBinaryObject>();

            int key = PrimaryKey(cache1);

            NestedCallListener cb = new NestedCallListener();

            using (cache.QueryContinuous(new ContinuousQuery<int, IBinaryObject>(cb)))
            {
                cache1.GetAndPut(key, Entry(key));

                cb.countDown.Wait();
            }

            cache.Remove(key);
        }

        /// <summary>
        /// Tests the initial query.
        /// </summary>
        [Test]
        public void TestInitialQuery()
        {
            // Scan query, GetAll
            TestInitialQuery(new ScanQuery<int, BinarizableEntry>(new InitialQueryScanFilter()), cur => cur.GetAll());

            // Scan query, iterator
            TestInitialQuery(new ScanQuery<int, BinarizableEntry>(new InitialQueryScanFilter()), cur => cur.ToList());

            // Sql query, GetAll
            TestInitialQuery(new SqlQuery(typeof(BinarizableEntry), "val < 33"), cur => cur.GetAll());

            // Sql query, iterator
            TestInitialQuery(new SqlQuery(typeof(BinarizableEntry), "val < 33"), cur => cur.ToList());

            // Text query, GetAll
            TestInitialQuery(new TextQuery(typeof(BinarizableEntry), "1*"), cur => cur.GetAll());

            // Text query, iterator
            TestInitialQuery(new TextQuery(typeof(BinarizableEntry), "1*"), cur => cur.ToList());

            // Test exception: invalid initial query
            var ex = Assert.Throws<IgniteException>(
                () => TestInitialQuery(new TextQuery(typeof (BinarizableEntry), "*"), cur => cur.GetAll()));

            Assert.AreEqual("Cannot parse '*': '*' or '?' not allowed as first character in WildcardQuery", ex.Message);
        }

        /// <summary>
        /// Tests the initial fields query.
        /// </summary>
        [Test]
        public void TestInitialFieldsQuery()
        {
            var sqlFieldsQuery = new SqlFieldsQuery("select _key, _val, val from BINARIZABLEENTRY where val < 33");

            TestInitialQuery(sqlFieldsQuery, cur => cur.GetAll());
            TestInitialQuery(sqlFieldsQuery, cur => cur.ToList());
        }

        /// <summary>
        /// Tests fields metadata in the initial fields query.
        /// </summary>
        [Test]
        public void TestInitialFieldsQueryMetadata()
        {
            var sqlFieldsQuery = new SqlFieldsQuery("select val, _val from BINARIZABLEENTRY where val < 33");
            var qry = new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            using (var contQry = cache1.QueryContinuous(qry, sqlFieldsQuery))
            {
                var fields = contQry.GetInitialQueryCursor().Fields;

                Assert.AreEqual(2, fields.Count);

                Assert.AreEqual("VAL", fields[0].Name);
                Assert.AreEqual(typeof(int), fields[0].Type);

                Assert.AreEqual("_VAL", fields[1].Name);
                Assert.AreEqual(typeof(object), fields[1].Type);
            }
        }

        /// <summary>
        /// Tests the initial fields query with bad SQL.
        /// </summary>
        [Test]
        public void TestInitialFieldsQueryWithBadSql()
        {
            // Invalid SQL query.
            var ex = Assert.Throws<IgniteException>(() => TestInitialQuery(
                new SqlFieldsQuery("select FOO from BAR"), cur => cur.GetAll()));

            StringAssert.StartsWith("Failed to parse query. Table \"BAR\" not found;", ex.Message);
        }

        /// <summary>
        /// Tests the initial query.
        /// </summary>
        private void TestInitialQuery(QueryBase initialQry, Func<IQueryCursor<ICacheEntry<int, BinarizableEntry>>,
            IEnumerable<ICacheEntry<int, BinarizableEntry>>> getAllFunc)
        {
            var qry = new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            cache1.Put(11, Entry(11));
            cache1.Put(12, Entry(12));
            cache1.Put(33, Entry(33));

            try
            {
                IContinuousQueryHandle<ICacheEntry<int, BinarizableEntry>> contQry;

                using (contQry = cache1.QueryContinuous(qry, initialQry))
                {
                    // Check initial query
                    var initialEntries =
                        getAllFunc(contQry.GetInitialQueryCursor()).Distinct().OrderBy(x => x.Key).ToList();

                    Assert.Throws<InvalidOperationException>(() => contQry.GetInitialQueryCursor());

                    Assert.AreEqual(2, initialEntries.Count);

                    for (int i = 0; i < initialEntries.Count; i++)
                    {
                        Assert.AreEqual(i + 11, initialEntries[i].Key);
                        Assert.AreEqual(i + 11, initialEntries[i].Value.val);
                    }

                    // Check continuous query
                    cache1.Put(44, Entry(44));
                    CheckCallbackSingle(44, null, Entry(44), CacheEntryEventType.Created);
                }

                Assert.Throws<ObjectDisposedException>(() => contQry.GetInitialQueryCursor());

                contQry.Dispose();  // multiple dispose calls are ok
            }
            finally
            {
                cache1.Clear();
            }
        }

        /// <summary>
        /// Tests the initial fields query.
        /// </summary>
        private void TestInitialQuery(SqlFieldsQuery initialQry,
            Func<IFieldsQueryCursor, IEnumerable<IList<object>>> getAllFunc)
        {
            var qry = new ContinuousQuery<int, BinarizableEntry>(new Listener<BinarizableEntry>());

            cache1.Put(11, Entry(11));
            cache1.Put(12, Entry(12));
            cache1.Put(33, Entry(33));

            try
            {
                IContinuousQueryHandleFields contQry;

                using (contQry = cache1.QueryContinuous(qry, initialQry))
                {
                    // Check initial query
                    var initialQueryCursor = contQry.GetInitialQueryCursor();
                    var initialEntries = getAllFunc(initialQueryCursor).OrderBy(x => x[0]).ToList();

                    Assert.Throws<InvalidOperationException>(() => contQry.GetInitialQueryCursor());

                    Assert.AreEqual(2, initialEntries.Count);
                    Assert.GreaterOrEqual(initialQueryCursor.Fields.Count, 2);

                    for (int i = 0; i < initialEntries.Count; i++)
                    {
                        var key = (int) initialEntries[i][0];
                        var val = (BinarizableEntry) initialEntries[i][1];

                        Assert.AreEqual(i + 11, key);
                        Assert.AreEqual(i + 11, val.val);
                    }

                    // Check continuous query
                    cache1.Put(44, Entry(44));
                    CheckCallbackSingle(44, null, Entry(44), CacheEntryEventType.Created);
                }

                Assert.Throws<ObjectDisposedException>(() => contQry.GetInitialQueryCursor());

                contQry.Dispose();  // multiple dispose calls are ok
            }
            finally
            {
                cache1.Clear();
            }
        }

        /// <summary>
        /// Check single filter event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected value.</param>
        private void CheckFilterSingle(int expKey, BinarizableEntry expOldVal, BinarizableEntry expVal)
        {
            CheckFilterSingle(expKey, expOldVal, expVal, 1000);
            ClearEvents();
        }

        /// <summary>
        /// Check single filter event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected value.</param>
        /// <param name="timeout">Timeout.</param>
        private static void CheckFilterSingle(int expKey, BinarizableEntry expOldVal, BinarizableEntry expVal, int timeout)
        {
            FilterEvent evt;

            Assert.IsTrue(FILTER_EVTS.TryTake(out evt, timeout));

            Assert.AreEqual(expKey, evt.entry.Key);
            Assert.AreEqual(expOldVal, evt.entry.OldValue);
            Assert.AreEqual(expVal, evt.entry.Value);

            ClearEvents();
        }

        /// <summary>
        /// Clears the events collection.
        /// </summary>
        private static void ClearEvents()
        {
            while (FILTER_EVTS.Count > 0)
                FILTER_EVTS.Take();
        }

        /// <summary>
        /// Ensure that no filter events are logged.
        /// </summary>
        /// <param name="timeout">Timeout.</param>
        private static void CheckNoFilter(int timeout)
        {
            FilterEvent _;

            Assert.IsFalse(FILTER_EVTS.TryTake(out _, timeout));
        }

        /// <summary>
        /// Check single callback event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected new value.</param>
        /// <param name="expType">Expected type.</param>
        /// <param name="timeout">Timeout.</param>
        private static void CheckCallbackSingle(int expKey, BinarizableEntry expOldVal, BinarizableEntry expVal,
            CacheEntryEventType expType, int timeout = 1000)
        {
            CallbackEvent evt;

            Assert.IsTrue(CB_EVTS.TryTake(out evt, timeout));
            Assert.AreEqual(0, CB_EVTS.Count);

            var e = evt.entries.Single();

            Assert.AreEqual(expKey, e.Key);
            Assert.AreEqual(expOldVal, e.OldValue);
            Assert.AreEqual(expVal, e.Value);
            Assert.AreEqual(expType, e.EventType);
        }

        /// <summary>
        /// Ensure that no callback events are logged.
        /// </summary>
        /// <param name="timeout">Timeout.</param>
        private void CheckNoCallback(int timeout)
        {
            CallbackEvent _;

            Assert.IsFalse(CB_EVTS.TryTake(out _, timeout));
        }

        /// <summary>
        /// Create entry.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <returns>Entry.</returns>
        private static BinarizableEntry Entry(int val)
        {
            return new BinarizableEntry(val);
        }

        /// <summary>
        /// Get primary key for cache.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <returns>Primary key.</returns>
        private static int PrimaryKey<T>(ICache<int, T> cache)
        {
            return TestUtils.GetPrimaryKey(cache.Ignite, cache.Name);
        }

        /// <summary>
        /// Creates object-typed event.
        /// </summary>
        private static ICacheEntryEvent<object, object> CreateEvent<T, V>(ICacheEntryEvent<T,V> e)
        {
            switch (e.EventType)
            {
                case CacheEntryEventType.Created:
                    return new CacheEntryCreateEvent<object, object>(e.Key, e.Value);
                case CacheEntryEventType.Updated:
                    return new CacheEntryUpdateEvent<object, object>(e.Key, e.OldValue, e.Value);
                default:
                    return new CacheEntryRemoveEvent<object, object>(e.Key, e.OldValue);
            }
        }

        /// <summary>
        /// Binarizable entry.
        /// </summary>
        public class BinarizableEntry
        {
            /** Value. */
            public readonly int val;

            /** <inheritDot /> */
            public override int GetHashCode()
            {
                return val;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="val">Value.</param>
            public BinarizableEntry(int val)
            {
                this.val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj != null && obj is BinarizableEntry && ((BinarizableEntry)obj).val == val;
            }

            /** <inheritDoc /> */
            public override string ToString()
            {
                return string.Format("BinarizableEntry [Val: {0}]", val);
            }
        }

        /// <summary>
        /// Abstract filter.
        /// </summary>
        [Serializable]
        public abstract class AbstractFilter<V> : ICacheEntryEventFilter<int, V>
        {
            /** Result. */
            public static volatile bool res = true;

            /** Throw error on invocation. */
            public static volatile bool err;

            /** Throw error during marshalling. */
            public static volatile bool marshErr;

            /** Throw error during unmarshalling. */
            public static volatile bool unmarshErr;

            /** Grid. */
            [InstanceResource]
            public IIgnite ignite;

            /** <inheritDoc /> */
            public bool Evaluate(ICacheEntryEvent<int, V> evt)
            {
                if (err)
                    throw new Exception("Filter error.");

                FILTER_EVTS.Add(new FilterEvent(ignite, CreateEvent(evt)));

                return res;
            }
        }

        /// <summary>
        /// Filter which cannot be serialized.
        /// </summary>
        public class LocalFilter : AbstractFilter<BinarizableEntry>, IBinarizable
        {
            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                throw new BinaryObjectException("Expected");
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                throw new BinaryObjectException("Expected");
            }
        }

        /// <summary>
        /// Binarizable filter.
        /// </summary>
        public class BinarizableFilter : AbstractFilter<BinarizableEntry>, IBinarizable
        {
            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                if (marshErr)
                    throw new Exception("Filter marshalling error.");
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                if (unmarshErr)
                    throw new Exception("Filter unmarshalling error.");
            }
        }

        /// <summary>
        /// Serializable filter.
        /// </summary>
        [Serializable]
        public class SerializableFilter : AbstractFilter<BinarizableEntry>, ISerializable
        {
            /// <summary>
            /// Constructor.
            /// </summary>
            public SerializableFilter()
            {
                // No-op.
            }

            /// <summary>
            /// Serialization constructor.
            /// </summary>
            /// <param name="info">Info.</param>
            /// <param name="context">Context.</param>
            protected SerializableFilter(SerializationInfo info, StreamingContext context)
            {
                if (unmarshErr)
                    throw new Exception("Filter unmarshalling error.");
            }

            /** <inheritDoc /> */
            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                if (marshErr)
                    throw new Exception("Filter marshalling error.");
            }
        }

        /// <summary>
        /// Filter for "keep-binary" scenario.
        /// </summary>
        public class KeepBinaryFilter : AbstractFilter<IBinaryObject>
        {
            // No-op.
        }

        /// <summary>
        /// Listener.
        /// </summary>
        public class Listener<V> : ICacheEntryEventListener<int, V>
        {
            [InstanceResource]
            public IIgnite ignite;

            /** <inheritDoc /> */
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, V>> evts)
            {
                CB_EVTS.Add(new CallbackEvent(evts.Select(CreateEvent).ToList()));
            }
        }

        /// <summary>
        /// Listener with nested Ignite API call.
        /// </summary>
        public class NestedCallListener : ICacheEntryEventListener<int, IBinaryObject>
        {
            /** Event. */
            public readonly CountdownEvent countDown = new CountdownEvent(1);

            public void OnEvent(IEnumerable<ICacheEntryEvent<int, IBinaryObject>> evts)
            {
                foreach (ICacheEntryEvent<int, IBinaryObject> evt in evts)
                {
                    IBinaryObject val = evt.Value;

                    IBinaryType meta = val.GetBinaryType();

                    Assert.AreEqual(typeof(BinarizableEntry).FullName, meta.TypeName);
                }

                countDown.Signal();
            }
        }

        /// <summary>
        /// Filter event.
        /// </summary>
        public class FilterEvent
        {
            /** Grid. */
            public IIgnite ignite;

            /** Entry. */
            public ICacheEntryEvent<object, object> entry;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="ignite">Grid.</param>
            /// <param name="entry">Entry.</param>
            public FilterEvent(IIgnite ignite, ICacheEntryEvent<object, object> entry)
            {
                this.ignite = ignite;
                this.entry = entry;
            }
        }

        /// <summary>
        /// Callbakc event.
        /// </summary>
        public class CallbackEvent
        {
            /** Entries. */
            public ICollection<ICacheEntryEvent<object, object>> entries;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="entries">Entries.</param>
            public CallbackEvent(ICollection<ICacheEntryEvent<object, object>> entries)
            {
                this.entries = entries;
            }
        }

        /// <summary>
        /// ScanQuery filter for InitialQuery test.
        /// </summary>
        [Serializable]
        private class InitialQueryScanFilter : ICacheEntryFilter<int, BinarizableEntry>
        {
            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, BinarizableEntry> entry)
            {
                return entry.Key < 33;
            }
        }
    }
}

