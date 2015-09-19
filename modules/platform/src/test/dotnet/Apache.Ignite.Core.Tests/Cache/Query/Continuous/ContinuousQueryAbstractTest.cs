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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;
    using CQU = Apache.Ignite.Core.Impl.Cache.Query.Continuous.ContinuousQueryUtils;

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
        private ICache<int, PortableEntry> cache1;

        /** Cache on the second node. */
        private ICache<int, PortableEntry> cache2;

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
            GC.Collect();
            TestUtils.JvmDebug = true;

            IgniteConfigurationEx cfg = new IgniteConfigurationEx();

            PortableConfiguration portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableEntry)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableFilter)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(KeepPortableFilter)));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;
            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = "config\\cache-query-continuous.xml";

            cfg.GridName = "grid-1";
            grid1 = Ignition.Start(cfg);
            cache1 = grid1.Cache<int, PortableEntry>(cacheName);

            cfg.GridName = "grid-2";
            grid2 = Ignition.Start(cfg);
            cache2 = grid2.Cache<int, PortableEntry>(cacheName);
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

            AbstractFilter<PortableEntry>.res = true;
            AbstractFilter<PortableEntry>.err = false;
            AbstractFilter<PortableEntry>.marshErr = false;
            AbstractFilter<PortableEntry>.unmarshErr = false;

            cache1.Remove(PrimaryKey(cache1));
            cache1.Remove(PrimaryKey(cache2));

            Assert.AreEqual(0, cache1.Size());
            Assert.AreEqual(0, cache2.Size());

            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }
        
        /// <summary>
        /// Test arguments validation.
        /// </summary>
        [Test]
        public void TestValidation()
        {
            Assert.Throws<ArgumentException>(() => { cache1.QueryContinuous(new ContinuousQuery<int, PortableEntry>(null)); });
        }

        /// <summary>
        /// Test multiple closes.
        /// </summary>
        [Test]
        public void TestMultipleClose()
        {
            int key1 = PrimaryKey(cache1);
            int key2 = PrimaryKey(cache2);

            ContinuousQuery<int, PortableEntry> qry =
                new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>());

            IDisposable qryHnd;

            using (qryHnd = cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1));

                // Put from remote node.
                cache2.GetAndPut(key2, Entry(key2));
                CheckCallbackSingle(key2, null, Entry(key2));
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
            
            ContinuousQuery<int, PortableEntry> qry = loc ?
                new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>(), true) :
                new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>());

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1));

                cache1.GetAndPut(key1, Entry(key1 + 1));
                CheckCallbackSingle(key1, Entry(key1), Entry(key1 + 1));

                cache1.Remove(key1);
                CheckCallbackSingle(key1, Entry(key1 + 1), null);

                // Put from remote node.
                cache2.GetAndPut(key2, Entry(key2));

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, null, Entry(key2));

                cache1.GetAndPut(key2, Entry(key2 + 1));

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, Entry(key2), Entry(key2 + 1));

                cache1.Remove(key2);

                if (loc)
                    CheckNoCallback(100);
                else
                    CheckCallbackSingle(key2, Entry(key2 + 1), null);
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
            Listener<PortableEntry> cb = new Listener<PortableEntry>();

            Assert.IsNull(cb.ignite);

            using (cache1.QueryContinuous(new ContinuousQuery<int, PortableEntry>(cb)))
            {
                Assert.IsNotNull(cb.ignite);
            }
        }
        
        /// <summary>
        /// Test portable filter logic.
        /// </summary>
        [Test]
        public void TestFilterPortable()
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
        /// Check filter.
        /// </summary>
        /// <param name="portable">Portable.</param>
        /// <param name="loc">Local cache flag.</param>
        protected void CheckFilter(bool portable, bool loc)
        {
            ICacheEntryEventListener<int, PortableEntry> lsnr = new Listener<PortableEntry>();
            ICacheEntryEventFilter<int, PortableEntry> filter = 
                portable ? (AbstractFilter<PortableEntry>)new PortableFilter() : new SerializableFilter();

            ContinuousQuery<int, PortableEntry> qry = loc ? 
                new ContinuousQuery<int, PortableEntry>(lsnr, filter, true) : 
                new ContinuousQuery<int, PortableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                int key1 = PrimaryKey(cache1);
                cache1.GetAndPut(key1, Entry(key1));
                CheckFilterSingle(key1, null, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1));

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
                    CheckCallbackSingle(key2, null, Entry(key2));
                }

                AbstractFilter<PortableEntry>.res = false;

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
        /// Test portable filter error during invoke.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterInvokeErrorPortable()
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
        private void CheckFilterInvokeError(bool portable)
        {
            AbstractFilter<PortableEntry>.err = true;

            ICacheEntryEventListener<int, PortableEntry> lsnr = new Listener<PortableEntry>();
            ICacheEntryEventFilter<int, PortableEntry> filter =
                portable ? (AbstractFilter<PortableEntry>) new PortableFilter() : new SerializableFilter();

            ContinuousQuery<int, PortableEntry> qry = new ContinuousQuery<int, PortableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                try
                {
                    cache1.GetAndPut(PrimaryKey(cache1), Entry(1));

                    Assert.Fail("Should not reach this place.");
                }
                catch (IgniteException)
                {
                    // No-op.
                }
                catch (Exception)
                {
                    Assert.Fail("Unexpected error.");
                }

                // Put from remote node.
                try
                {
                    cache1.GetAndPut(PrimaryKey(cache2), Entry(1));

                    Assert.Fail("Should not reach this place.");
                }
                catch (IgniteException)
                {
                    // No-op.
                }
                catch (Exception)
                {
                    Assert.Fail("Unexpected error.");
                }
            }
        }

        /// <summary>
        /// Test portable filter marshalling error.
        /// </summary>
        [Test]
        public void TestFilterMarshalErrorPortable()
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
        /// <param name="portable">Portable flag.</param>
        private void CheckFilterMarshalError(bool portable)
        {
            AbstractFilter<PortableEntry>.marshErr = true;

            ICacheEntryEventListener<int, PortableEntry> lsnr = new Listener<PortableEntry>();
            ICacheEntryEventFilter<int, PortableEntry> filter =
                portable ? (AbstractFilter<PortableEntry>)new PortableFilter() : new SerializableFilter();

            ContinuousQuery<int, PortableEntry> qry = new ContinuousQuery<int, PortableEntry>(lsnr, filter);

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
            AbstractFilter<PortableEntry>.unmarshErr = true;

            ICacheEntryEventListener<int, PortableEntry> lsnr = new Listener<PortableEntry>();
            ICacheEntryEventFilter<int, PortableEntry> filter = new LocalFilter();

            ContinuousQuery<int, PortableEntry> qry = loc
                ? new ContinuousQuery<int, PortableEntry>(lsnr, filter, true)
                : new ContinuousQuery<int, PortableEntry>(lsnr, filter);

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
                Assert.Throws<SerializationException>(() =>
                {
                    using (cache1.QueryContinuous(qry))
                    {
                        // No-op.
                    }
                });
            }
        }

        /// <summary>
        /// Test portable filter unmarshalling error.
        /// </summary>
        [Ignore("IGNITE-521")]
        [Test]
        public void TestFilterUnmarshalErrorPortable()
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
        /// <param name="portable">Portable flag.</param>
        private void CheckFilterUnmarshalError(bool portable)
        {
            AbstractFilter<PortableEntry>.unmarshErr = true;

            ICacheEntryEventListener<int, PortableEntry> lsnr = new Listener<PortableEntry>();
            ICacheEntryEventFilter<int, PortableEntry> filter =
                portable ? (AbstractFilter<PortableEntry>)new PortableFilter() : new SerializableFilter();

            ContinuousQuery<int, PortableEntry> qry = new ContinuousQuery<int, PortableEntry>(lsnr, filter);

            using (cache1.QueryContinuous(qry))
            {
                // Local put must be fine.
                int key1 = PrimaryKey(cache1);
                cache1.GetAndPut(key1, Entry(key1));
                CheckFilterSingle(key1, null, Entry(key1));
                
                // Remote put must fail.
                try
                {
                    cache1.GetAndPut(PrimaryKey(cache2), Entry(1));

                    Assert.Fail("Should not reach this place.");
                }
                catch (IgniteException)
                {
                    // No-op.
                }
                catch (Exception)
                {
                    Assert.Fail("Unexpected error.");
                }
            }
        }

        /// <summary>
        /// Test Ignite injection into filters.
        /// </summary>
        [Test]
        public void TestFilterInjection()
        {
            Listener<PortableEntry> cb = new Listener<PortableEntry>();
            PortableFilter filter = new PortableFilter();

            Assert.IsNull(filter.ignite);

            using (cache1.QueryContinuous(new ContinuousQuery<int, PortableEntry>(cb, filter)))
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
        /// Test "keep-portable" scenario.
        /// </summary>
        [Test]
        public void TestKeepPortable()
        {
            var cache = cache1.WithKeepPortable<int, IPortableObject>();

            ContinuousQuery<int, IPortableObject> qry = new ContinuousQuery<int, IPortableObject>(
                    new Listener<IPortableObject>(), new KeepPortableFilter());

            using (cache.QueryContinuous(qry))
            {
                // 1. Local put.
                cache1.GetAndPut(PrimaryKey(cache1), Entry(1));

                CallbackEvent cbEvt;
                FilterEvent filterEvt;

                Assert.IsTrue(FILTER_EVTS.TryTake(out filterEvt, 500));
                Assert.AreEqual(PrimaryKey(cache1), filterEvt.entry.Key);
                Assert.AreEqual(null, filterEvt.entry.OldValue);
                Assert.AreEqual(Entry(1), (filterEvt.entry.Value as IPortableObject)
                    .Deserialize<PortableEntry>());

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                Assert.AreEqual(1, cbEvt.entries.Count);
                Assert.AreEqual(PrimaryKey(cache1), cbEvt.entries.First().Key);
                Assert.AreEqual(null, cbEvt.entries.First().OldValue);
                Assert.AreEqual(Entry(1), (cbEvt.entries.First().Value as IPortableObject)
                    .Deserialize<PortableEntry>());

                // 2. Remote put.
                cache1.GetAndPut(PrimaryKey(cache2), Entry(2));

                Assert.IsTrue(FILTER_EVTS.TryTake(out filterEvt, 500));
                Assert.AreEqual(PrimaryKey(cache2), filterEvt.entry.Key);
                Assert.AreEqual(null, filterEvt.entry.OldValue);
                Assert.AreEqual(Entry(2), (filterEvt.entry.Value as IPortableObject)
                    .Deserialize<PortableEntry>());

                Assert.IsTrue(CB_EVTS.TryTake(out cbEvt, 500));
                Assert.AreEqual(1, cbEvt.entries.Count);
                Assert.AreEqual(PrimaryKey(cache2), cbEvt.entries.First().Key);
                Assert.AreEqual(null, cbEvt.entries.First().OldValue);
                Assert.AreEqual(Entry(2),
                    (cbEvt.entries.First().Value as IPortableObject).Deserialize<PortableEntry>());
            }
        }

        /// <summary>
        /// Test whether buffer size works fine.
        /// </summary>
        [Test]
        public void TestBufferSize()
        {
            // Put two remote keys in advance.
            List<int> rmtKeys = PrimaryKeys(cache2, 2);

            ContinuousQuery<int, PortableEntry> qry = new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>());

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

            ContinuousQuery<int, PortableEntry> qry =
                new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>());

            qry.BufferSize = 2;
            qry.TimeInterval = TimeSpan.FromMilliseconds(500);

            using (cache1.QueryContinuous(qry))
            {
                // Put from local node.
                cache1.GetAndPut(key1, Entry(key1));
                CheckCallbackSingle(key1, null, Entry(key1));

                // Put from remote node.
                cache1.GetAndPut(key2, Entry(key2));
                CheckNoCallback(100);
                CheckCallbackSingle(key2, null, Entry(key2), 1000);
            }
        }

        /// <summary>
        /// Test whether nested Ignite API call from callback works fine.
        /// </summary>
        [Test]
        public void TestNestedCallFromCallback()
        {
            var cache = cache1.WithKeepPortable<int, IPortableObject>();

            int key = PrimaryKey(cache1);

            NestedCallListener cb = new NestedCallListener();

            using (cache.QueryContinuous(new ContinuousQuery<int, IPortableObject>(cb)))
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
            TestInitialQuery(new ScanQuery<int, PortableEntry>(new InitialQueryScanFilter()), cur => cur.GetAll());

            // Scan query, iterator
            TestInitialQuery(new ScanQuery<int, PortableEntry>(new InitialQueryScanFilter()), cur => cur.ToList());

            // Sql query, GetAll
            TestInitialQuery(new SqlQuery(typeof(PortableEntry), "val < 33"), cur => cur.GetAll());
            
            // Sql query, iterator
            TestInitialQuery(new SqlQuery(typeof(PortableEntry), "val < 33"), cur => cur.ToList());

            // Text query, GetAll
            TestInitialQuery(new TextQuery(typeof(PortableEntry), "1*"), cur => cur.GetAll());
            
            // Text query, iterator
            TestInitialQuery(new TextQuery(typeof(PortableEntry), "1*"), cur => cur.ToList());

            // Test exception: invalid initial query
            var ex = Assert.Throws<IgniteException>(
                () => TestInitialQuery(new TextQuery(typeof (PortableEntry), "*"), cur => cur.GetAll()));

            Assert.AreEqual("Cannot parse '*': '*' or '?' not allowed as first character in WildcardQuery", ex.Message);
        }

        /// <summary>
        /// Tests the initial query.
        /// </summary>
        private void TestInitialQuery(QueryBase initialQry, Func<IQueryCursor<ICacheEntry<int, PortableEntry>>, 
            IEnumerable<ICacheEntry<int, PortableEntry>>> getAllFunc)
        {
            var qry = new ContinuousQuery<int, PortableEntry>(new Listener<PortableEntry>());

            cache1.Put(11, Entry(11));
            cache1.Put(12, Entry(12));
            cache1.Put(33, Entry(33));

            try
            {
                IContinuousQueryHandle<ICacheEntry<int, PortableEntry>> contQry;
                
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
                    CheckCallbackSingle(44, null, Entry(44));
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
        private void CheckFilterSingle(int expKey, PortableEntry expOldVal, PortableEntry expVal)
        {
            CheckFilterSingle(expKey, expOldVal, expVal, 1000);
        }

        /// <summary>
        /// Check single filter event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected value.</param>
        /// <param name="timeout">Timeout.</param>
        private void CheckFilterSingle(int expKey, PortableEntry expOldVal, PortableEntry expVal, int timeout)
        {
            FilterEvent evt;

            Assert.IsTrue(FILTER_EVTS.TryTake(out evt, timeout));

            Assert.AreEqual(expKey, evt.entry.Key);
            Assert.AreEqual(expOldVal, evt.entry.OldValue);
            Assert.AreEqual(expVal, evt.entry.Value);
        }

        /// <summary>
        /// Ensure that no filter events are logged.
        /// </summary>
        /// <param name="timeout">Timeout.</param>
        private void CheckNoFilter(int timeout)
        {
            FilterEvent evt;

            Assert.IsFalse(FILTER_EVTS.TryTake(out evt, timeout));
        }

        /// <summary>
        /// Check single callback event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected new value.</param>
        private void CheckCallbackSingle(int expKey, PortableEntry expOldVal, PortableEntry expVal)
        {
            CheckCallbackSingle(expKey, expOldVal, expVal, 1000);
        }

        /// <summary>
        /// Check single callback event.
        /// </summary>
        /// <param name="expKey">Expected key.</param>
        /// <param name="expOldVal">Expected old value.</param>
        /// <param name="expVal">Expected new value.</param>
        /// <param name="timeout">Timeout.</param>
        private void CheckCallbackSingle(int expKey, PortableEntry expOldVal, PortableEntry expVal, int timeout)
        {
            CallbackEvent evt;

            Assert.IsTrue(CB_EVTS.TryTake(out evt, timeout));

            Assert.AreEqual(1, evt.entries.Count);

            Assert.AreEqual(expKey, evt.entries.First().Key);
            Assert.AreEqual(expOldVal, evt.entries.First().OldValue);
            Assert.AreEqual(expVal, evt.entries.First().Value);
        }

        /// <summary>
        /// Ensure that no callback events are logged.
        /// </summary>
        /// <param name="timeout">Timeout.</param>
        private void CheckNoCallback(int timeout)
        {
            CallbackEvent evt;

            Assert.IsFalse(CB_EVTS.TryTake(out evt, timeout));
        }

        /// <summary>
        /// Craate entry.
        /// </summary>
        /// <param name="val">Value.</param>
        /// <returns>Entry.</returns>
        private static PortableEntry Entry(int val)
        {
            return new PortableEntry(val);
        }

        /// <summary>
        /// Get primary key for cache.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <returns>Primary key.</returns>
        private static int PrimaryKey<T>(ICache<int, T> cache)
        {
            return PrimaryKeys(cache, 1)[0];
        }

        /// <summary>
        /// Get primary keys for cache.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="cnt">Amount of keys.</param>
        /// <param name="startFrom">Value to start from.</param>
        /// <returns></returns>
        private static List<int> PrimaryKeys<T>(ICache<int, T> cache, int cnt, int startFrom = 0)
        {
            IClusterNode node = cache.Ignite.Cluster.LocalNode;

            ICacheAffinity aff = cache.Ignite.Affinity(cache.Name);

            List<int> keys = new List<int>(cnt);

            for (int i = startFrom; i < startFrom + 100000; i++)
            {
                if (aff.IsPrimary(node, i))
                {
                    keys.Add(i);

                    if (keys.Count == cnt)
                        return keys;
                }
            }

            Assert.Fail("Failed to find " + cnt + " primary keys.");

            return null;
        }

        /// <summary>
        /// Portable entry.
        /// </summary>
        public class PortableEntry
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
            public PortableEntry(int val)
            {
                this.val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj != null && obj is PortableEntry && ((PortableEntry)obj).val == val;
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

                FILTER_EVTS.Add(new FilterEvent(ignite,
                    CQU.CreateEvent<object, object>(evt.Key, evt.OldValue, evt.Value)));

                return res;
            }
        }

        /// <summary>
        /// Filter which cannot be serialized.
        /// </summary>
        public class LocalFilter : AbstractFilter<PortableEntry>
        {
            // No-op.
        }

        /// <summary>
        /// Portable filter.
        /// </summary>
        public class PortableFilter : AbstractFilter<PortableEntry>, IPortableMarshalAware
        {
            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                if (marshErr)
                    throw new Exception("Filter marshalling error.");
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                if (unmarshErr)
                    throw new Exception("Filter unmarshalling error.");
            }
        }

        /// <summary>
        /// Serializable filter.
        /// </summary>
        [Serializable]
        public class SerializableFilter : AbstractFilter<PortableEntry>, ISerializable
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
        /// Filter for "keep-portable" scenario.
        /// </summary>
        public class KeepPortableFilter : AbstractFilter<IPortableObject>
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
                ICollection<ICacheEntryEvent<object, object>> entries0 =
                    new List<ICacheEntryEvent<object, object>>();

                foreach (ICacheEntryEvent<int, V> evt in evts)
                    entries0.Add(CQU.CreateEvent<object, object>(evt.Key, evt.OldValue, evt.Value));

                CB_EVTS.Add(new CallbackEvent(entries0));
            }
        }

        /// <summary>
        /// Listener with nested Ignite API call.
        /// </summary>
        public class NestedCallListener : ICacheEntryEventListener<int, IPortableObject>
        {
            /** Event. */
            public readonly CountdownEvent countDown = new CountdownEvent(1);

            public void OnEvent(IEnumerable<ICacheEntryEvent<int, IPortableObject>> evts)
            {
                foreach (ICacheEntryEvent<int, IPortableObject> evt in evts)
                {
                    IPortableObject val = evt.Value;

                    IPortableMetadata meta = val.Metadata();

                    Assert.AreEqual(typeof(PortableEntry).Name, meta.TypeName);
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
        private class InitialQueryScanFilter : ICacheEntryFilter<int, PortableEntry>
        {
            /** <inheritdoc /> */
            public bool Invoke(ICacheEntry<int, PortableEntry> entry)
            {
                return entry.Key < 33;
            }
        }
    }
}

