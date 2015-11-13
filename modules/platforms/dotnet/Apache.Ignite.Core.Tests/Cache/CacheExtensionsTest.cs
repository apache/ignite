/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

// ReSharper disable AccessToModifiedClosure
namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Tests;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="CacheExtensions"/>.
    /// </summary>
    [Serializable]
    public class CacheExtensionsTest : IgniteTestBase
    {
        /** */
        private int _testField = 15;

        /** Initial cache entries. */
        private static readonly Dictionary<int, int> Entries = Enumerable.Range(1, 100).ToDictionary(x => x, x => x);

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheExtensionsTest"/> class.
        /// </summary>
        public CacheExtensionsTest()
            : base(3, "config\\compute\\compute-grid1.xml", "config\\native-client-test-cache-store.xml")
        {
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            Cache.Clear();
            Cache.PutAll(Entries);
        }

        /// <summary>
        /// Tests Invoke methods.
        /// </summary>
        [Test]
        public void TestInvoke()
        {
            // static
            Assert.AreEqual(Cache.Get(1) + 2, Cache.Invoke(1, (e, a) => e.Value + a, 2));

            // field capture
            Assert.AreEqual(Cache.Get(1) + _testField, Cache.Invoke(1, (e, a) => e.Value + _testField, 2));

            // variable capture
            var testVal = 7;

            Assert.AreEqual(Cache.Get(1) + testVal, Cache.Invoke(1, (e, a) => e.Value + testVal, 2));

            // unsupported
            var invalid = new NonSerializable { Prop = 10 };

            Assert.Throws<SerializationException>(() => Cache.Invoke(1, (e, a) => e.Value + invalid.Prop, 0));
        }

        /// <summary>
        /// Tests InvokeAll methods.
        /// </summary>
        [Test]
        public void TestInvokeAll()
        {
            var keys = Enumerable.Range(1, 10).ToArray();
            var vals = Cache.GetAll(keys).Values;

            // static
            CollectionAssert.AreEquivalent(vals.Select(v => v + 2),
                Cache.InvokeAll(keys, (e, a) => e.Value + a, 2).Select(e => e.Value.Result));

            // field capture
            CollectionAssert.AreEquivalent(vals.Select(v => v + _testField),
                Cache.InvokeAll(keys, (e, a) => e.Value + _testField, 0).Select(e => e.Value.Result));

            // variable capture
            var testVal = 7;

            CollectionAssert.AreEquivalent(vals.Select(v => v + testVal),
                Cache.InvokeAll(keys, (e, a) => e.Value + testVal, 0).Select(e => e.Value.Result));

            // unsupported
            var invalid = new NonSerializable();

            Assert.Throws<SerializationException>(() => Cache.InvokeAll(keys, (e, a) => e.Value + invalid.Prop, 0));
        }

        /// <summary>
        /// Tests continuous query methods.
        /// </summary>
        [Test]
        public void TestQueryContinuous()
        {
            int entriesArrived = 0;
            int key = -1;

            // No filter
            using (Cache.QueryContinuous(e => entriesArrived += e.Count(), null, false))
            {
                Cache.Put(-1, -1);
                Cache.Put(-2, -2);

                Assert.IsTrue(TestUtils.WaitForCondition(() => entriesArrived == 2, 500));
            }

            entriesArrived = 0;

            // With filter
            using (Cache.QueryContinuous(e => entriesArrived += e.Count(), e => e.Key == key, false))
            {
                Cache.Put(-1, -1);
                Cache.Put(-2, -2);

                Assert.IsTrue(TestUtils.WaitForCondition(() => entriesArrived == 1, 500));
            }

            entriesArrived = 0;

            // With initial query
            var initialQry = new ScanQuery<int, int>();

            using (var cur = Cache.QueryContinuous(e => entriesArrived += e.Count(), e => e.Key == key, false, initialQry))
            {
                Assert.AreEqual(Entries.Count + 2, cur.GetInitialQueryCursor().GetAll().Count);

                Cache.Put(-1, -1);
                Cache.Put(-2, -2);

                Assert.IsTrue(TestUtils.WaitForCondition(() => entriesArrived == 1, 500));
            }
        }

        /// <summary>
        /// Tests the load cache.
        /// </summary>
        [Test]
        public void TestLoadCache()
        {
            var cache = Grid2.GetCache<int, string>("portable_store");

            // Static filter
            cache.LoadCache(e => e.Key % 2 == 0, 300, 10);

            var entries = cache.GetAll(Enumerable.Range(300, 10).ToArray());

            Assert.AreEqual(5, entries.Count);
            Assert.IsTrue(entries.All(e => e.Value == "val_" + e.Key));

            // Filter with variable and field capture
            int maxKey = 405;

            cache.LoadCache(e => e.Key < maxKey && e.Value != _testField.ToString(), 400, 10);

            entries = cache.GetAll(Enumerable.Range(400, 10).ToArray());

            Assert.AreEqual(5, entries.Count);
            Assert.IsTrue(entries.All(e => e.Value == "val_" + e.Key));

            // Invalid filter
            var invalid = new NonSerializable();

            try
            {
                // Can't use Assert.Throws because it adds cache to the captured variables.
                cache.LoadCache(e => e.Key > invalid.Prop);

                Assert.Fail();
            }
            catch (SerializationException)
            {
                // Expected
            }
        }

        /// <summary>
        /// Tests the local load cache.
        /// </summary>
        [Test]
        public void TestLocalLoadCache()
        {
            var cache = Grid2.GetCache<int, string>("portable_store");

            // Static filter
            cache.LocalLoadCache(e => e.Key % 2 == 0, 700, 10);

            var entries = cache.GetAll(Enumerable.Range(700, 10).ToArray());

            Assert.AreEqual(5, entries.Count);
            Assert.IsTrue(entries.All(e => e.Value == "val_" + e.Key));

            // Filter with variable and field capture
            int maxKey = 805;

            cache.LocalLoadCache(e => e.Key < maxKey && e.Value != _testField.ToString(), 800, 10);

            entries = cache.GetAll(Enumerable.Range(800, 10).ToArray());

            Assert.AreEqual(5, entries.Count);
            Assert.IsTrue(entries.All(e => e.Value == "val_" + e.Key));

            // Invalid filter
            var invalid = new NonSerializable();

            try
            {
                // Can't use Assert.Throws because it adds cache to the captured variables.
                cache.LoadCache(e => e.Key > invalid.Prop);

                Assert.Fail();
            }
            catch (SerializationException)
            {
                // Expected
            }
        }

        /// <summary>
        /// Tests the scan query.
        /// </summary>
        [Test]
        public void TestScanQuery()
        {
            // No filter
            using (var cursor = Cache.ScanQuery())
            {
                CollectionAssert.AreEquivalent(Entries, cursor.GetAll().ToDictionary(x => x.Key, x => x.Value));
            }

            // Static filter
            using (var cursor = Cache.ScanQuery(e => e.Key < 50))
            {
                CollectionAssert.AreEquivalent(Entries.Where(e => e.Key < 50),
                    cursor.GetAll().ToDictionary(x => x.Key, x => x.Value));
            }

            // Filter with variable and field capture
            int minKey = 20;
            _testField = 70;

            using (var cursor = Cache.ScanQuery(e => e.Key > minKey && e.Key < _testField))
            {
                CollectionAssert.AreEquivalent(Entries.Where(e => e.Key > minKey && e.Key < _testField),
                    cursor.GetAll().ToDictionary(x => x.Key, x => x.Value));
            }

            // Invalid filter
            var invalid = new NonSerializable();

            Assert.Throws<SerializationException>(() => Cache.ScanQuery(e => e.Key > invalid.Prop));
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<int, int> Cache
        {
            get { return Grid.GetCache<int, int>(null); }
        }

        /// <summary>
        /// Non-serializable func.
        /// </summary>
        private class NonSerializable
        {
            public int Prop { get; set; }
        }
    }
}
