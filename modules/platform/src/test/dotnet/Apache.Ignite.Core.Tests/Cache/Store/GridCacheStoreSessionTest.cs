/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Store
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;

    using GridGain.Client;
    using GridGain.Impl;
    using GridGain.Resource;

    using NUnit.Framework;

    /// <summary>
    /// Tests for store session.
    /// </summary>
    public class GridCacheStoreSessionTest
    {
        /** Grid name. */
        private const string GRID = "grid";

        /** Cache 1 name. */
        private const string CACHE_1 = "cache1";

        /** Cache 2 name. */
        private const string CACHE_2 = "cache2";

        /** Operations. */
        private static ConcurrentBag<ICollection<Operation>> dumps;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void BeforeTests()
        {
            //GridTestUtils.JVM_DEBUG = true;

            GridTestUtils.KillProcesses();

            GridTestUtils.JVM_DEBUG = true;

            GridConfigurationEx cfg = new GridConfigurationEx
            {
                GridName = GRID,
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                SpringConfigUrl = @"config\cache\store\cache-store-session.xml"
            };


            GridFactory.Start(cfg);
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void AfterTests()
        {
            GridFactory.StopAll(true);
        }
        
        /// <summary>
        /// Test basic session API.
        /// </summary>
        [Test]
        public void TestSession()
        {
            dumps = new ConcurrentBag<ICollection<Operation>>();

            var grid = GridFactory.Grid(GRID);

            var cache1 = GridFactory.Grid(GRID).Cache<int, int>(CACHE_1);
            var cache2 = GridFactory.Grid(GRID).Cache<int, int>(CACHE_2);

            // 1. Test rollback.
            using (var tx = grid.Transactions.TxStart())
            {
                cache1.Put(1, 1);
                cache2.Put(2, 2);

                tx.Rollback();
            }

            Assert.AreEqual(1, dumps.Count);
            var ops = dumps.First();
            Assert.AreEqual(1, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SES_END && !op.Commit));

            dumps = new ConcurrentBag<ICollection<Operation>>();

            // 2. Test puts.
            using (var tx = grid.Transactions.TxStart())
            {
                cache1.Put(1, 1);
                cache2.Put(2, 2);

                tx.Commit();
            }

            Assert.AreEqual(1, dumps.Count);
            ops = dumps.First();
            Assert.AreEqual(3, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.WRITE && CACHE_1.Equals(op.CacheName) && 1.Equals(op.Key) && 1.Equals(op.Value)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.WRITE && CACHE_2.Equals(op.CacheName) && 2.Equals(op.Key) && 2.Equals(op.Value)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SES_END && op.Commit));

            dumps = new ConcurrentBag<ICollection<Operation>>();

            // 3. Test removes.
            using (var tx = grid.Transactions.TxStart())
            {
                cache1.Remove(1);
                cache2.Remove(2);

                tx.Commit();
            }

            Assert.AreEqual(1, dumps.Count);
            ops = dumps.First();
            Assert.AreEqual(3, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.DELETE && CACHE_1.Equals(op.CacheName) && 1.Equals(op.Key)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.DELETE && CACHE_2.Equals(op.CacheName) && 2.Equals(op.Key)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SES_END && op.Commit));
        }

        /// <summary>
        /// Dump operations.
        /// </summary>
        /// <param name="dump">Dump.</param>
        internal static void DumpOperations(ICollection<Operation> dump)
        {
            dumps.Add(dump);
        }

        /// <summary>
        /// Test store implementation.
        /// </summary>
        public class Store : CacheStoreAdapter
        {
            /** Store session. */
            [StoreSessionResource]
#pragma warning disable 649
            private ICacheStoreSession ses;
#pragma warning restore 649

            /** <inheritdoc /> */
            public override object Load(object key)
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public override void Write(object key, object val)
            {
                GetOperations().Add(new Operation(ses.CacheName, OperationType.WRITE, (int)key, (int)val));
            }

            /** <inheritdoc /> */
            public override void Delete(object key)
            {
                GetOperations().Add(new Operation(ses.CacheName, OperationType.DELETE, (int)key, 0));
            }

            /** <inheritdoc /> */
            public override void SessionEnd(bool commit)
            {
                Operation op = new Operation(ses.CacheName, OperationType.SES_END) { Commit = commit };

                ICollection<Operation> ops = GetOperations();

                ops.Add(op);

                DumpOperations(ops);
            }

            /// <summary>
            /// Get collection with operations.
            /// </summary>
            /// <returns>Operations.</returns>
            private ICollection<Operation> GetOperations()
            {
                object ops;

                if (!ses.Properties.TryGetValue("ops", out ops))
                {
                    ops = new List<Operation>();

                    ses.Properties["ops"] = ops;
                }

                return (ICollection<Operation>) ops;
            } 
        }

        /// <summary>
        /// Logged operation.
        /// </summary>
        internal class Operation
        {
            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="cacheName">Cache name.</param>
            /// <param name="type">Operation type.</param>
            public Operation(string cacheName, OperationType type)
            {
                CacheName = cacheName;
                Type = type;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="cacheName">Cache name.</param>
            /// <param name="type">Operation type.</param>
            /// <param name="key">Key.</param>
            /// <param name="val">Value.</param>
            public Operation(string cacheName, OperationType type, int key, int val) : this(cacheName, type)
            {
                Key = key;
                Value = val;
            }

            /// <summary>
            /// Cache name.
            /// </summary>
            public string CacheName { get; set; }
            
            /// <summary>
            /// Operation type.
            /// </summary>
            public OperationType Type { get; set; }

            /// <summary>
            /// Key.
            /// </summary>
            public int Key { get; set; }

            /// <summary>
            /// Value.
            /// </summary>
            public int Value { get; set; }

            /// <summary>
            /// Commit flag.
            /// </summary>
            public bool Commit { get; set; }
        }

        /// <summary>
        /// Operation types.
        /// </summary>
        internal enum OperationType
        {
            /** Write. */
            WRITE,

            /** Delete. */
            DELETE,

            /** Session end. */
            SES_END
        }
    }
}
