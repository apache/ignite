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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests for store session.
    /// </summary>
    public sealed class CacheStoreSessionTest
    {
        /** Grid name. */
        private const string IgniteName = "grid";

        /** Cache 1 name. */
        private const string Cache1 = "cache1";

        /** Cache 2 name. */
        private const string Cache2 = "cache2";

        /** Operations. */
        private static ConcurrentBag<ICollection<Operation>> _dumps;

        /// <summary>
        /// Set up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTests()
        {
            //TestUtils.JVM_DEBUG = true;

            TestUtils.KillProcesses();

            TestUtils.JvmDebug = true;

            IgniteConfiguration cfg = new IgniteConfiguration
            {
                GridName = IgniteName,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                SpringConfigUrl = @"config\cache\store\cache-store-session.xml"
            };


            Ignition.Start(cfg);
        }

        /// <summary>
        /// Tear down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void AfterTests()
        {
            Ignition.StopAll(true);
        }
        
        /// <summary>
        /// Test basic session API.
        /// </summary>
        [Test]
        public void TestSession()
        {
            _dumps = new ConcurrentBag<ICollection<Operation>>();

            var ignite = Ignition.GetIgnite(IgniteName);

            var cache1 = Ignition.GetIgnite(IgniteName).GetCache<int, int>(Cache1);
            var cache2 = Ignition.GetIgnite(IgniteName).GetCache<int, int>(Cache2);

            // 1. Test rollback.
            using (var tx = ignite.GetTransactions().TxStart())
            {
                cache1.Put(1, 1);
                cache2.Put(2, 2);

                tx.Rollback();
            }

            Assert.AreEqual(1, _dumps.Count);
            var ops = _dumps.First();
            Assert.AreEqual(1, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SesEnd && !op.Commit));

            _dumps = new ConcurrentBag<ICollection<Operation>>();

            // 2. Test puts.
            using (var tx = ignite.GetTransactions().TxStart())
            {
                cache1.Put(1, 1);
                cache2.Put(2, 2);

                tx.Commit();
            }

            Assert.AreEqual(1, _dumps.Count);
            ops = _dumps.First();
            Assert.AreEqual(3, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.Write && Cache1.Equals(op.CacheName) && 1.Equals(op.Key) && 1.Equals(op.Value)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.Write && Cache2.Equals(op.CacheName) && 2.Equals(op.Key) && 2.Equals(op.Value)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SesEnd && op.Commit));

            _dumps = new ConcurrentBag<ICollection<Operation>>();

            // 3. Test removes.
            using (var tx = ignite.GetTransactions().TxStart())
            {
                cache1.Remove(1);
                cache2.Remove(2);

                tx.Commit();
            }

            Assert.AreEqual(1, _dumps.Count);
            ops = _dumps.First();
            Assert.AreEqual(3, ops.Count);

            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.Delete && Cache1.Equals(op.CacheName) && 1.Equals(op.Key)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.Delete && Cache2.Equals(op.CacheName) && 2.Equals(op.Key)));
            Assert.AreEqual(1, ops.Count(op => op.Type == OperationType.SesEnd && op.Commit));
        }

        /// <summary>
        /// Dump operations.
        /// </summary>
        /// <param name="dump">Dump.</param>
        private static void DumpOperations(ICollection<Operation> dump)
        {
            _dumps.Add(dump);
        }

        /// <summary>
        /// Test store implementation.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public class Store : CacheStoreAdapter
        {
            /** Store session. */
            [StoreSessionResource]
#pragma warning disable 649
            private ICacheStoreSession _ses;
#pragma warning restore 649

            /** <inheritdoc /> */
            public override object Load(object key)
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public override void Write(object key, object val)
            {
                GetOperations().Add(new Operation(_ses.CacheName, OperationType.Write, (int)key, (int)val));
            }

            /** <inheritdoc /> */
            public override void Delete(object key)
            {
                GetOperations().Add(new Operation(_ses.CacheName, OperationType.Delete, (int)key, 0));
            }

            /** <inheritdoc /> */
            public override void SessionEnd(bool commit)
            {
                Operation op = new Operation(_ses.CacheName, OperationType.SesEnd) { Commit = commit };

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

                if (!_ses.Properties.TryGetValue("ops", out ops))
                {
                    ops = new List<Operation>();

                    _ses.Properties["ops"] = ops;
                }

                return (ICollection<Operation>) ops;
            } 
        }

        /// <summary>
        /// Logged operation.
        /// </summary>
        private class Operation
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
            public string CacheName { get; private set; }
            
            /// <summary>
            /// Operation type.
            /// </summary>
            public OperationType Type { get; private set; }

            /// <summary>
            /// Key.
            /// </summary>
            public int Key { get; private set; }

            /// <summary>
            /// Value.
            /// </summary>
            public int Value { get; private set; }

            /// <summary>
            /// Commit flag.
            /// </summary>
            public bool Commit { get; set; }
        }

        /// <summary>
        /// Operation types.
        /// </summary>
        private enum OperationType
        {
            /** Write. */
            Write,

            /** Delete. */
            Delete,

            /** Session end. */
            SesEnd
        }
    }
}
