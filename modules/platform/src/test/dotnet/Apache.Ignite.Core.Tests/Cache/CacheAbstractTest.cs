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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Tests.Query;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    ///
    /// </summary>
    class CacheTestKey
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public CacheTestKey()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        public CacheTestKey(int id)
        {
            Id = id;
        }

        /// <summary>
        /// ID.
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            CacheTestKey other = obj as CacheTestKey;

            return other != null && Id == other.Id;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Id;
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            return new StringBuilder()
                .Append(typeof(CacheTestKey).Name)
                .Append(" [id=").Append(Id)
                .Append(']').ToString();
        }
    }

    /// <summary>
    ///
    /// </summary>
    class TestReferenceObject
    {
        public TestReferenceObject Obj;

        /// <summary>
        /// Default constructor.
        /// </summary>
        public TestReferenceObject()
        {
            // No-op.
        }

        public TestReferenceObject(TestReferenceObject obj)
        {
            Obj = obj;
        }
    }

    [Serializable]
    public class TestSerializableObject
    {
        public string Name { get; set; }
        public int Id { get; set; }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            var other = (TestSerializableObject) obj;
            return obj.GetType() == GetType() && (string.Equals(Name, other.Name) && Id == other.Id);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Name != null ? Name.GetHashCode() : 0) * 397) ^ Id;
            }
        }
    }

    /// <summary>
    /// Cache entry processor that adds argument value to the entry value.
    /// </summary>
    [Serializable]
    public class AddArgCacheEntryProcessor : ICacheEntryProcessor<int, int, int, int>
    {
        // Expected exception text
        public const string ExceptionText = "Exception from AddArgCacheEntryProcessor.";

        // Error flag
        public bool ThrowErr { get; set; }

        // Error flag
        public bool ThrowErrPortable { get; set; }

        // Error flag
        public bool ThrowErrNonSerializable { get; set; }

        // Key value to throw error on
        public int ThrowOnKey { get; set; }

        // Remove flag
        public bool Remove { get; set; }

        // Exists flag
        public bool Exists { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AddArgCacheEntryProcessor"/> class.
        /// </summary>
        public AddArgCacheEntryProcessor()
        {
            Exists = true;
            ThrowOnKey = -1;
        }

        /** <inheritdoc /> */
        int ICacheEntryProcessor<int, int, int, int>.Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            if (ThrowOnKey < 0 || ThrowOnKey == entry.Key)
            {
                if (ThrowErr)
                    throw new Exception(ExceptionText);

                if (ThrowErrPortable)
                    throw new PortableTestException {Info = ExceptionText};

                if (ThrowErrNonSerializable)
                    throw new NonSerializableException();
            }

            Assert.AreEqual(Exists, entry.Exists);

            if (Remove)
                entry.Remove();
            else
                entry.Value = entry.Value + arg;
            
            return entry.Value;
        }

        /** <inheritdoc /> */
        public int Process(IMutableCacheEntry<int, int> entry, int arg)
        {
            throw new Exception("Invalid method");
        }
    }

    /// <summary>
    /// Portable add processor.
    /// </summary>
    public class PortableAddArgCacheEntryProcessor : AddArgCacheEntryProcessor, IPortableMarshalAware
    {
        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var w = writer.RawWriter();

            w.WriteBoolean(ThrowErr);
            w.WriteBoolean(ThrowErrPortable);
            w.WriteBoolean(ThrowErrNonSerializable);
            w.WriteInt(ThrowOnKey);
            w.WriteBoolean(Remove);
            w.WriteBoolean(Exists);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            var r = reader.RawReader();

            ThrowErr = r.ReadBoolean();
            ThrowErrPortable = r.ReadBoolean();
            ThrowErrNonSerializable = r.ReadBoolean();
            ThrowOnKey = r.ReadInt();
            Remove = r.ReadBoolean();
            Exists = r.ReadBoolean();
        }
    }

    /// <summary>
    /// Non-serializable processor.
    /// </summary>
    public class NonSerializableCacheEntryProcessor : AddArgCacheEntryProcessor
    {
        // No-op.
    }

    /// <summary>
    /// Portable exception.
    /// </summary>
    public class PortableTestException : Exception, IPortableMarshalAware
    {
        /// <summary>
        /// Gets or sets exception info.
        /// </summary>
        public string Info { get; set; }

        /** <inheritdoc /> */
        public override string Message
        {
            get { return Info; }
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            writer.RawWriter().WriteString(Info);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IPortableReader reader)
        {
            Info = reader.RawReader().ReadString();
        }
    }

    /// <summary>
    /// Non-serializable exception.
    /// </summary>
    public class NonSerializableException : Exception
    {
        // No-op
    }

    /// <summary>
    ///
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedVariable")]
    public abstract class CacheAbstractTest {
        /// <summary>
        ///
        /// </summary>
        [TestFixtureSetUp]
        public virtual void StartGrids() {
            TestUtils.KillProcesses();

            IgniteConfigurationEx cfg = new IgniteConfigurationEx();

            PortableConfiguration portCfg = new PortableConfiguration();

            ICollection<PortableTypeConfiguration> portTypeCfgs = new List<PortableTypeConfiguration>();

            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortablePerson)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(CacheTestKey)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(TestReferenceObject)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableAddArgCacheEntryProcessor)));
            portTypeCfgs.Add(new PortableTypeConfiguration(typeof(PortableTestException)));

            portCfg.TypeConfigurations = portTypeCfgs;

            cfg.PortableConfiguration = portCfg;
            cfg.JvmClasspath = TestUtils.CreateTestClasspath();
            cfg.JvmOptions = TestUtils.TestJavaOptions();
            cfg.SpringConfigUrl = "config\\native-client-test-cache.xml";

            for (int i = 0; i < GridCount(); i++) {
                cfg.GridName = "grid-" + i;

                Ignition.Start(cfg);
            }
        }

        /// <summary>
        ///
        /// </summary>
        [TestFixtureTearDown]
        public virtual void StopGrids() {
            for (int i = 0; i < GridCount(); i++)
                Ignition.Stop("grid-" + i, true);
        }

        /// <summary>
        ///
        /// </summary>
        [SetUp]
        public virtual void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);
        }

        /// <summary>
        ///
        /// </summary>
        [TearDown]
        public virtual void AfterTest() {
            for (int i = 0; i < GridCount(); i++) 
                Cache(i).RemoveAll();

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                if (!cache.IsEmpty)
                {
                    var entries = Enumerable.Range(0, 2000)
                        .Select(x => new KeyValuePair<int, int>(x, cache.LocalPeek(x)))
                        .Where(x => x.Value != 0)
                        .Select(pair => pair.ToString() + GetKeyAffinity(cache, pair.Key))
                        .Aggregate((acc, val) => string.Format("{0}, {1}", acc, val));

                    Assert.Fail("Cache '{0}' is not empty in grid [{1}]: ({2})", CacheName(), i, entries);
                }
            }

            Console.WriteLine("Test finished: " + TestContext.CurrentContext.Test.Name);
        }

        public IIgnite GetIgnite(int idx)
        {
            return Ignition.GetIgnite("grid-" + idx);
        }

        public ICache<int, int> Cache(int idx) {
            return Cache<int, int>(idx);
        }

        public ICache<TK, TV> Cache<TK, TV>(int idx) {
            return GetIgnite(idx).Cache<TK, TV>(CacheName());
        }

        public ICache<int, int> Cache()
        {
            return Cache<int, int>(0);
        }

        public ICache<TK, TV> Cache<TK, TV>()
        {
            return Cache<TK, TV>(0);
        }

        public ICacheAffinity Affinity()
        {
            return GetIgnite(0).Affinity(CacheName());
        }

        public ITransactions Transactions
        {
            get { return GetIgnite(0).Transactions; }
        }

        [Test]
        public void TestCircularReference()
        {
            var cache = Cache().WithKeepPortable<int, object>();

            TestReferenceObject obj1 = new TestReferenceObject();

            obj1.Obj = new TestReferenceObject(obj1);

            cache.Put(1, obj1);

            var po = (IPortableObject) cache.Get(1);

            Assert.IsNotNull(po);

            TestReferenceObject objRef = po.Deserialize<TestReferenceObject>();

            Assert.IsNotNull(objRef);
        }

        [Test]
        public void TestName()
        {
            for (int i = 0; i < GridCount(); i++ )
                Assert.AreEqual(CacheName(), Cache(i).Name);
        }

        [Test]
        public void TestIsEmpty()
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                Assert.IsTrue(cache.IsEmpty);
            }

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                cache.Put(PrimaryKeyForCache(cache), 1);
            }

            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                Assert.IsFalse(cache.IsEmpty);
            }
        }

        [Test]
        public void TestContainsKey()
        {
            var cache = Cache();

            int key = PrimaryKeyForCache(cache);

            cache.Put(key, 1);

            Assert.IsTrue(cache.ContainsKey(key));
            Assert.IsFalse(cache.ContainsKey(-1));
        }
        
        [Test]
        public void TestContainsKeys()
        {
            var cache = Cache();

            var keys = PrimaryKeysForCache(cache, 5);

            Assert.IsFalse(cache.ContainsKeys(keys));

            cache.PutAll(keys.ToDictionary(k => k, k => k));

            Assert.IsTrue(cache.ContainsKeys(keys));

            Assert.IsFalse(cache.ContainsKeys(keys.Concat(new[] {int.MaxValue})));
        }
        
        [Test]
        public void TestPeek()
        {
            var cache = Cache();

            int key1 = PrimaryKeyForCache(cache);

            cache.Put(key1, 1);

            Assert.AreEqual(1, cache.LocalPeek(key1));
            Assert.AreEqual(0, cache.LocalPeek(-1));

            Assert.AreEqual(1, cache.LocalPeek(key1, CachePeekMode.All));
            Assert.AreEqual(0, cache.LocalPeek(-1, CachePeekMode.All));
        }

        [Test]
        public void TestGet()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(0, cache.Get(3));
        }

        [Test]
        public void TestGetAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.Put(1, 1);
            cache.Put(2, 2);
            
            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(0, cache.Get(3));
        }

        [Test]
        public void TestGetAll()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);
            cache.Put(4, 4);
            cache.Put(5, 5);

            IDictionary<int, int> map = cache.GetAll(new List<int> { 0, 1, 2, 5 });

            Assert.AreEqual(3, map.Count);

            Assert.AreEqual(1, map[1]);
            Assert.AreEqual(2, map[2]);
        }

        [Test]
        public void TestGetAllAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            var map = cache.GetAll(new List<int> { 0, 1, 2 });

            Assert.AreEqual(2, map.Count);

            Assert.AreEqual(1, map[1]);
            Assert.AreEqual(2, map[2]);
        }

        [Test]
        public void TestGetAndPut()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Get(1));

            int old = cache.GetAndPut(1, 1);

            Assert.AreEqual(0, old);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPut(1, 2);

            Assert.AreEqual(1, old);

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestGetAndReplace()
        {
            var cache = Cache();

            cache.Put(1, 10);

            Assert.AreEqual(10, cache.GetAndReplace(1, 100));

            Assert.AreEqual(0, cache.GetAndReplace(2, 2));

            Assert.AreEqual(0, cache.Get(2));

            Assert.AreEqual(100, cache.Get(1));

            Assert.IsTrue(cache.Remove(1));
        }

        [Test]
        public void TestGetAndRemove()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(0, cache.GetAndRemove(0));
            
            Assert.AreEqual(1, cache.GetAndRemove(1));
            
            Assert.AreEqual(0, cache.GetAndRemove(1));
            
            Assert.AreEqual(0, cache.Get(1));
        }

        [Test]
        public void TestGetAndPutAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Get(1));

            int old = cache.GetAndPut(1, 1);

            Assert.AreEqual(0, old);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPut(1, 2);

            Assert.AreEqual(1, old);

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestPut()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestPutxAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestPutIfAbsent()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Get(1));

            Assert.AreEqual(true, cache.PutIfAbsent(1, 1));

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(false, cache.PutIfAbsent(1, 2));

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestGetAndPutIfAbsent()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Get(1));

            Assert.AreEqual(0, cache.GetAndPutIfAbsent(1, 1));

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(1, cache.GetAndPutIfAbsent(1, 2));

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestGetAndPutIfAbsentAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Get(1));

            int old = cache.GetAndPutIfAbsent(1, 1);

            Assert.AreEqual(0, old);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndPutIfAbsent(1, 2);

            Assert.AreEqual(1, old);

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestPutIfAbsentAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Get(1));

            Assert.IsTrue(cache.PutIfAbsent(1, 1));

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.PutIfAbsent(1, 2));

            Assert.AreEqual(1, cache.Get(1));
        }

        [Test]
        public void TestReplace()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Get(1));

            bool success = cache.Replace(1, 1);

            Assert.AreEqual(false, success);

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            success = cache.Replace(1, 2);

            Assert.AreEqual(true, success);

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, -1, 3));

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2, 3));

            Assert.AreEqual(3, cache.Get(1));
        }

        [Test]
        public void TestGetAndReplaceAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Get(1));

            int old = cache.GetAndReplace(1, 1);

            Assert.AreEqual(0, old);

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            old = cache.GetAndReplace(1, 2);

            Assert.AreEqual(1, old);

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, -1, 3));

            Assert.AreEqual(2, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2, 3));

            Assert.AreEqual(3, cache.Get(1));
        }

        [Test]
        public void TestReplacex()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, 1));

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2));

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestReplaceAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Get(1));

            Assert.IsFalse(cache.Replace(1, 1));

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsTrue(cache.Replace(1, 2));

            Assert.AreEqual(2, cache.Get(1));
        }

        [Test]
        public void TestPutAll()
        {
            var cache = Cache();

            cache.PutAll(new Dictionary<int, int> { { 1, 1 }, { 2, 2 }, { 3, 3 } });

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));
        }

        [Test]
        public void TestPutAllAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.PutAll(new Dictionary<int, int> { { 1, 1 }, { 2, 2 }, { 3, 3 } });

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));
        }

        /// <summary>
        /// Expiry policy tests.
        /// </summary>
        [Test]
        public void TestWithExpiryPolicy()
        {
            ICache<int, int> cache0 = Cache(0);
            
            int key0;
            int key1;

            if (LocalCache())
            {
                key0 = 0;
                key1 = 1;
            }
            else
            {
                key0 = PrimaryKeyForCache(cache0);
                key1 = PrimaryKeyForCache(Cache(1));
            }
            
            // Test unchanged expiration.
            ICache<int, int> cache = cache0.WithExpiryPolicy(new ExpiryPolicy(null, null, null));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Get(key0);
            cache.Get(key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test eternal expiration.
            cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.MaxValue, TimeSpan.MaxValue, TimeSpan.MaxValue));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache.Get(key0);
            cache.Get(key1);
            Thread.Sleep(200);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test zero expiration.
            cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Get(key0);
            cache.Get(key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test negative expiration.
            cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromMilliseconds(-100), 
                TimeSpan.FromMilliseconds(-100), TimeSpan.FromMilliseconds(-100)));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Get(key0);
            cache.Get(key1);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.RemoveAll(new List<int> { key0, key1 });

            // Test regular expiration.
            cache = cache0.WithExpiryPolicy(new ExpiryPolicy(TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100)));

            cache.Put(key0, key0);
            cache.Put(key1, key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Put(key0, key0 + 1);
            cache.Put(key1, key1 + 1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));

            cache0.Put(key0, key0);
            cache0.Put(key1, key1);
            cache.Get(key0); 
            cache.Get(key1);
            Assert.IsTrue(cache0.ContainsKey(key0));
            Assert.IsTrue(cache0.ContainsKey(key1));
            Thread.Sleep(200);
            Assert.IsFalse(cache0.ContainsKey(key0));
            Assert.IsFalse(cache0.ContainsKey(key1));
        }

        [Test]
        public void TestEvict()
        {
            var cache = Cache();

            int key = PrimaryKeyForCache(cache);

            cache.Put(key, 1);

            Assert.AreEqual(1, PeekInt(cache, key));

            cache.LocalEvict(new[] {key});

            Assert.AreEqual(0, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, key));

            Assert.AreEqual(1, cache.Get(key));

            Assert.AreEqual(1, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(1, PeekInt(cache, key));
        }

        [Test]
        public void TestEvictAllKeys()
        {
            var cache = Cache();

            List<int> keys = PrimaryKeysForCache(cache, 3);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);
            cache.Put(keys[2], 3);

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            cache.LocalEvict(new List<int> { -1, keys[0], keys[1] });

            Assert.AreEqual(1, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, keys[0]));
            Assert.AreEqual(0, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            Assert.AreEqual(3, cache.LocalSize());

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));
        }

        [Test]
        public void TestClear()
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                cache.Put(PrimaryKeyForCache(cache, 500), 1);

                Assert.IsFalse(cache.IsEmpty);
            }

            Cache().Clear();

            for (int i = 0; i < GridCount(); i++)
                Assert.IsTrue(Cache(i).IsEmpty);
        }

        [Test]
        public void TestClearKey()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            var i = cache.Size();

            foreach (var key in keys)
            {
                cache.Clear(key);

                Assert.AreEqual(0, cache.Get(key));

                Assert.Less(cache.Size(), i);

                i = cache.Size();
            }
        }

        [Test]
        public void TestClearKeys()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            cache.ClearAll(keys);

            foreach (var key in keys)
                Assert.AreEqual(0, cache.Get(key));
        }

        [Test]
        public void TestLocalClearKey()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            var i = cache.Size();

            foreach (var key in keys)
            {
                cache.LocalClear(key);

                Assert.AreEqual(0, cache.LocalPeek(key));

                Assert.Less(cache.Size(), i);

                i = cache.Size();
            }

            cache.Clear();
        }

        [Test]
        public void TestLocalClearKeys()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 10);

            foreach (var key in keys)
                cache.Put(key, 3);

            cache.LocalClearAll(keys);

            foreach (var key in keys)
                Assert.AreEqual(0, cache.LocalPeek(key));

            cache.Clear();
        }

        [Test]
        public void TestRemove()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(true, cache.Remove(1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(1, -1));
            Assert.IsTrue(cache.Remove(1, 1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));
        }

        [Test]
        public void TestGetAndRemoveAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(1, cache.GetAndRemove(1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(1, -1));
            Assert.IsTrue(cache.Remove(1, 1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));
        }

        [Test]
        public void TestRemovex()
        {
            var cache = Cache();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(-1));
            Assert.IsTrue(cache.Remove(1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));
        }

        [Test]
        public void TestRemoveAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            cache.Put(1, 1);

            Assert.AreEqual(1, cache.Get(1));

            Assert.IsFalse(cache.Remove(-1));
            Assert.IsTrue(cache.Remove(1));

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(1));
        }

        [Test]
        public void TestRemoveAll()
        {
            var cache = Cache();

            List<int> keys = PrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            cache.RemoveAll();

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(keys[0]));
            Assert.AreEqual(0, cache.Get(keys[1]));
        }

        [Test]
        public void TestRemoveAllAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            List<int> keys = PrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            Assert.AreEqual(1, cache.Get(keys[0]));
            Assert.AreEqual(2, cache.Get(keys[1]));

            cache.RemoveAll();

            Assert.AreEqual(0, cache.Size());

            Assert.AreEqual(0, cache.Get(keys[0]));
            Assert.AreEqual(0, cache.Get(keys[1]));
        }

        [Test]
        public void TestRemoveAllKeys()
        {
            var cache = Cache();

            Assert.AreEqual(0, cache.Size());

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));

            cache.RemoveAll(new List<int> { 0, 1, 2 });

            Assert.AreEqual(1, cache.Size(CachePeekMode.Primary));

            Assert.AreEqual(0, cache.Get(1));
            Assert.AreEqual(0, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));
        }

        [Test]
        public void TestRemoveAllKeysAsync()
        {
            var cache = Cache().WithAsync().WrapAsync();

            Assert.AreEqual(0, cache.Size());

            cache.Put(1, 1);
            cache.Put(2, 2);
            cache.Put(3, 3);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));

            cache.RemoveAll(new List<int> { 0, 1, 2 });

            Assert.AreEqual(1, cache.Size(CachePeekMode.Primary));

            Assert.AreEqual(0, cache.Get(1));
            Assert.AreEqual(0, cache.Get(2));
            Assert.AreEqual(3, cache.Get(3));
        }

        [Test]
        public void TestSizes()
        {
            for (int i = 0; i < GridCount(); i++)
            {
                var cache = Cache(i);

                List<int> keys = PrimaryKeysForCache(cache, 2);

                foreach (int key in keys)
                    cache.Put(key, 1);

                Assert.IsTrue(cache.Size() >= 2);
                Assert.AreEqual(2, cache.LocalSize(CachePeekMode.Primary));
            }

            ICache<int, int> cache0 = Cache();

            Assert.AreEqual(GridCount() * 2, cache0.Size(CachePeekMode.Primary));

            if (!LocalCache() && !ReplicatedCache())
            {
                int nearKey = NearKeyForCache(cache0);

                cache0.Put(nearKey, 1);

                Assert.AreEqual(NearEnabled() ? 1 : 0, cache0.Size(CachePeekMode.Near));
            }
        }

        [Test]
        public void TestLocalSize()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 3);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);

            var localSize = cache.LocalSize();

            cache.LocalEvict(keys.Take(2).ToArray());

            Assert.AreEqual(0, cache.LocalSize(CachePeekMode.Onheap));
            Assert.AreEqual(localSize, cache.LocalSize(CachePeekMode.All));

            cache.Put(keys[2], 3);

            Assert.AreEqual(localSize + 1, cache.LocalSize(CachePeekMode.All));

            cache.RemoveAll(keys.Take(2).ToArray());
        }

        /// <summary>
        /// Test enumerators.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public void TestEnumerators()
        {
            var cache = Cache();
            var keys = PrimaryKeysForCache(cache, 2);

            cache.Put(keys[0], keys[0] + 1);
            cache.Put(keys[1], keys[1] + 1);

            // Check distributed enumerator.
            IEnumerable<ICacheEntry<int, int>> e = cache;

            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            // Check local enumerator.
            e = cache.GetLocalEntries();

            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            // Evict and check peek modes.
            cache.LocalEvict(new List<int> { keys[0] } );

            e = cache.GetLocalEntries(CachePeekMode.Onheap);
            CheckEnumerator(e.GetEnumerator(), new List<int> { keys[1] });
            CheckEnumerator(e.GetEnumerator(), new List<int> { keys[1] });

            e = cache.GetLocalEntries(CachePeekMode.All);
            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            e = cache.GetLocalEntries(CachePeekMode.Onheap, CachePeekMode.Swap);
            CheckEnumerator(e.GetEnumerator(), keys);
            CheckEnumerator(e.GetEnumerator(), keys);

            cache.Remove(keys[0]);
        }

        /// <summary>
        /// Check enumerator content.
        /// </summary>
        /// <param name="e">Enumerator.</param>
        /// <param name="keys">Keys.</param>
        private static void CheckEnumerator(IEnumerator<ICacheEntry<int, int>> e, IList<int> keys)
        {
            CheckEnumerator0(e, keys);

            e.Reset();

            CheckEnumerator0(e, keys);

            e.Dispose();

            Assert.Throws<ObjectDisposedException>(() => { e.MoveNext(); });
            Assert.Throws<ObjectDisposedException>(() => { var entry = e.Current; });
            Assert.Throws<ObjectDisposedException>(e.Reset);

            e.Dispose();
        }

        /// <summary>
        /// Check enumerator content.
        /// </summary>
        /// <param name="e">Enumerator.</param>
        /// <param name="keys">Keys.</param>
        private static void CheckEnumerator0(IEnumerator<ICacheEntry<int, int>> e, IList<int> keys)
        {
            Assert.Throws<InvalidOperationException>(() => { var entry = e.Current; });

            int cnt = 0;

            while (e.MoveNext())
            {
                ICacheEntry<int, int> entry = e.Current;

                Assert.IsTrue(keys.Contains(entry.Key), "Unexpected entry: " + entry);

                Assert.AreEqual(entry.Key + 1, entry.Value);

                cnt++;
            }

            Assert.AreEqual(keys.Count, cnt);

            Assert.IsFalse(e.MoveNext());

            Assert.Throws<InvalidOperationException>(() => { var entry = e.Current; });
        }

        [Test]
        public void TestPromote()
        {
            var cache = Cache();

            int key = PrimaryKeyForCache(cache);

            cache.Put(key, 1);

            Assert.AreEqual(1, PeekInt(cache, key));

            cache.LocalEvict(new[] {key});

            Assert.AreEqual(0, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, key));

            cache.LocalPromote(new[] { key });

            Assert.AreEqual(1, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(1, PeekInt(cache, key));
        }

        [Test]
        public void TestPromoteAll()
        {
            var cache = Cache();

            List<int> keys = PrimaryKeysForCache(cache, 3);

            cache.Put(keys[0], 1);
            cache.Put(keys[1], 2);
            cache.Put(keys[2], 3);

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            cache.LocalEvict(new List<int> { -1, keys[0], keys[1] });

            Assert.AreEqual(1, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(0, PeekInt(cache, keys[0]));
            Assert.AreEqual(0, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));

            cache.LocalPromote(new[] {keys[0], keys[1]});

            Assert.AreEqual(3, cache.LocalSize(CachePeekMode.Onheap));

            Assert.AreEqual(1, PeekInt(cache, keys[0]));
            Assert.AreEqual(2, PeekInt(cache, keys[1]));
            Assert.AreEqual(3, PeekInt(cache, keys[2]));
        }

        [Test]
        public void TestPutGetPortable()
        {
            var cache = Cache<int, PortablePerson>();

            PortablePerson obj1 = new PortablePerson("obj1", 1);

            cache.Put(1, obj1);

            obj1 = cache.Get(1);

            Assert.AreEqual("obj1", obj1.Name);
            Assert.AreEqual(1, obj1.Age);
        }

        [Test]
        public void TestPutGetPortableAsync()
        {
            var cache = Cache<int, PortablePerson>().WithAsync().WrapAsync();

            PortablePerson obj1 = new PortablePerson("obj1", 1);

            cache.Put(1, obj1);

            obj1 = cache.Get(1);

            Assert.AreEqual("obj1", obj1.Name);
            Assert.AreEqual(1, obj1.Age);
        }

        [Test]
        public void TestPutGetPortableKey()
        {
            var cache = Cache<CacheTestKey, string>();

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache.Put(new CacheTestKey(i), "val-" + i);

            for (int i = 0; i < cnt; i++)
                Assert.AreEqual("val-" + i, cache.Get(new CacheTestKey(i)));
        }

        [Test]
        public void TestGetAsync2()
        {
            var cache = Cache().WithAsync();

            for (int i = 0; i < 100; i++)
            {
                cache.Put(i, i);

                cache.GetFuture<object>().Get();
            }

            var futs = new List<IFuture<int>>();

            for (int i = 0; i < 1000; i++)
            {
                cache.Get(i % 100);

                futs.Add(cache.GetFuture<int>());
            }

            for (int i = 0; i < 1000; i++) {
                Assert.AreEqual(i % 100, futs[i].Get(), "Unexpected result: " + i);

                Assert.IsTrue(futs[i].IsDone);
            }
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestGetAsyncMultithreaded()
        {
            var cache = Cache().WithAsync();

            for (int i = 0; i < 100; i++)
            {
                cache.Put(i, i);

                cache.GetFuture<object>().Get();
            }

            TestUtils.RunMultiThreaded(() =>
            {
                for (int i = 0; i < 100; i++)
                {
                    var futs = new List<IFuture<int>>();

                    for (int j = 0; j < 100; j++)
                    {
                        cache.Get(j);

                        futs.Add(cache.GetFuture<int>());
                    }

                    for (int j = 0; j < 100; j++)
                        Assert.AreEqual(j, futs[j].Get());
                }
            }, 10);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPutxAsyncMultithreaded()
        {
            var cache = Cache().WithAsync();

            TestUtils.RunMultiThreaded(() =>
            {
                Random rnd = new Random();

                for (int i = 0; i < 50; i++)
                {
                    var futs = new List<IFuture<object>>();

                    for (int j = 0; j < 10; j++)
                    {
                        cache.Put(rnd.Next(1000), i);

                        futs.Add(cache.GetFuture<object>());
                    }

                    foreach (var fut in futs)
                        fut.Get();
                }
            }, 5);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPutGetAsyncMultithreaded()
        {
            var cache = Cache<CacheTestKey, PortablePerson>().WithAsync();

            const int threads = 10;
            const int objPerThread = 1000;

            int cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<IFuture<object>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Put(new CacheTestKey(key), new PortablePerson("Person-" + key, key));

                    futs.Add(cache.GetFuture<object>());
                }

                foreach (var fut in futs)
                {
                    fut.Get();

                    Assert.IsTrue(fut.IsDone);
                }
            }, threads);

            for (int i = 0; i < threads; i++)
            {
                int threadIdx = i + 1;

                for (int j = 0; j < objPerThread; j++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Get(new CacheTestKey(key));
                    var p = cache.GetFuture<PortablePerson>().Get();

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Age);
                    Assert.AreEqual("Person-" + key, p.Name);
                }
            }

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Put(new CacheTestKey(key), new PortablePerson("Person-" + key, key));

                    cache.GetFuture<object>().Get();
                }
            }, threads);

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<IFuture<PortablePerson>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Get(new CacheTestKey(key));

                    futs.Add(cache.GetFuture<PortablePerson>());
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    int key = threadIdx * objPerThread + i;

                    var p = fut.Get();

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Age);
                    Assert.AreEqual("Person-" + key, p.Name);
                }
            }, threads);
        }

        //[Test]
        //[Category(TestUtils.CATEGORY_INTENSIVE)]
        public void TestAsyncMultithreadedKeepPortable()
        {
            var cache = Cache().WithAsync().WithKeepPortable<CacheTestKey, PortablePerson>();
            var portCache = Cache().WithAsync().WithKeepPortable<CacheTestKey, IPortableObject>();

            const int threads = 10;
            const int objPerThread = 1000;

            int cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<IFuture<object>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Put(new CacheTestKey(key), new PortablePerson("Person-" + key, key));

                    futs.Add(cache.GetFuture<object>());
                }

                foreach (var fut in futs)
                    Assert.IsNull(fut.Get());
            }, threads);

            for (int i = 0; i < threads; i++)
            {
                int threadIdx = i + 1;

                for (int j = 0; j < objPerThread; j++)
                {
                    int key = threadIdx * objPerThread + i;

                    IPortableObject p = portCache.Get(new CacheTestKey(key));

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Field<int>("age"));
                    Assert.AreEqual("Person-" + key, p.Field<string>("name"));
                }
            }

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<IFuture<IPortableObject>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    portCache.Get(new CacheTestKey(key));

                    futs.Add(cache.GetFuture<IPortableObject>());
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    int key = threadIdx * objPerThread + i;

                    var p = fut.Get();

                    Assert.IsNotNull(p);
                    Assert.AreEqual(key, p.Field<int>("age"));
                    Assert.AreEqual("Person-" + key, p.Field<string>("name"));
                }
            }, threads);

            cntr = 0;

            TestUtils.RunMultiThreaded(() =>
            {
                int threadIdx = Interlocked.Increment(ref cntr);

                var futs = new List<IFuture<bool>>();

                for (int i = 0; i < objPerThread; i++)
                {
                    int key = threadIdx * objPerThread + i;

                    cache.Remove(new CacheTestKey(key));

                    futs.Add(cache.GetFuture<bool>());
                }

                for (int i = 0; i < objPerThread; i++)
                {
                    var fut = futs[i];

                    Assert.AreEqual(true, fut.Get());
                }
            }, threads);
        }

        [Test]
        [Ignore("IGNITE-835")]
        public void TestLock()
        {
            if (!LockingEnabled())
                return;

            var cache = Cache();

            const int key = 7;

            // Lock
            CheckLock(cache, key, () => cache.Lock(key));

            // LockAll
            CheckLock(cache, key, () => cache.LockAll(new[] { key, 2, 3, 4, 5 }));
        }

        /// <summary>
        /// Internal lock test routine.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="key">Key.</param>
        /// <param name="getLock">Function to get the lock.</param>
        private static void CheckLock(ICache<int, int> cache, int key, Func<ICacheLock> getLock)
        {
            var sharedLock = getLock();
            
            using (sharedLock)
            {
                Assert.Throws<InvalidOperationException>(() => sharedLock.Exit());  // can't exit if not entered

                sharedLock.Enter();

                try
                {
                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    EnsureCannotLock(getLock, sharedLock);

                    sharedLock.Enter();

                    try
                    {
                        Assert.IsTrue(cache.IsLocalLocked(key, true));
                        Assert.IsTrue(cache.IsLocalLocked(key, false));

                        EnsureCannotLock(getLock, sharedLock);
                    }
                    finally
                    {
                        sharedLock.Exit();
                    }

                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    EnsureCannotLock(getLock, sharedLock);

                    Assert.Throws<SynchronizationLockException>(() => sharedLock.Dispose()); // can't dispose while locked
                }
                finally
                {
                    sharedLock.Exit();
                }

                Assert.IsFalse(cache.IsLocalLocked(key, true));
                Assert.IsFalse(cache.IsLocalLocked(key, false));

                var innerTask = new Task(() =>
                {
                    Assert.IsTrue(sharedLock.TryEnter());
                    sharedLock.Exit();

                    using (var otherLock = getLock())
                    {
                        Assert.IsTrue(otherLock.TryEnter());
                        otherLock.Exit();
                    }
                });

                innerTask.Start();
                innerTask.Wait();
            }
            
            Assert.IsFalse(cache.IsLocalLocked(key, true));
            Assert.IsFalse(cache.IsLocalLocked(key, false));
            
            var outerTask = new Task(() =>
            {
                using (var otherLock = getLock())
                {
                    Assert.IsTrue(otherLock.TryEnter());
                    otherLock.Exit();
                }
            });

            outerTask.Start();
            outerTask.Wait();

            Assert.Throws<ObjectDisposedException>(() => sharedLock.Enter());  // Can't enter disposed lock
        }

        /// <summary>
        /// ENsure taht lock cannot be obtained by other threads.
        /// </summary>
        /// <param name="getLock">Get lock function.</param>
        /// <param name="sharedLock">Shared lock.</param>
        private static void EnsureCannotLock(Func<ICacheLock> getLock, ICacheLock sharedLock)
        {
            var task = new Task(() =>
            {
                Assert.IsFalse(sharedLock.TryEnter());
                Assert.IsFalse(sharedLock.TryEnter(TimeSpan.FromMilliseconds(100)));

                using (var otherLock = getLock())
                {
                    Assert.IsFalse(otherLock.TryEnter());
                    Assert.IsFalse(otherLock.TryEnter(TimeSpan.FromMilliseconds(100)));
                }
            });

            task.Start();
            task.Wait();
        }

        [Test]
        public void TestTxCommit()
        {
            TestTxCommit(false);
        }

        [Test]
        public void TestTxCommitAsync()
        {
            TestTxCommit(true);
        }

        private void TestTxCommit(bool async)
        {
            if (!TxEnabled())
                return;

            var cache = Cache();

            ITransaction tx = Transactions.Tx;

            Assert.IsNull(tx);

            tx = Transactions.TxStart();

            try
            {
                cache.Put(1, 1);

                cache.Put(2, 2);

                if (async)
                {
                    var asyncTx = tx.WithAsync();
                    
                    asyncTx.Commit();

                    var fut = asyncTx.GetFuture();

                    fut.Get();

                    Assert.IsTrue(fut.IsDone);
                    Assert.AreEqual(fut.Get(), null);
                }
                else
                    tx.Commit();
            }
            finally
            {
                tx.Dispose();
            }

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(2, cache.Get(2));

            tx = Transactions.Tx;

            Assert.IsNull(tx);
        }

        [Test]
        public void TestTxRollback()
        {
            if (!TxEnabled())
                return;

            var cache = Cache();

            cache.Put(1, 1);

            cache.Put(2, 2);

            ITransaction tx = Transactions.Tx;

            Assert.IsNull(tx);

            tx = Transactions.TxStart();

            try {
                cache.Put(1, 10);

                cache.Put(2, 20);
            }
            finally {
                tx.Rollback();
            }

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(2, cache.Get(2));

            Assert.IsNull(Transactions.Tx);
        }

        [Test]
        public void TestTxClose()
        {
            if (!TxEnabled())
                return;

            var cache = Cache();

            cache.Put(1, 1);

            cache.Put(2, 2);

            ITransaction tx = Transactions.Tx;

            Assert.IsNull(tx);

            tx = Transactions.TxStart();

            try
            {
                cache.Put(1, 10);

                cache.Put(2, 20);
            }
            finally
            {
                tx.Dispose();
            }

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(2, cache.Get(2));

            tx = Transactions.Tx;

            Assert.IsNull(tx);
        }
        
        [Test]
        public void TestTxAllModes()
        {
            TestTxAllModes(false);

            TestTxAllModes(true);

            Console.WriteLine("Done");
        }

        protected void TestTxAllModes(bool withTimeout)
        {
            if (!TxEnabled())
                return;

            var cache = Cache();

            int cntr = 0;

            foreach (TransactionConcurrency concurrency in Enum.GetValues(typeof(TransactionConcurrency))) {
                foreach (TransactionIsolation isolation in Enum.GetValues(typeof(TransactionIsolation))) {
                    Console.WriteLine("Test tx [concurrency=" + concurrency + ", isolation=" + isolation + "]");

                    ITransaction tx = Transactions.Tx;

                    Assert.IsNull(tx);

                    tx = withTimeout 
                        ? Transactions.TxStart(concurrency, isolation, TimeSpan.FromMilliseconds(1100), 10)
                        : Transactions.TxStart(concurrency, isolation);

                    Assert.AreEqual(concurrency, tx.Concurrency);
                    Assert.AreEqual(isolation, tx.Isolation);

                    if (withTimeout)
                        Assert.AreEqual(1100, tx.Timeout.TotalMilliseconds);

                    try {
                        cache.Put(1, cntr);

                        tx.Commit();
                    }
                    finally {
                        tx.Dispose();
                    }

                    tx = Transactions.Tx;

                    Assert.IsNull(tx);

                    Assert.AreEqual(cntr, cache.Get(1));

                    cntr++;
                }
            }
        }

        [Test]
        public void TestTxAttributes()
        {
            if (!TxEnabled())
                return;

            ITransaction tx = Transactions.TxStart(TransactionConcurrency.Optimistic,
                TransactionIsolation.RepeatableRead, TimeSpan.FromMilliseconds(2500), 100);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime.Ticks > 0);
            Assert.AreEqual(tx.NodeId, GetIgnite(0).Cluster.LocalNode.Id);

            DateTime startTime1 = tx.StartTime;

            tx.Commit();

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionState.Committed, tx.State);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime1, tx.StartTime);

            Thread.Sleep(100);

            tx = Transactions.TxStart(TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted,
                TimeSpan.FromMilliseconds(3500), 200);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Pessimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.ReadCommitted, tx.Isolation);
            Assert.AreEqual(3500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime.Ticks > 0);
            Assert.IsTrue(tx.StartTime > startTime1);

            DateTime startTime2 = tx.StartTime;

            tx.Rollback();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);
            Assert.AreEqual(TransactionConcurrency.Pessimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.ReadCommitted, tx.Isolation);
            Assert.AreEqual(3500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime2, tx.StartTime);

            Thread.Sleep(100);

            tx = Transactions.TxStart(TransactionConcurrency.Optimistic, TransactionIsolation.RepeatableRead,
                TimeSpan.FromMilliseconds(2500), 100);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime > startTime2);

            DateTime startTime3 = tx.StartTime;

            tx.Commit();

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionState.Committed, tx.State);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime3, tx.StartTime);
        }

        [Test]
        public void TestTxRollbackOnly()
        {
            if (!TxEnabled())
                return;

            var cache = Cache();

            cache.Put(1, 1);

            cache.Put(2, 2);

            ITransaction tx = Transactions.TxStart();

            cache.Put(1, 10);

            cache.Put(2, 20);

            Assert.IsFalse(tx.IsRollbackOnly);

            tx.SetRollbackonly();

            Assert.IsTrue(tx.IsRollbackOnly);

            Assert.AreEqual(TransactionState.MarkedRollback, tx.State);

            try
            {
                tx.Commit();

                Assert.Fail("Commit must fail.");
            }
            catch (IgniteException e)
            {
                Console.WriteLine("Expected exception: " + e);
            }

            tx.Dispose();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);

            Assert.IsTrue(tx.IsRollbackOnly);

            Assert.AreEqual(1, cache.Get(1));

            Assert.AreEqual(2, cache.Get(2));

            tx = Transactions.Tx;

            Assert.IsNull(tx);
        }

        [Test]
        public void TestTxMetrics()
        {
            if (!TxEnabled())
                return;

            var cache = Cache();
            
            var startTime = DateTime.UtcNow.AddSeconds(-1);

            Transactions.ResetMetrics();

            var metrics = Transactions.GetMetrics();
            
            Assert.AreEqual(0, metrics.TxCommits);
            Assert.AreEqual(0, metrics.TxRollbacks);

            using (Transactions.TxStart())
            {
                cache.Put(1, 1);
            }
            
            using (var tx = Transactions.TxStart())
            {
                cache.Put(1, 1);
                tx.Commit();
            }

            metrics = Transactions.GetMetrics();

            Assert.AreEqual(1, metrics.TxCommits);
            Assert.AreEqual(1, metrics.TxRollbacks);

            Assert.LessOrEqual(startTime, metrics.CommitTime);
            Assert.LessOrEqual(startTime, metrics.RollbackTime);

            Assert.GreaterOrEqual(DateTime.UtcNow, metrics.CommitTime);
            Assert.GreaterOrEqual(DateTime.UtcNow, metrics.RollbackTime);
        }

        [Test]
        public void TestTxStateAndExceptions()
        {
            if (!TxEnabled())
                return;

            var tx = Transactions.TxStart();
            
            Assert.AreEqual(TransactionState.Active, tx.State);

            tx.Rollback();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);

            try
            {
                tx.Commit();
                Assert.Fail();
            }
            catch (InvalidOperationException)
            {
                // Expected
            }

            tx = Transactions.TxStart().WithAsync();

            Assert.AreEqual(TransactionState.Active, tx.State);

            tx.Commit();

            tx.GetFuture().Get();

            Assert.AreEqual(TransactionState.Committed, tx.State);

            tx.Rollback();  // Illegal, but should not fail here; will fail in future

            try
            {
                tx.GetFuture<object>().Get();
                Assert.Fail();
            }
            catch (InvalidOperationException)
            {
                // Expected
            }
        }
        
        /// <summary>
        /// Test thraed-locals leak.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestThreadLocalLeak()
        {
            var cache = Cache<string, string>();

            Exception err = null;

            const int threadCnt = 10;

            Thread[] threads = new Thread[threadCnt];

            ThreadStart[] threadStarts = new ThreadStart[threadCnt];

            for (int j = 0; j < threadCnt; j++)
            {
                string key = "key" + j;

                threadStarts[j] = () =>
                {
                    try
                    {
                        cache.Put(key, key);

                        Assert.AreEqual(key, cache.Get(key));
                    }
                    catch (Exception e)
                    {
                        Interlocked.CompareExchange(ref err, e, null);

                        Assert.Fail("Unexpected error: " + e);
                    }
                };
            }

            for (int i = 0; i < 100 && err == null; i++)
            {
                for (int j = 0 ; j < threadCnt; j++) {
                    Thread t = new Thread(threadStarts[j]);

                    threads[j] = t;
                }

                foreach (Thread t in threads)
                    t.Start();

                foreach (Thread t in threads)
                    t.Join();

                if (i % 500 == 0)
                {
                    Console.WriteLine("Iteration: " + i);

                    GC.Collect();
                }
            }

            Assert.IsNull(err);
        }
        
        /**
         * Test tries to provoke garbage collection for .Net future before it was completed to verify
         * futures pinning works.
         */
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestFuturesGc()
        {
            var cache = Cache().WithAsync();

            cache.Put(1, 1);

            for (int i = 0; i < 10; i++)
            {
                TestUtils.RunMultiThreaded(() =>
                {
                    for (int j = 0; j < 1000; j++)
                        cache.Get(1);
                }, 5);

                GC.Collect();

                cache.Get(1);
                Assert.AreEqual(1, cache.GetFuture<int>().Get());
            }

            Thread.Sleep(2000);
        }

        [Test]
        public void TestPartitions()
        {
            ICacheAffinity aff = Affinity();

            for (int i = 0; i < 5; i++ )
                Assert.AreEqual(CachePartitions(), aff.Partitions);
        }

        [Test]
        public void TestKeyPartition()
        {
            ICacheAffinity aff = Affinity();

            {
                ISet<int> parts = new HashSet<int>();

                for (int i = 0; i < 1000; i++)
                    parts.Add(aff.Partition(i));

                if (LocalCache())
                    Assert.AreEqual(1, parts.Count);
                else
                    Assert.IsTrue(parts.Count > 10);
            }

            {
                ISet<int> parts = new HashSet<int>();

                for (int i = 0; i < 1000; i++)
                    parts.Add(aff.Partition("key" + i));

                if (LocalCache())
                    Assert.AreEqual(1, parts.Count);
                else
                    Assert.IsTrue(parts.Count > 10);
            }
        }

        [Test]
        public void TestIsPrimaryOrBackup()
        {
            ICacheAffinity aff = Affinity();

            ICollection<IClusterNode> nodes = GetIgnite(0).Cluster.Nodes();

            Assert.IsTrue(nodes.Count > 0);

            IClusterNode node = nodes.First();

            {
                bool found = false;

                for (int i = 0; i < 1000; i++)
                {
                    if (aff.IsPrimary(node, i))
                    {
                        Assert.IsTrue(aff.IsPrimaryOrBackup(node, i));

                        found = true;

                        if (nodes.Count > 1)
                            Assert.IsFalse(aff.IsPrimary(nodes.Last(), i));

                        break;
                    }
                }

                Assert.IsTrue(found, "Failed to find primary key for node " + node);
            }

            if (nodes.Count > 1)
            {
                bool found = false;

                for (int i = 0; i < 1000; i++)
                {
                    if (aff.IsBackup(node, i))
                    {
                        Assert.IsTrue(aff.IsPrimaryOrBackup(node, i));

                        found = true;

                        break;
                    }
                }

                Assert.IsTrue(found, "Failed to find backup key for node " + node);
            }
        }

        [Test]
        public void TestNodePartitions()
        {
            ICacheAffinity aff = Affinity();

            ICollection<IClusterNode> nodes = GetIgnite(0).Cluster.Nodes();

            Assert.IsTrue(nodes.Count > 0);

            if (nodes.Count == 1)
            {
                IClusterNode node = nodes.First();

                int[] parts = aff.BackupPartitions(node);

                Assert.AreEqual(0, parts.Length);

                parts = aff.AllPartitions(node);

                Assert.AreEqual(CachePartitions(), parts.Length);
            }
            else
            {
                IList<int> allPrimaryParts = new List<int>();
                IList<int> allBackupParts = new List<int>();
                IList<int> allParts = new List<int>();

                foreach(IClusterNode node in nodes) {
                    int[] parts = aff.PrimaryPartitions(node);

                    foreach (int part in parts)
                        allPrimaryParts.Add(part);

                    parts = aff.BackupPartitions(node);

                    foreach (int part in parts)
                        allBackupParts.Add(part);

                    parts = aff.AllPartitions(node);

                    foreach (int part in parts)
                        allParts.Add(part);
                }

                Assert.AreEqual(CachePartitions(), allPrimaryParts.Count);
                Assert.AreEqual(CachePartitions() * Backups(), allBackupParts.Count);
                Assert.AreEqual(CachePartitions() * (Backups() + 1), allParts.Count);
            }
        }

        [Test]
        public void TestAffinityKey()
        {
            ICacheAffinity aff = Affinity();

            Assert.AreEqual(10, aff.AffinityKey<int, int>(10));

            Assert.AreEqual("string", aff.AffinityKey<string, string>("string"));
        }

        [Test]
        public void TestMapToNode()
        {
            ICacheAffinity aff = Affinity();

            const int key = 1;

            IClusterNode node = aff.MapKeyToNode(key);

            Assert.IsNotNull(node);

            Assert.IsTrue(GetIgnite(0).Cluster.Nodes().Contains(node));

            Assert.IsTrue(aff.IsPrimary(node, key));

            Assert.IsTrue(aff.IsPrimaryOrBackup(node, key));

            Assert.IsFalse(aff.IsBackup(node, key));

            int part = aff.Partition(key);

            IClusterNode partNode = aff.MapPartitionToNode(part);

            Assert.AreEqual(node, partNode);
        }

        [Test]
        public void TestMapToPrimaryAndBackups()
        {
            ICacheAffinity aff = Affinity();

            const int key = 1;

            IList<IClusterNode> nodes = aff.MapKeyToPrimaryAndBackups(key);

            Assert.IsTrue(nodes.Count > 0);

            for (int i = 0; i < nodes.Count; i++)
            {
                if (i == 0)
                    Assert.IsTrue(aff.IsPrimary(nodes[i], key));
                else
                    Assert.IsTrue(aff.IsBackup(nodes[i], key));
            }

            int part = aff.Partition(key);

            IList<IClusterNode> partNodes = aff.MapPartitionToPrimaryAndBackups(part);

            Assert.AreEqual(nodes, partNodes);
        }

        [Test]
        public void TestMapKeysToNodes()
        {
            ICacheAffinity aff = Affinity();

            IList<int> keys = new List<int> {1, 2, 3};

            IDictionary<IClusterNode, IList<int>> map = aff.MapKeysToNodes(keys);

            Assert.IsTrue(map.Count > 0);

            foreach (int key in keys)
            {
                IClusterNode primary = aff.MapKeyToNode(key);

                Assert.IsTrue(map.ContainsKey(primary));

                IList<int> nodeKeys = map[primary];

                Assert.IsNotNull(nodeKeys);

                Assert.IsTrue(nodeKeys.Contains(key));
            }
        }

        [Test]
        public void TestMapPartitionsToNodes()
        {
            ICacheAffinity aff = Affinity();

            if (LocalCache())
            {
                IList<int> parts = new List<int> { 0 };

                IDictionary<int, IClusterNode> map = aff.MapPartitionsToNodes(parts);

                Assert.AreEqual(parts.Count, map.Count);

                Assert.AreEqual(GetIgnite(0).Cluster.LocalNode, map[0]);
            }
            else
            {
                IList<int> parts = new List<int> { 1, 2, 3 };

                IDictionary<int, IClusterNode> map = aff.MapPartitionsToNodes(parts);

                Assert.AreEqual(parts.Count, map.Count);

                foreach (int part in parts)
                {
                    Assert.IsTrue(map.ContainsKey(part));

                    IClusterNode primary = aff.MapPartitionToNode(part);

                    Assert.AreEqual(primary, map[part], "Wrong node for partition: " + part);
                }
            }
        }

        [Test]
        public void TestKeepPortableFlag()
        {
            TestKeepPortableFlag(false);
        }

        [Test]
        public void TestKeepPortableFlagAsync()
        {
            TestKeepPortableFlag(true);
        }

        [Test]
        public void TestNearKeys()
        {
            if (!NearEnabled())
                return;

            const int count = 20;

            var cache = Cache();
            var aff = cache.Ignite.Affinity(cache.Name);
            var node = cache.Ignite.Cluster.LocalNode;

            for (int i = 0; i < count; i++)
                cache.Put(i, -i - 1);

            var nearKeys = Enumerable.Range(0, count).Where(x => !aff.IsPrimaryOrBackup(node, x)).ToArray();

            var nearKeysString = nearKeys.Select(x => x.ToString()).Aggregate((x, y) => x + ", " + y);

            Console.WriteLine("Near keys: " + nearKeysString);

            foreach (var nearKey in nearKeys.Take(3))
                Assert.AreNotEqual(0, cache.Get(nearKey));
        }
        
        [Test]
        public void TestSerializable()
        {
            var cache = Cache<int, TestSerializableObject>();

            var obj = new TestSerializableObject {Name = "Vasya", Id = 128};

            cache.Put(1, obj);

            var resultObj = cache.Get(1);

            Assert.AreEqual(obj, resultObj);
        }

        [Test]
        public void TestInvoke()
        {
            TestInvoke(false);
        }

        [Test]
        public void TestInvokeAsync()
        {
            TestInvoke(true);
        }

        private void TestInvoke(bool async)
        {
            TestInvoke<AddArgCacheEntryProcessor>(async);
            TestInvoke<PortableAddArgCacheEntryProcessor>(async);

            try
            {
                TestInvoke<NonSerializableCacheEntryProcessor>(async);
                Assert.Fail();
            }
            catch (SerializationException)
            {
                // Expected
            }
        }

        private void TestInvoke<T>(bool async) where T: AddArgCacheEntryProcessor, new()
        {
            var cache = async ? Cache().WithAsync().WrapAsync() : Cache();

            cache.Clear();

            const int key = 1;
            const int value = 3;
            const int arg = 5;

            cache.Put(key, value);

            // Existing entry
            Assert.AreEqual(value + arg, cache.Invoke(key, new T(), arg));
            Assert.AreEqual(value + arg, cache.Get(key));

            // Non-existing entry
            Assert.AreEqual(arg, cache.Invoke(10, new T {Exists = false}, arg));
            Assert.AreEqual(arg, cache.Get(10));

            // Remove entry
            Assert.AreEqual(0, cache.Invoke(key, new T {Remove = true}, arg));
            Assert.AreEqual(0, cache.Get(key));

            // Test exceptions
            AssertThrowsCacheEntryProcessorException(() => cache.Invoke(key, new T {ThrowErr = true}, arg));
            AssertThrowsCacheEntryProcessorException(
                () => cache.Invoke(key, new T {ThrowErrPortable = true}, arg));
            AssertThrowsCacheEntryProcessorException(
                () => cache.Invoke(key, new T { ThrowErrNonSerializable = true }, arg), "SerializationException");
        }

        private static void AssertThrowsCacheEntryProcessorException(Action action, string containsText = null)
        {
            try
            {
                action();

                Assert.Fail();
            }
            catch (Exception ex)
            {
                Assert.IsInstanceOf<CacheEntryProcessorException>(ex);

                if (string.IsNullOrEmpty(containsText))
                    Assert.AreEqual(ex.InnerException.Message, AddArgCacheEntryProcessor.ExceptionText);
                else
                    Assert.IsTrue(ex.ToString().Contains(containsText));
            }
        }

        [Test]
        public void TestInvokeAll()
        {
            TestInvokeAll(false);
        }

        [Test]
        public void TestInvokeAllAsync()
        {
            TestInvokeAll(true);
        }

        private void TestInvokeAll(bool async)
        {
            for (var i = 1; i < 10; i++)
            {
                TestInvokeAll<AddArgCacheEntryProcessor>(async, i);
                TestInvokeAll<PortableAddArgCacheEntryProcessor>(async, i);

                try
                {
                    TestInvokeAll<NonSerializableCacheEntryProcessor>(async, i);
                    Assert.Fail();
                }
                catch (SerializationException)
                {
                    // Expected
                }
            }
        }

        public void TestInvokeAll<T>(bool async, int entryCount) where T : AddArgCacheEntryProcessor, new()
        {
            var cache = async ? Cache().WithAsync().WrapAsync() : Cache();

            var entries = Enumerable.Range(1, entryCount).ToDictionary(x => x, x => x + 1);

            cache.PutAll(entries);

            const int arg = 5;

            // Existing entries
            var res = cache.InvokeAll(entries.Keys, new T(), arg);

            var results = res.OrderBy(x => x.Key).Select(x => x.Value.Result);
            var expectedResults = entries.OrderBy(x => x.Key).Select(x => x.Value + arg);
            
            Assert.IsTrue(results.SequenceEqual(expectedResults));

            var resultEntries = cache.GetAll(entries.Keys);

            Assert.IsTrue(resultEntries.All(x => x.Value == x.Key + 1 + arg));

            // Remove entries
            res = cache.InvokeAll(entries.Keys, new T {Remove = true}, arg);

            Assert.IsTrue(res.All(x => x.Value.Result == 0));
            Assert.AreEqual(0, cache.GetAll(entries.Keys).Count);

            // Non-existing entries
            res = cache.InvokeAll(entries.Keys, new T {Exists = false}, arg);

            Assert.IsTrue(res.All(x => x.Value.Result == arg));
            Assert.IsTrue(cache.GetAll(entries.Keys).All(x => x.Value == arg)); 

            // Test exceptions
            var errKey = entries.Keys.Reverse().Take(5).Last();

            TestInvokeAllException(cache, entries, new T { ThrowErr = true, ThrowOnKey = errKey }, arg, errKey);
            TestInvokeAllException(cache, entries, new T { ThrowErrPortable = true, ThrowOnKey = errKey }, 
                arg, errKey);
            TestInvokeAllException(cache, entries, new T { ThrowErrNonSerializable = true, ThrowOnKey = errKey }, 
                arg, errKey, "SerializationException");

        }

        private static void TestInvokeAllException<T>(ICache<int, int> cache, Dictionary<int, int> entries, 
            T processor, int arg, int errKey, string exceptionText = null) where T : AddArgCacheEntryProcessor
        {
            var res = cache.InvokeAll(entries.Keys, processor, arg);

            foreach (var procRes in res)
            {
                if (procRes.Key == errKey)
                    // ReSharper disable once AccessToForEachVariableInClosure
                    AssertThrowsCacheEntryProcessorException(() => { var x = procRes.Value.Result; }, exceptionText);
                else
                    Assert.Greater(procRes.Value.Result, 0);
            }
        }

        /// <summary>
        /// Test skip-store semantics.
        /// </summary>
        [Test]
        public void TestSkipStore()
        {
            CacheProxyImpl<int, int> cache = (CacheProxyImpl<int, int>)Cache();

            Assert.IsFalse(cache.SkipStore);

            // Ensure correct flag set.
            CacheProxyImpl<int, int> cacheSkipStore1 = (CacheProxyImpl<int, int>)cache.WithSkipStore();

            Assert.AreNotSame(cache, cacheSkipStore1);
            Assert.IsFalse(cache.SkipStore);
            Assert.IsTrue(cacheSkipStore1.SkipStore);

            // Ensure that the same instance is returned if flag is already set.
            CacheProxyImpl<int, int> cacheSkipStore2 = (CacheProxyImpl<int, int>)cacheSkipStore1.WithSkipStore();

            Assert.IsTrue(cacheSkipStore2.SkipStore);
            Assert.AreSame(cacheSkipStore1, cacheSkipStore2);

            // Ensure other flags are preserved.
            Assert.IsTrue(((CacheProxyImpl<int, int>)cache.WithKeepPortable<int, int>().WithSkipStore()).KeepPortable);
            Assert.IsTrue(cache.WithAsync().WithSkipStore().IsAsync);
        }

        [Test]
        public void TestCacheMetrics()
        {
            var cache = Cache();

            cache.Put(1, 1);

            var m = cache.GetMetrics();

            Assert.AreEqual(cache.Name, m.CacheName);

            Assert.AreEqual(cache.Size(), m.Size);
        }

        [Test]
        public void TestRebalance()
        {
            var cache = Cache();

            var fut = cache.Rebalance();

            Assert.IsNull(fut.Get());
        }

        [Test]
        public void TestCreate()
        {
            // Create a cache with random name
            var randomName = "template" + Guid.NewGuid();

            // Can't get non-existent cache with Cache method
            Assert.Throws<ArgumentException>(() => GetIgnite(0).Cache<int, int>(randomName));

            var cache = GetIgnite(0).CreateCache<int, int>(randomName);

            cache.Put(1, 10);

            Assert.AreEqual(10, cache.Get(1));

            // Can't create again
            Assert.Throws<IgniteException>(() => GetIgnite(0).CreateCache<int, int>(randomName));

            var cache0 = GetIgnite(0).Cache<int, int>(randomName);

            Assert.AreEqual(10, cache0.Get(1));
        }

        [Test]
        public void TestGetOrCreate()
        {
            // Create a cache with random name
            var randomName = "template" + Guid.NewGuid();

            // Can't get non-existent cache with Cache method
            Assert.Throws<ArgumentException>(() => GetIgnite(0).Cache<int, int>(randomName));

            var cache = GetIgnite(0).GetOrCreateCache<int, int>(randomName);

            cache.Put(1, 10);

            Assert.AreEqual(10, cache.Get(1));

            var cache0 = GetIgnite(0).GetOrCreateCache<int, int>(randomName);

            Assert.AreEqual(10, cache0.Get(1));

            var cache1 = GetIgnite(0).Cache<int, int>(randomName);

            Assert.AreEqual(10, cache1.Get(1));
        }

        private void TestKeepPortableFlag(bool async)
        {
            var cache0 = async ? Cache().WithAsync().WrapAsync() : Cache();

            var cache = cache0.WithKeepPortable<int, PortablePerson>();

            var portCache = cache0.WithKeepPortable<int, IPortableObject>();

            int cnt = 10;

            IList<int> keys = new List<int>();

            for (int i = 0; i < cnt; i++ ) {
                cache.Put(i, new PortablePerson("person-" + i, i));

                keys.Add(i);
            }

            IList<IPortableObject> objs = new List<IPortableObject>();

            for (int i = 0; i < cnt; i++)
            {
                var obj = portCache.Get(i);

                CheckPersonData(obj, "person-" + i, i);

                objs.Add(obj);
            }

            // Check objects weren't corrupted by subsequent cache operations.
            for (int i = 0; i < cnt; i++)
            {
                IPortableObject obj = objs[i];

                CheckPersonData(obj, "person-" + i, i);
            }

            // Check keepPortable for GetAll operation.
            var allObjs1 = portCache.GetAll(keys);

            var allObjs2 = portCache.GetAll(keys);

            for (int i = 0; i < cnt; i++)
            {
                CheckPersonData(allObjs1[i], "person-" + i, i);

                CheckPersonData(allObjs2[i], "person-" + i, i);
            }

            // Check keepPortable for Remove operation.
            var success0 = cache.Remove(0);
            var success1 = cache.Remove(1);

            Assert.AreEqual(true, success0);
            Assert.AreEqual(true, success1);
        }

        private void CheckPersonData(IPortableObject obj, string expName, int expAge)
        {
            Assert.AreEqual(expName, obj.Field<string>("name"));
            Assert.AreEqual(expAge, obj.Field<int>("age"));

            PortablePerson person = obj.Deserialize<PortablePerson>();

            Assert.AreEqual(expName, person.Name);
            Assert.AreEqual(expAge, person.Age);
        }

        protected static int PrimaryKeyForCache(ICache<int, int> cache)
        {
            return PrimaryKeysForCache(cache, 1, 0).First();
        }

        protected static int PrimaryKeyForCache(ICache<int, int> cache, int startFrom)
        {
            return PrimaryKeysForCache(cache, 1, startFrom).First();
        }

        protected static List<int> PrimaryKeysForCache(ICache<int, int> cache, int cnt)
        {
            return PrimaryKeysForCache(cache, cnt, 0).Take(cnt).ToList();
        }

        protected static IEnumerable<int> PrimaryKeysForCache(ICache<int, int> cache, int cnt, int startFrom)
        {
            IClusterNode node = cache.Ignite.Cluster.LocalNode;

            ICacheAffinity aff = cache.Ignite.Affinity(cache.Name);

            return Enumerable.Range(startFrom, int.MaxValue - startFrom).Where(x => aff.IsPrimary(node, x));
        }

        protected static int NearKeyForCache(ICache<int, int> cache)
        {
            IClusterNode node = cache.Ignite.Cluster.LocalNode;

            ICacheAffinity aff = cache.Ignite.Affinity(cache.Name);

            for (int i = 0; i < 100000; i++)
            {
                if (!aff.IsPrimaryOrBackup(node, i))
                    return i;
            }

            Assert.Fail("Failed to find near key.");

            return 0;
        }

        protected static string GetKeyAffinity(ICache<int, int> cache, int key)
        {
            if (cache.Ignite.Affinity(cache.Name).IsPrimary(cache.Ignite.Cluster.LocalNode, key))
                return "primary";

            if (cache.Ignite.Affinity(cache.Name).IsBackup(cache.Ignite.Cluster.LocalNode, key))
                return "backup";

            return "near";
        }

        protected virtual int CachePartitions()
        {
            return 1024;
        }

        protected virtual int Backups()
        {
            return 0;
        }

        protected virtual int GridCount()
        {
            return 1;
        }

        protected virtual string CacheName()
        {
            return "partitioned";
        }

        protected virtual bool NearEnabled()
        {
            return false;
        }

        protected virtual bool TxEnabled()
        {
            return true;
        }

        protected virtual bool LockingEnabled()
        {
            return TxEnabled();
        }

        protected virtual bool LocalCache()
        {
            return false;
        }

        protected virtual bool ReplicatedCache()
        {
            return true;
        }

        private static int PeekInt(ICache<int, int> cache, int key)
        {
            return cache.LocalPeek(key, CachePeekMode.Onheap);
        }
    }
}
