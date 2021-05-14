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

namespace Apache.Ignite.Core.Tests.Client.Datastream
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IDataStreamerClient{TK,TV}"/>.
    /// </summary>
    public class DataStreamerClientTest : ClientTestBase
    {
        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientTest"/>.
        /// </summary>
        public DataStreamerClientTest()
            : this(false)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientTest"/>.
        /// </summary>
        public DataStreamerClientTest(bool enablePartitionAwareness)
            : base(gridCount: 3, enableSsl: false, enablePartitionAwareness: enablePartitionAwareness)
        {
            // No-op.
        }

        [Test]
        public void TestBasicStreaming()
        {
            var cache = GetClientCache<string>();

            using (var streamer = Client.GetDataStreamer<int, string>(cache.Name))
            {
                streamer.Add(1, "1");
                streamer.Add(2, "2");
            }

            Assert.AreEqual("1", cache[1]);
            Assert.AreEqual("2", cache[2]);
        }

        [Test]
        public void TestAddRemoveOverwrite()
        {
            var cache = GetClientCache<int>();
            cache.PutAll(Enumerable.Range(1, 10).ToDictionary(x => x, x => x + 1));

            using (var streamer = Client.GetDataStreamer<int, int>(
                cache.Name,
                new DataStreamerClientOptions {AllowOverwrite = true}))
            {
                streamer.Add(1, 11);
                streamer.Add(20, 20);
                streamer.Remove(2);
                streamer.Remove(new[] {4, 6, 7, 8, 9, 10});
            }

            var resKeys = cache.GetAll(Enumerable.Range(1, 30))
                .Select(x => x.Key)
                .OrderBy(x => x)
                .ToArray();

            Assert.AreEqual(11, cache.Get(1));
            Assert.AreEqual(20, cache.Get(20));
            Assert.AreEqual(4, cache.GetSize());
            Assert.AreEqual(new[] {1, 3, 5, 20}, resKeys);
        }

        [Test]
        public void TestAutoFlushOnFullBuffer()
        {
            var cache = GetClientCache<string>();
            var keys = TestUtils.GetPrimaryKeys(GetIgnite(), cache.Name).Take(10).ToArray();

            using (var streamer = Client.GetDataStreamer(
                cache.Name,
                new DataStreamerClientOptions<int, int>{ClientPerNodeBufferSize = 3}))
            {
                streamer.Add(keys[1], 1);
                Assert.AreEqual(0, cache.GetSize());

                streamer.Add(keys[2], 2);
                Assert.AreEqual(0, cache.GetSize());

                streamer.Add(keys[3], 3);
                TestUtils.WaitForTrueCondition(() => cache.GetSize() == 3);
            }
        }

        [Test]
        public void TestManualFlush()
        {
            var cache = GetClientCache<int>();

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                streamer.Add(1, 1);
                streamer.Add(2, 2);

                streamer.Flush();

                streamer.Add(3, 3);

                Assert.AreEqual(2, cache.GetSize());
                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(2, cache[2]);

                streamer.Flush();

                Assert.AreEqual(3, cache.GetSize());
                Assert.AreEqual(3, cache[3]);
            }
        }

        [Test]
        public void TestRemoveNoAllowOverwriteThrows()
        {
            var cache = GetClientCache<string>();

            using (var streamer = Client.GetDataStreamer<int, string>(cache.Name))
            {
                var ex = Assert.Throws<IgniteClientException>(() => streamer.Remove(1));

                Assert.AreEqual("DataStreamer can't remove data when AllowOverwrite is false.", ex.Message);
            }
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestStreamLongList()
        {
            var cache = GetClientCache<int>();
            const int count = 50000;

            // Note: this test is ~10 times slower than ThinClientDataStreamerBenchmark
            // because of the logging in the base class.
            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                for (var k = 0; k < count; k++)
                {
                    streamer.Add(k, -k);
                }
            }

            Assert.AreEqual(count, cache.GetSize());
            Assert.AreEqual(-2, cache[2]);
            Assert.AreEqual(-200, cache[200]);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestStreamMultithreaded()
        {
            var cache = GetClientCache<int>();
            const int count = 250000;
            int id = 0;

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                TestUtils.RunMultiThreaded(() =>
                {
                    while (true)
                    {
                        var key = Interlocked.Increment(ref id);

                        if (key > count)
                        {
                            break;
                        }

                        // ReSharper disable once AccessToDisposedClosure
                        streamer.Add(key, key + 2);
                    }
                }, 8);
            }

            Assert.AreEqual(count, cache.GetSize());
            Assert.AreEqual(4, cache[2]);
            Assert.AreEqual(22, cache[20]);
        }

        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestStreamParallelFor()
        {
            var cache = GetClientCache<int>();
            const int count = 250000;

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                // ReSharper disable once AccessToDisposedClosure
                Parallel.For(0, count, i => streamer.Add(i, i + 2));
            }

            var size = cache.GetSize();
            if (size != count)
            {
                Thread.Sleep(3000);
                var newSize = cache.GetSize();

                Assert.AreEqual(size, count, "After 3 seconds: " + newSize);
            }

            Assert.AreEqual(4, cache[2]);
            Assert.AreEqual(22, cache[20]);
        }

        [Test]
        public void TestDisposeWithNoDataAdded()
        {
            var cache = GetClientCache<int>();

            using (Client.GetDataStreamer<int, int>(cache.Name))
            {
                // No-op.
            }

            Assert.AreEqual(0, cache.GetSize());
        }

        [Test]
        public void TestCloseWithNoDataAdded([Values(true, false)] bool cancel)
        {
            var cache = GetClientCache<int>();

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                streamer.Close(cancel);
            }

            Assert.AreEqual(0, cache.GetSize());
        }

        [Test]
        public void TestBackPressure()
        {
            var serverCache = Ignition.GetIgnite().CreateCache<int, int>(new CacheConfiguration
            {
                Name = TestUtils.TestName,
                CacheStoreFactory = new BlockingCacheStore(),
                WriteThrough = true
            });

            var options = new DataStreamerClientOptions
            {
                ClientPerNodeParallelOperations = 2,
                ClientPerNodeBufferSize = 1,
                AllowOverwrite = true // Required for cache store to be invoked.
            };

            BlockingCacheStore.Gate.Reset();

            using (var streamer = Client.GetDataStreamer<int, int>(serverCache.Name, options))
            {
                streamer.Add(1, 1);
                streamer.Add(2, 2);

                // ReSharper disable once AccessToDisposedClosure
                var task = Task.Factory.StartNew(() => streamer.Add(3, 3));

                // Task is blocked because two streamer operations are already in progress.
                Assert.IsFalse(TestUtils.WaitForCondition(() => task.IsCompleted, 500));

                BlockingCacheStore.Gate.Set();
                TestUtils.WaitForTrueCondition(() => task.IsCompleted, 500);
            }

            Assert.AreEqual(3, serverCache.GetSize());
        }

        [Test]
        public void TestOptionsValidation()
        {
            var opts = new DataStreamerClientOptions();

            Assert.Throws<ArgumentException>(() => opts.ClientPerNodeBufferSize = -1);
            Assert.Throws<ArgumentException>(() => opts.ClientPerNodeParallelOperations = -1);
            Assert.Throws<ArgumentException>(() => opts.ServerPerNodeBufferSize = -1);
            Assert.Throws<ArgumentException>(() => opts.ServerPerThreadBufferSize = -1);
        }

        [Test]
        public void TestFlushThrowsWhenCacheDoesNotExist()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(Guid.NewGuid().ToString()))
            {
                streamer.Add(1, 1);

                var ex = Assert.Throws<AggregateException>(() => streamer.Flush());
                var baseEx = ex.GetBaseException();
                StringAssert.StartsWith("Cache does not exist", baseEx.Message);

                // Close and Dispose do not throw.
                streamer.Flush();
                streamer.Close(cancel: false);
            }
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                Logger = new TestUtils.TestContextLogger()
            };
        }

        private class BlockingCacheStore : CacheStoreAdapter<int, int>, IFactory<ICacheStore>
        {
            public static ManualResetEventSlim Gate = new ManualResetEventSlim();

            public override int Load(int key)
            {
                throw new NotImplementedException();
            }

            public override void Write(int key, int val)
            {
                Gate.Wait();
            }

            public override void Delete(int key)
            {
                throw new NotImplementedException();
            }

            public ICacheStore CreateInstance()
            {
                return new BlockingCacheStore();
            }
        }
    }
}
