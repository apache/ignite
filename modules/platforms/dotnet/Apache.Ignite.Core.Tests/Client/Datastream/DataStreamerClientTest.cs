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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Client.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IDataStreamerClient{TK,TV}"/>.
    /// </summary>
    public class DataStreamerClientTest : ClientTestBase
    {
        /** */
        private const int GridCount = 3;
        
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
            : base(GridCount, enableSsl: false, enablePartitionAwareness: enablePartitionAwareness)
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

            var options = new DataStreamerClientOptions {AllowOverwrite = true};

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name, options))
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

            // Set server buffers to 1 so that server always flushes the data.
            var options = new DataStreamerClientOptions<int, int>
            {
                PerNodeBufferSize = 3
            };

            using (var streamer = Client.GetDataStreamer(
                cache.Name,
                options))
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
                Thread.Sleep(500);

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
                
                streamer.Flush();
                CheckArrayPoolLeak(streamer);
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
        public void TestSkipStore()
        {
            var serverCache = Ignition.GetIgnite().CreateCache<int, int>(new CacheConfiguration
            {
                Name = TestUtils.TestName,
                CacheStoreFactory = new BlockingCacheStore(),
                WriteThrough = true
            });

            var options = new DataStreamerClientOptions
            {
                SkipStore = true,
                AllowOverwrite = true // Required for cache store to be invoked.
            };

            BlockingCacheStore.Block();

            using (var streamer = Client.GetDataStreamer<int, int>(serverCache.Name, options))
            {
                streamer.Add(Enumerable.Range(1, 300).ToDictionary(x => x, x => -x));
            }

            Assert.AreEqual(300, serverCache.GetSize());
            Assert.AreEqual(-100, serverCache[100]);
        }

        [Test]
        public void TestExceedingPerNodeParallelOperationsBlocksAddMethod()
        {
            var serverCache = Ignition.GetIgnite().CreateCache<int, int>(new CacheConfiguration
            {
                Name = TestUtils.TestName,
                CacheStoreFactory = new BlockingCacheStore(),
                WriteThrough = true
            });

            var options = new DataStreamerClientOptions
            {
                PerNodeParallelOperations = 2,
                PerNodeBufferSize = 1,
                AllowOverwrite = true // Required for cache store to be invoked.
            };

            // Get primary keys for one of the nodes.
            var keys = TestUtils.GetPrimaryKeys(Ignition.GetIgnite(), serverCache.Name).Take(5).ToArray();

            using (var streamer = Client.GetDataStreamer<int, int>(serverCache.Name, options))
            {
                // Block writes and add data.
                BlockingCacheStore.Block();
                streamer.Add(keys[1], 1);
                streamer.Add(keys[2], 2);

                // ReSharper disable once AccessToDisposedClosure
                var task = Task.Factory.StartNew(() => streamer.Add(keys[3], 3));

                // Task is blocked because two streamer operations are already in progress.
                Assert.IsFalse(TestUtils.WaitForCondition(() => task.IsCompleted, 500));
                BlockingCacheStore.Unblock();
                TestUtils.WaitForTrueCondition(() => task.IsCompleted, 500);
            }

            Assert.AreEqual(3, serverCache.GetSize());
        }

        [Test]
        public void TestOptionsDefaults()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(CacheName))
            {
                var opts = streamer.Options;

                Assert.AreEqual(DataStreamerClientOptions.DefaultPerNodeBufferSize, opts.PerNodeBufferSize);
                Assert.AreEqual(DataStreamerClientOptions.DefaultPerNodeParallelOperations, opts.PerNodeParallelOperations);
                Assert.AreEqual(Environment.ProcessorCount * 4, opts.PerNodeParallelOperations);
            }
        }

        [Test]
        public void TestOptionsValidation()
        {
            var opts = new DataStreamerClientOptions();

            Assert.Throws<ArgumentException>(() => opts.PerNodeBufferSize = -1);
            Assert.Throws<ArgumentException>(() => opts.PerNodeParallelOperations = -1);
        }

        [Test]
        public void TestFlushThrowsWhenCacheDoesNotExist()
        {
            var streamer = Client.GetDataStreamer<int, int>("bad-cache-name");
            streamer.Add(1, 1);

            var ex = Assert.Throws<AggregateException>(() => streamer.Flush());
            StringAssert.StartsWith("Cache does not exist", ex.GetBaseException().Message);

            // Streamer is closed because of the flush failure.
            Assert.IsTrue(streamer.IsClosed);
        }

        [Test]
        public void TestFlushThrowsOnCacheStoreException()
        {
            var serverCache = Ignition.GetIgnite().CreateCache<int, int>(new CacheConfiguration
            {
                Name = TestUtils.TestName,
                CacheStoreFactory = new BlockingCacheStore(),
                WriteThrough = true
            });

            var options = new DataStreamerClientOptions
            {
                AllowOverwrite = true // Required for cache store to be invoked.
            };

            var streamer = Client.GetDataStreamer<int, int>(serverCache.Name, options);
            streamer.Remove(1);

            var ex = Assert.Throws<AggregateException>(() => streamer.Flush());
            StringAssert.Contains("Failed to finish operation (too many remaps)", ex.GetBaseException().Message);

            // Streamer is closed because of the flush failure.
            Assert.IsTrue(streamer.IsClosed);
        }

        [Test]
        public void TestDisposeThrowsWhenCacheDoesNotExist()
        {
            var streamer = Client.GetDataStreamer<int, int>("bad-cache-name");
            streamer.Add(1, 1);
            Assert.IsFalse(streamer.IsClosed);

            var ex = Assert.Throws<AggregateException>(() => streamer.Dispose());
            StringAssert.StartsWith("Cache does not exist", ex.GetBaseException().Message);
            Assert.IsTrue(streamer.IsClosed);
        }

        [Test]
        public void TestAllOperationThrowWhenStreamerIsClosed()
        {
            var options = new DataStreamerClientOptions
            {
                AllowOverwrite = true
            };
            
            var streamer = Client.GetDataStreamer<int, int>(CacheName, options);
            streamer.Close(true);

            Assert.Throws<ObjectDisposedException>(() => streamer.Add(1, 1));
            Assert.Throws<ObjectDisposedException>(() => streamer.Remove(1));
            Assert.Throws<ObjectDisposedException>(() => streamer.Remove(new[] {1, 2, 3}));
            Assert.Throws<ObjectDisposedException>(() => streamer.Flush());
            Assert.Throws<ObjectDisposedException>(() => streamer.FlushAsync());
        }

        [Test]
        public void TestMultipleCloseAndDisposeCallsAreAllowed()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(CacheName))
            {
                streamer.Add(1, 2);
                streamer.Close(cancel: false);

                streamer.Dispose();
                streamer.Close(true);
                streamer.Close(false);
                streamer.CloseAsync(true).Wait();
                streamer.CloseAsync(false).Wait();
            }
            
            Assert.AreEqual(2, GetCache<int>()[1]);
        }

        [Test]
        public void TestCloseCancelDiscardsBufferedData()
        {
            using (var streamer = Client.GetDataStreamer<int, int>(CacheName))
            {
                streamer.Add(1, 1);
                streamer.Add(2, 2);
                streamer.Close(cancel: true);
            }
            
            Assert.AreEqual(0, GetCache<int>().GetSize());
        }

        [Test]
        public void TestFlushAsyncContinuationDoesNotRunOnSocketReceiverThread()
        {
            var cache = GetClientCache<int>();

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                streamer.Add(1, 1);
                streamer.FlushAsync().ContinueWith(t =>
                {
                    var trace = new StackTrace().ToString();

                    StringAssert.DoesNotContain("ClientSocket", trace);
                }, TaskContinuationOptions.ExecuteSynchronously).Wait();
            }
        }

        [Test]
        public void TestStreamReceiver()
        {
            var cache = GetClientCache<int>();

            var options = new DataStreamerClientOptions<int, int>
            {
                Receiver = new StreamReceiverAddOne()
            };
            
            using (var streamer = Client.GetDataStreamer(cache.Name, options))
            {
                streamer.Add(1, 1);
            }
            
            Assert.AreEqual(2, cache[1]);
        }

        [Test]
        public void TestStreamReceiverKeepBinary()
        {
            var cache = GetClientCache<Test>().WithKeepBinary<int, IBinaryObject>();

            var options = new DataStreamerClientOptions<int, IBinaryObject>
            {
                Receiver = new StreamReceiverAddTwoKeepBinary(),
                ReceiverKeepBinary = true
            };
            
            using (var streamer = Client.GetDataStreamer(cache.Name, options))
            {
                streamer.Add(1, Client.GetBinary().ToBinary<IBinaryObject>(new Test {Val = 3}));
            }
            
            Assert.AreEqual(5, cache[1]);
        }

        [Test]
        public void TestStreamReceiverException()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestStreamReceiverDeserializationException()
        {
            Assert.Fail("TODO");
        }

#if NETCOREAPP

        [Test]
        public async Task TestStreamingAsyncAwait()
        {
            var cache = GetClientCache<int>();

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                streamer.Add(1, 1);
                await streamer.FlushAsync();
                Assert.AreEqual(1, await cache.GetAsync(1));

                streamer.Add(2, 2);
                await streamer.FlushAsync();
                Assert.AreEqual(2, await cache.GetAsync(2));

                streamer.Add(3, 3);
                await streamer.CloseAsync(false);
            }

            Assert.AreEqual(3, await cache.GetSizeAsync());
            Assert.AreEqual(3, await cache.GetAsync(3));
        }

#endif

        internal static void CheckArrayPoolLeak<TK, TV>(IDataStreamerClient<TK, TV> streamer, int maxDiff = 0)
        {
            var streamerImpl = (DataStreamerClient<TK, TV>) streamer;
                
            var poolStats = string.Format("Allocated={0}, Pooled={1}, MaxDiff={2}", 
                streamerImpl.ArraysAllocated, streamerImpl.ArraysPooled, maxDiff);

            Assert.LessOrEqual(streamerImpl.ArraysAllocated - streamerImpl.ArraysPooled, maxDiff, poolStats);
            Console.WriteLine("Array pool stats: " + poolStats);
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                Logger = new TestUtils.TestContextLogger()
            };
        }
        
        private class StreamReceiverAddOne : IStreamReceiver<int, int>
        {
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        private class StreamReceiverAddTwoKeepBinary : IStreamReceiver<int, IBinaryObject>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, IBinaryObject> cache, ICollection<ICacheEntry<int, IBinaryObject>> entries)
            {
                var binary = cache.Ignite.GetBinary();

                cache.PutAll(entries.ToDictionary(x => x.Key, x =>
                    binary.ToBinary<IBinaryObject>(new Test
                    {
                        Val = x.Value.Deserialize<Test>().Val + 2
                    })));
            }
        }

        private class Test
        {
            public int Val { get; set; }
        }

        private class BlockingCacheStore : CacheStoreAdapter<int, int>, IFactory<ICacheStore>
        {
            private static readonly ManualResetEventSlim Gate = new ManualResetEventSlim();

            public static void Block()
            {
                Gate.Reset();
            }

            public static void Unblock()
            {
                Gate.Set();
            }

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
