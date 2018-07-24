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

namespace Apache.Ignite.Core.Tests.Dataload
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Data streamer tests.
    /// </summary>
    public sealed class DataStreamerTest
    {
        /** Cache name. */
        private const string CacheName = "partitioned";

        /** Node. */
        private IIgnite _grid;

        /** Node 2. */
        private IIgnite _grid2;

        /** Cache. */
        private ICache<int, int?> _cache;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public void InitClient()
        {
            _grid = Ignition.Start(TestUtils.GetTestConfiguration());

            _grid2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                IgniteInstanceName = "grid1"
            });

            _cache = _grid.CreateCache<int, int?>(CacheName);
        }

        /// <summary>
        /// Fixture teardown.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        ///
        /// </summary>
        [SetUp]
        public void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);

            for (int i = 0; i < 100; i++)
                _cache.Remove(i);
        }

        [TearDown]
        public void AfterTest()
        {
            TestUtils.AssertHandleRegistryIsEmpty(1000, _grid);
        }

        /// <summary>
        /// Test data streamer property configuration. Ensures that at least no exceptions are thrown.
        /// </summary>
        [Test]
        public void TestPropertyPropagation()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                Assert.AreEqual(CacheName, ldr.CacheName);
                Assert.AreEqual(0, ldr.AutoFlushFrequency);

                Assert.IsFalse(ldr.AllowOverwrite);
                ldr.AllowOverwrite = true;
                Assert.IsTrue(ldr.AllowOverwrite);
                ldr.AllowOverwrite = false;
                Assert.IsFalse(ldr.AllowOverwrite);

                Assert.IsFalse(ldr.SkipStore);
                ldr.SkipStore = true;
                Assert.IsTrue(ldr.SkipStore);
                ldr.SkipStore = false;
                Assert.IsFalse(ldr.SkipStore);

                Assert.AreEqual(DataStreamerDefaults.DefaultPerNodeBufferSize, ldr.PerNodeBufferSize);
                ldr.PerNodeBufferSize = 1;
                Assert.AreEqual(1, ldr.PerNodeBufferSize);
                ldr.PerNodeBufferSize = 2;
                Assert.AreEqual(2, ldr.PerNodeBufferSize);
                
                Assert.AreEqual(DataStreamerDefaults.DefaultPerThreadBufferSize, ldr.PerThreadBufferSize);
                ldr.PerThreadBufferSize = 1;
                Assert.AreEqual(1, ldr.PerThreadBufferSize);
                ldr.PerThreadBufferSize = 2;
                Assert.AreEqual(2, ldr.PerThreadBufferSize);

                Assert.AreEqual(0, ldr.PerNodeParallelOperations);
                var ops = DataStreamerDefaults.DefaultParallelOperationsMultiplier *
                          IgniteConfiguration.DefaultThreadPoolSize;
                ldr.PerNodeParallelOperations = ops;
                Assert.AreEqual(ops, ldr.PerNodeParallelOperations);
                ldr.PerNodeParallelOperations = 2;
                Assert.AreEqual(2, ldr.PerNodeParallelOperations);

                Assert.AreEqual(DataStreamerDefaults.DefaultTimeout, ldr.Timeout);
                ldr.Timeout = TimeSpan.MaxValue;
                Assert.AreEqual(TimeSpan.MaxValue, ldr.Timeout);
                ldr.Timeout = TimeSpan.FromSeconds(1.5);
                Assert.AreEqual(1.5, ldr.Timeout.TotalSeconds);
            }
        }

        /// <summary>
        /// Test data add/remove.
        /// </summary>
        [Test]        
        public void TestAddRemove()
        {
            IDataStreamer<int, int> ldr;

            using (ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                Assert.IsFalse(ldr.Task.IsCompleted);

                ldr.AllowOverwrite = true;

                // Additions.
                var task = ldr.AddData(1, 1);
                ldr.Flush();                
                Assert.AreEqual(1, _cache.Get(1));
                Assert.IsTrue(task.IsCompleted);
                Assert.IsFalse(ldr.Task.IsCompleted);

                task = ldr.AddData(new KeyValuePair<int, int>(2, 2));
                ldr.Flush();
                Assert.AreEqual(2, _cache.Get(2));
                Assert.IsTrue(task.IsCompleted);

                task = ldr.AddData(new [] { new KeyValuePair<int, int>(3, 3), new KeyValuePair<int, int>(4, 4) });
                ldr.Flush();
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.IsTrue(task.IsCompleted);

                // Removal.
                task = ldr.RemoveData(1);
                ldr.Flush();
                Assert.IsFalse(_cache.ContainsKey(1));
                Assert.IsTrue(task.IsCompleted);

                // Mixed.
                ldr.AddData(5, 5);                
                ldr.RemoveData(2);
                ldr.AddData(new KeyValuePair<int, int>(7, 7));
                ldr.AddData(6, 6);
                ldr.RemoveData(4);
                ldr.AddData(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(9, 9), new KeyValuePair<int, int>(10, 10) });
                ldr.AddData(new KeyValuePair<int, int>(8, 8));
                ldr.RemoveData(3);
                ldr.AddData(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(11, 11), new KeyValuePair<int, int>(12, 12) });

                ldr.Flush();

                for (int i = 2; i < 5; i++)
                    Assert.IsFalse(_cache.ContainsKey(i));

                for (int i = 5; i < 13; i++)
                    Assert.AreEqual(i, _cache.Get(i));
            }

            Assert.IsTrue(ldr.Task.IsCompleted);
        }

        /// <summary>
        /// Tests object graphs with loops.
        /// </summary>
        [Test]
        public void TestObjectGraphs()
        {
            var obj1 = new Container();
            var obj2 = new Container();
            var obj3 = new Container();
            var obj4 = new Container();

            obj1.Inner = obj2;
            obj2.Inner = obj1;
            obj3.Inner = obj1;
            obj4.Inner = new Container();

            using (var ldr = _grid.GetDataStreamer<int, Container>(CacheName))
            {
                ldr.AllowOverwrite = true;

                ldr.AddData(1, obj1);
                ldr.AddData(2, obj2);
                ldr.AddData(3, obj3);
                ldr.AddData(4, obj4);
            }

            var cache = _grid.GetCache<int, Container>(CacheName);

            var res = cache[1];
            Assert.AreEqual(res, res.Inner.Inner);

            Assert.IsNotNull(cache[2].Inner);
            Assert.IsNotNull(cache[2].Inner.Inner);
            Assert.IsNotNull(cache[3].Inner);
            Assert.IsNotNull(cache[3].Inner.Inner);
            
            Assert.IsNotNull(cache[4].Inner);
            Assert.IsNull(cache[4].Inner.Inner);
        }

        /// <summary>
        /// Test "tryFlush".
        /// </summary>
        [Test]
        public void TestTryFlush()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.AddData(1, 1);

                ldr.TryFlush();

                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
            }
        }

        /// <summary>
        /// Test buffer size adjustments.
        /// </summary>
        [Test]
        public void TestBufferSize()
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                const int timeout = 5000;

                var part1 = GetPrimaryPartitionKeys(_grid, 4);
                var part2 = GetPrimaryPartitionKeys(_grid2, 4);

                var task = ldr.AddData(part1[0], part1[0]);

                Thread.Sleep(100);

                Assert.IsFalse(task.IsCompleted);

                ldr.PerNodeBufferSize = 2;
                ldr.PerThreadBufferSize = 1;

                ldr.AddData(part2[0], part2[0]);
                ldr.AddData(part1[1], part1[1]);
                Assert.IsTrue(ldr.AddData(part2[1], part2[1]).Wait(timeout));
                Assert.IsTrue(task.Wait(timeout));

                Assert.AreEqual(part1[0], _cache.Get(part1[0]));
                Assert.AreEqual(part1[1], _cache.Get(part1[1]));
                Assert.AreEqual(part2[0], _cache.Get(part2[0]));
                Assert.AreEqual(part2[1], _cache.Get(part2[1]));

                Assert.IsTrue(ldr.AddData(new[]
                {
                    new KeyValuePair<int, int>(part1[2], part1[2]),
                    new KeyValuePair<int, int>(part1[3], part1[3]),
                    new KeyValuePair<int, int>(part2[2], part2[2]),
                    new KeyValuePair<int, int>(part2[3], part2[3])
                }).Wait(timeout));

                Assert.AreEqual(part1[2], _cache.Get(part1[2]));
                Assert.AreEqual(part1[3], _cache.Get(part1[3]));
                Assert.AreEqual(part2[2], _cache.Get(part2[2]));
                Assert.AreEqual(part2[3], _cache.Get(part2[3]));
            }
        }

        /// <summary>
        /// Gets the primary partition keys.
        /// </summary>
        private static int[] GetPrimaryPartitionKeys(IIgnite ignite, int count)
        {
            var affinity = ignite.GetAffinity(CacheName);
            
            var localNode = ignite.GetCluster().GetLocalNode();

            var part = affinity.GetPrimaryPartitions(localNode).First();

            return Enumerable.Range(0, int.MaxValue)
                .Where(k => affinity.GetPartition(k) == part)
                .Take(count)
                .ToArray();
        }

        /// <summary>
        /// Test close.
        /// </summary>
        [Test]
        public void TestClose()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.AddData(1, 1);

                ldr.Close(false);

                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
            }
        }

        /// <summary>
        /// Test close with cancellation.
        /// </summary>
        [Test]
        public void TestCancel()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                var fut = ldr.AddData(1, 1);

                ldr.Close(true);

                fut.Wait();

                Assert.IsFalse(_cache.ContainsKey(1));
            }
        }

        /// <summary>
        /// Tests that streamer gets collected when there are no references to it.
        /// </summary>
        [Test]
        public void TestFinalizer()
        {
            Assert.Fail("https://issues.apache.org/jira/browse/IGNITE-8731");

            var streamer = _grid.GetDataStreamer<int, int>(CacheName);
            var streamerRef = new WeakReference(streamer);

            Assert.IsNotNull(streamerRef.Target);

            // ReSharper disable once RedundantAssignment
            streamer = null;

            GC.Collect();
            GC.WaitForPendingFinalizers();

            Assert.IsNull(streamerRef.Target);
        }

        /// <summary>
        /// Test auto-flush feature.
        /// </summary>
        [Test]
        public void TestAutoFlush()
        {
            using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                // Test auto flush turning on.
                var fut = ldr.AddData(1, 1);
                Thread.Sleep(100);
                Assert.IsFalse(fut.IsCompleted);
                ldr.AutoFlushFrequency = 1000;                
                fut.Wait();

                // Test forced flush after frequency change.
                fut = ldr.AddData(2, 2);
                ldr.AutoFlushFrequency = long.MaxValue;
                fut.Wait();

                // Test another forced flush after frequency change.
                fut = ldr.AddData(3, 3);
                ldr.AutoFlushFrequency = 1000;
                fut.Wait();

                // Test flush before stop.
                fut = ldr.AddData(4, 4);
                ldr.AutoFlushFrequency = 0;
                fut.Wait();

                // Test flush after second turn on.
                fut = ldr.AddData(5, 5);
                ldr.AutoFlushFrequency = 1000;
                fut.Wait();

                Assert.AreEqual(1, _cache.Get(1));
                Assert.AreEqual(2, _cache.Get(2));
                Assert.AreEqual(3, _cache.Get(3));
                Assert.AreEqual(4, _cache.Get(4));
                Assert.AreEqual(5, _cache.Get(5));
            }
        }

        /// <summary>
        /// Test multithreaded behavior. 
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestMultithreaded()
        {
            int entriesPerThread = 100000;
            int threadCnt = 8;

            for (int i = 0; i < 5; i++)
            {
                _cache.Clear();

                Assert.AreEqual(0, _cache.GetSize());

                Stopwatch watch = new Stopwatch();

                watch.Start();

                using (IDataStreamer<int, int> ldr = _grid.GetDataStreamer<int, int>(CacheName))
                {
                    ldr.PerNodeBufferSize = 1024;

                    int ctr = 0;

                    TestUtils.RunMultiThreaded(() =>
                    {
                        int threadIdx = Interlocked.Increment(ref ctr);

                        int startIdx = (threadIdx - 1) * entriesPerThread;
                        int endIdx = startIdx + entriesPerThread;

                        for (int j = startIdx; j < endIdx; j++)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            ldr.AddData(j, j);

                            if (j % 100000 == 0)
                                Console.WriteLine("Put [thread=" + threadIdx + ", cnt=" + j  + ']');
                        }
                    }, threadCnt);
                }

                Console.WriteLine("Iteration " + i + ": " + watch.ElapsedMilliseconds);

                watch.Reset();

                for (int j = 0; j < threadCnt * entriesPerThread; j++)
                    Assert.AreEqual(j, j);
            }
        }

        /// <summary>
        /// Tests custom receiver.
        /// </summary>
        [Test]
        public void TestStreamReceiver()
        {
            TestStreamReceiver(new StreamReceiverBinarizable());
            TestStreamReceiver(new StreamReceiverSerializable());
        }

        /// <summary>
        /// Tests StreamVisitor.
        /// </summary>
        [Test]
        public void TestStreamVisitor()
        {
            TestStreamReceiver(new StreamVisitor<int, int>((c, e) => c.Put(e.Key, e.Value + 1)));
        }

        /// <summary>
        /// Tests StreamTransformer.
        /// </summary>
        [Test]
        public void TestStreamTransformer()
        {
            TestStreamReceiver(new StreamTransformer<int, int, int, int>(new EntryProcessorSerializable()));
            TestStreamReceiver(new StreamTransformer<int, int, int, int>(new EntryProcessorBinarizable()));
        }

        /// <summary>
        /// Tests specified receiver.
        /// </summary>
        private void TestStreamReceiver(IStreamReceiver<int, int> receiver)
        {
            using (var ldr = _grid.GetDataStreamer<int, int>(CacheName))
            {
                ldr.AllowOverwrite = true;

                ldr.Receiver = new StreamReceiverBinarizable();

                ldr.Receiver = receiver;  // check double assignment

                Assert.AreEqual(ldr.Receiver, receiver);

                for (var i = 0; i < 100; i++)
                    ldr.AddData(i, i);

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, _cache.Get(i));
            }
        }

        /// <summary>
        /// Tests the stream receiver in keepBinary mode.
        /// </summary>
        [Test]
        public void TestStreamReceiverKeepBinary()
        {
            // ReSharper disable once LocalVariableHidesMember
            var cache = _grid.GetCache<int, BinarizableEntry>(CacheName);

            using (var ldr0 = _grid.GetDataStreamer<int, int>(CacheName))
            using (var ldr = ldr0.WithKeepBinary<int, IBinaryObject>())
            {
                ldr.Receiver = new StreamReceiverKeepBinary();

                ldr.AllowOverwrite = true;

                for (var i = 0; i < 100; i++)
                    ldr.AddData(i, _grid.GetBinary().ToBinary<IBinaryObject>(new BinarizableEntry {Val = i}));

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, cache.Get(i).Val);

                // Repeating WithKeepBinary call: valid args.
                Assert.AreSame(ldr, ldr.WithKeepBinary<int, IBinaryObject>());

                // Invalid type args.
                var ex = Assert.Throws<InvalidOperationException>(() => ldr.WithKeepBinary<string, IBinaryObject>());

                Assert.AreEqual(
                    "Can't change type of binary streamer. WithKeepBinary has been called on an instance of " +
                    "binary streamer with incompatible generic arguments.", ex.Message);
            }
        }

        /// <summary>
        /// Test binarizable receiver.
        /// </summary>
        private class StreamReceiverBinarizable : IStreamReceiver<int, int>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        /// <summary>
        /// Test binary receiver.
        /// </summary>
        [Serializable]
        private class StreamReceiverKeepBinary : IStreamReceiver<int, IBinaryObject>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, IBinaryObject> cache, ICollection<ICacheEntry<int, IBinaryObject>> entries)
            {
                var binary = cache.Ignite.GetBinary();

                cache.PutAll(entries.ToDictionary(x => x.Key, x =>
                    binary.ToBinary<IBinaryObject>(new BinarizableEntry
                    {
                        Val = x.Value.Deserialize<BinarizableEntry>().Val + 1
                    })));
            }
        }

        /// <summary>
        /// Test serializable receiver.
        /// </summary>
        [Serializable]
        private class StreamReceiverSerializable : IStreamReceiver<int, int>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        /// <summary>
        /// Test entry processor.
        /// </summary>
        [Serializable]
        private class EntryProcessorSerializable : ICacheEntryProcessor<int, int, int, int>
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value = entry.Key + 1;
                
                return 0;
            }
        }

        /// <summary>
        /// Test entry processor.
        /// </summary>
        private class EntryProcessorBinarizable : ICacheEntryProcessor<int, int, int, int>, IBinarizable
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value = entry.Key + 1;
                
                return 0;
            }

            /** <inheritdoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Binarizable entry.
        /// </summary>
        private class BinarizableEntry
        {
            public int Val { get; set; }
        }

        /// <summary>
        /// Container class.
        /// </summary>
        private class Container
        {
            public Container Inner;
        }

    }
}
