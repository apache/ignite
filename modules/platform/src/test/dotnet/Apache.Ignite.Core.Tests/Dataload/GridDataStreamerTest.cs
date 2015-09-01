/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Dataload
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;

    using GridGain;
    using GridGain.Cache;
    using GridGain.Client;
    using GridGain.Datastream;
    using GridGain.Impl;
    using GridGain.Portable;
    
    using NUnit.Framework;

    /// <summary>
    /// Data streamer tests.
    /// </summary>
    public class GridDataStreamerTest
    {
        /** Node name. */
        protected const string GRID_NAME = "grid";

        /** Cache name. */
        protected const string CACHE_NAME = "partitioned";

        /** Node. */
        private IGrid grid;

        /** Cache. */
        private ICache<int, int?> cache;

        /// <summary>
        /// Initialization routine.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void InitClient()
        {
            grid = GridFactory.Start(GetGridConfiguration(GRID_NAME));

            GridFactory.Start(GetGridConfiguration(GRID_NAME + "_1"));

            cache = grid.Cache<int, int?>(CACHE_NAME);
        }

        /// <summary>
        ///
        /// </summary>
        [TestFixtureTearDown]
        public virtual void StopGrids()
        {
            GridFactory.StopAll(true);
        }

        /// <summary>
        ///
        /// </summary>
        [SetUp]
        public virtual void BeforeTest()
        {
            Console.WriteLine("Test started: " + TestContext.CurrentContext.Test.Name);

            for (int i = 0; i < 100; i++)
                cache.Remove(i);
        }

        [TearDown]
        public void AfterTest()
        {
            GridTestUtils.AssertHandleRegistryIsEmpty(grid, 1000);
        }

        /// <summary>
        /// Test data streamer property configuration. Ensures that at least no exceptions are thrown.
        /// </summary>
        [Test]
        public void TestPropertyPropagation()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                ldr.AllowOverwrite = true;
                Assert.IsTrue(ldr.AllowOverwrite);
                ldr.AllowOverwrite = false;
                Assert.IsFalse(ldr.AllowOverwrite);

                ldr.SkipStore = true;
                Assert.IsTrue(ldr.SkipStore);
                ldr.SkipStore = false;
                Assert.IsFalse(ldr.SkipStore);

                ldr.PerNodeBufferSize = 1;
                Assert.AreEqual(1, ldr.PerNodeBufferSize);
                ldr.PerNodeBufferSize = 2;
                Assert.AreEqual(2, ldr.PerNodeBufferSize);

                ldr.PerNodeParallelOperations = 1;
                Assert.AreEqual(1, ldr.PerNodeParallelOperations);
                ldr.PerNodeParallelOperations = 2;
                Assert.AreEqual(2, ldr.PerNodeParallelOperations);
            }
        }

        /// <summary>
        /// Test data add/remove.
        /// </summary>
        [Test]        
        public void TestAddRemove()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                ldr.AllowOverwrite = true;

                // Additions.
                ldr.AddData(1, 1);
                ldr.Flush();                
                Assert.AreEqual(1, cache.Get(1));

                ldr.AddData(new KeyValuePair<int, int>(2, 2));
                ldr.Flush();
                Assert.AreEqual(2, cache.Get(2));

                ldr.AddData(new List<KeyValuePair<int, int>> { new KeyValuePair<int, int>(3, 3), new KeyValuePair<int, int>(4, 4) });
                ldr.Flush();
                Assert.AreEqual(3, cache.Get(3));
                Assert.AreEqual(4, cache.Get(4));

                // Removal.
                ldr.RemoveData(1);
                ldr.Flush();
                Assert.IsNull(cache.Get(1));

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
                    Assert.IsNull(cache.Get(i));

                for (int i = 5; i < 13; i++)
                    Assert.AreEqual(i, cache.Get(i));
            }
        }

        /// <summary>
        /// Test "tryFlush".
        /// </summary>
        [Test]
        public void TestTryFlush()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                var fut = ldr.AddData(1, 1);

                ldr.TryFlush();

                fut.Get();

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Test buffer size adjustments.
        /// </summary>
        [Test]
        public void TestBufferSize()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                var fut = ldr.AddData(1, 1);

                Thread.Sleep(100);

                Assert.IsFalse(fut.IsDone);

                ldr.PerNodeBufferSize = 2;

                ldr.AddData(2, 2);
                ldr.AddData(3, 3);
                ldr.AddData(4, 4).Get();
                fut.Get();

                Assert.AreEqual(1, cache.Get(1));
                Assert.AreEqual(2, cache.Get(2));
                Assert.AreEqual(3, cache.Get(3));
                Assert.AreEqual(4, cache.Get(4));

                ldr.AddData(new List<KeyValuePair<int, int>>
                {
                    new KeyValuePair<int, int>(5, 5), 
                    new KeyValuePair<int, int>(6, 6),
                    new KeyValuePair<int, int>(7, 7), 
                    new KeyValuePair<int, int>(8, 8)
                }).Get();

                Assert.AreEqual(5, cache.Get(5));
                Assert.AreEqual(6, cache.Get(6));
                Assert.AreEqual(7, cache.Get(7));
                Assert.AreEqual(8, cache.Get(8));
            }
        }

        /// <summary>
        /// Test close.
        /// </summary>
        [Test]
        public void TestClose()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                var fut = ldr.AddData(1, 1);

                ldr.Close(false);

                fut.Get();

                Assert.AreEqual(1, cache.Get(1));
            }
        }

        /// <summary>
        /// Test close with cancellation.
        /// </summary>
        [Test]
        public void TestCancel()
        {
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                var fut = ldr.AddData(1, 1);

                ldr.Close(true);

                fut.Get();

                Assert.IsNull(cache.Get(1));
            }
        }

        /// <summary>
        /// Tests that streamer gets collected when there are no references to it.
        /// </summary>
        [Test]
        public void TestFinalizer()
        {
            var streamer = grid.DataStreamer<int, int>(CACHE_NAME);
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
            using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                // Test auto flush turning on.
                var fut = ldr.AddData(1, 1);
                Thread.Sleep(100);
                Assert.IsFalse(fut.IsDone);
                ldr.AutoFlushFrequency = 1000;                
                fut.Get();

                // Test forced flush after frequency change.
                fut = ldr.AddData(2, 2);
                ldr.AutoFlushFrequency = Int64.MaxValue;                
                fut.Get();

                // Test another forced flush after frequency change.
                fut = ldr.AddData(3, 3);
                ldr.AutoFlushFrequency = 1000;
                fut.Get();

                // Test flush before stop.
                fut = ldr.AddData(4, 4);
                ldr.AutoFlushFrequency = 0;
                fut.Get();

                // Test flush after second turn on.
                fut = ldr.AddData(5, 5);
                ldr.AutoFlushFrequency = 1000;
                fut.Get();

                Assert.AreEqual(1, cache.Get(1));
                Assert.AreEqual(2, cache.Get(2));
                Assert.AreEqual(3, cache.Get(3));
                Assert.AreEqual(4, cache.Get(4));
                Assert.AreEqual(5, cache.Get(5));
            }
        }

        /// <summary>
        /// Test multithreaded behavior. 
        /// </summary>
        [Test]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestMultithreaded()
        {
            int entriesPerThread = 100000;
            int threadCnt = 8;

            for (int i = 0; i < 5; i++)
            {
                cache.Clear();

                Assert.AreEqual(0, cache.Size());

                Stopwatch watch = new Stopwatch();

                watch.Start();

                using (IDataStreamer<int, int> ldr = grid.DataStreamer<int, int>(CACHE_NAME))
                {
                    ldr.PerNodeBufferSize = 1024;

                    int ctr = 0;

                    GridTestUtils.RunMultiThreaded(() =>
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
            TestStreamReceiver(new StreamReceiverPortable());
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
            TestStreamReceiver(new StreamTransformer<int, int, int, int>(new EntryProcessorPortable()));
        }

        /// <summary>
        /// Tests specified receiver.
        /// </summary>
        private void TestStreamReceiver(IStreamReceiver<int, int> receiver)
        {
            using (var ldr = grid.DataStreamer<int, int>(CACHE_NAME))
            {
                ldr.AllowOverwrite = true;

                ldr.Receiver = new StreamReceiverPortable();

                ldr.Receiver = receiver;  // check double assignment

                Assert.AreEqual(ldr.Receiver, receiver);

                for (var i = 0; i < 100; i++)
                    ldr.AddData(i, i);

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, cache.Get(i));
            }
        }

        /// <summary>
        /// Tests the stream receiver in keepPortable mode.
        /// </summary>
        [Test]
        public void TestStreamReceiverKeepPortable()
        {
            // ReSharper disable once LocalVariableHidesMember
            var cache = grid.Cache<int, PortableEntry>(CACHE_NAME);

            using (var ldr0 = grid.DataStreamer<int, int>(CACHE_NAME))
            using (var ldr = ldr0.WithKeepPortable<int, IPortableObject>())
            {
                ldr.Receiver = new StreamReceiverKeepPortable();

                ldr.AllowOverwrite = true;

                for (var i = 0; i < 100; i++)
                    ldr.AddData(i, grid.Portables().ToPortable<IPortableObject>(new PortableEntry {Val = i}));

                ldr.Flush();

                for (var i = 0; i < 100; i++)
                    Assert.AreEqual(i + 1, cache.Get(i).Val);
            }
        }

        /// <summary>
        /// Gets the grid configuration.
        /// </summary>
        /// <param name="gridName">Grid name.</param>
        private static GridConfigurationEx GetGridConfiguration(string gridName)
        {
            return new GridConfigurationEx
            {
                GridName = gridName,
                SpringConfigUrl = "config\\native-client-test-cache.xml",
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new List<PortableTypeConfiguration>
                    {
                        new PortableTypeConfiguration(typeof (GridCacheTestKey)),
                        new PortableTypeConfiguration(typeof (TestReferenceObject)),
                        new PortableTypeConfiguration(typeof (StreamReceiverPortable)),
                        new PortableTypeConfiguration(typeof (EntryProcessorPortable)),
                        new PortableTypeConfiguration(typeof (PortableEntry))
                    }
                },
                JvmOptions = GridTestUtils.TestJavaOptions().Concat(new[]
                {
                    "-Xms3096m",
                    "-Xmx3096m",
                    "-XX:+UseParNewGC",
                    "-XX:+UseConcMarkSweepGC",
                    "-XX:+UseTLAB",
                    "-XX:NewSize=128m",
                    "-XX:MaxNewSize=128m",
                    "-XX:MaxTenuringThreshold=0",
                    "-XX:SurvivorRatio=1024",
                    "-XX:+UseCMSInitiatingOccupancyOnly",
                    "-XX:CMSInitiatingOccupancyFraction=60"
                }).ToArray()
            };
        }

        /// <summary>
        /// Test portable receiver.
        /// </summary>
        private class StreamReceiverPortable : IStreamReceiver<int, int>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, int> cache, ICollection<ICacheEntry<int, int>> entries)
            {
                cache.PutAll(entries.ToDictionary(x => x.Key, x => x.Value + 1));
            }
        }

        /// <summary>
        /// Test portable receiver.
        /// </summary>
        [Serializable]
        private class StreamReceiverKeepPortable : IStreamReceiver<int, IPortableObject>
        {
            /** <inheritdoc /> */
            public void Receive(ICache<int, IPortableObject> cache, ICollection<ICacheEntry<int, IPortableObject>> entries)
            {
                var portables = cache.Grid.Portables();

                cache.PutAll(entries.ToDictionary(x => x.Key, x =>
                    portables.ToPortable<IPortableObject>(new PortableEntry
                    {
                        Val = x.Value.Deserialize<PortableEntry>().Val + 1
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
        private class EntryProcessorPortable : ICacheEntryProcessor<int, int, int, int>, IPortableMarshalAware
        {
            /** <inheritdoc /> */
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value = entry.Key + 1;
                
                return 0;
            }

            /** <inheritdoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                // No-op.
            }

            /** <inheritdoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                // No-op.
            }
        }

        /// <summary>
        /// Portablecache entry.
        /// </summary>
        private class PortableEntry
        {
            public int Val { get; set; }
        }
    }
}
