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
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Datastream;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IDataStreamerClient{TK,TV}"/>.
    /// TODO:
    /// * Test for small buffer size
    /// * Test for mismatching buffer size
    /// *
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

            using (var streamer = Client.GetDataStreamer(
                cache.Name,
                new DataStreamerClientOptions<int, int> {AllowOverwrite = true}))
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
            const int count = 150000;
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
            Assert.Fail("TODO: Test that Add method blocks when too many parallel operations are active");
        }
    }
}
