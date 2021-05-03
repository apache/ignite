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
    using System.Threading.Tasks;
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
        public void TestStreamLongList()
        {
            var cache = GetClientCache<int>();
            const int count = 50000;

            // TODO: Why is this 10 times slower than the benchmark?
            // Something is wrong with AllowOverwrite or cache mode?
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
        public void TestStreamMultithreaded()
        {
            var cache = GetClientCache<int>();
            var keys = Enumerable.Range(1, 150000).ToArray();

            using (var streamer = Client.GetDataStreamer<int, int>(cache.Name))
            {
                // ReSharper disable once AccessToDisposedClosure
                Parallel.ForEach(keys, k => streamer.Add(k, k + 2));
            }

            Assert.AreEqual(keys.Length, cache.GetSize());
            Assert.AreEqual(4, cache[2]);
            Assert.AreEqual(22, cache[20]);
        }

        [Test]
        public void TestCloseWithNoDataAdded()
        {
            var cache = GetClientCache<int>();

            using (Client.GetDataStreamer<int, int>(cache.Name))
            {
                // No-op.
            }

            Assert.AreEqual(0, cache.GetSize());
        }
    }
}
