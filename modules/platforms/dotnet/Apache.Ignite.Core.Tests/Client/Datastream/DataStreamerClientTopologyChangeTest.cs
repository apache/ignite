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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client data streamer with topology changes.
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class DataStreamerClientTopologyChangeTest
    {
        /** */
        private readonly bool _enablePartitionAwareness;

        public DataStreamerClientTopologyChangeTest() : this(false)
        {
            // No-op.
        }

        public DataStreamerClientTopologyChangeTest(bool enablePartitionAwareness)
        {
            _enablePartitionAwareness = enablePartitionAwareness;
        }

        [Test]
        public void TestStreamerDoesNotLoseDataOnFlushWhenNewNodeEntersAndOriginalNodeLeaves()
        {
            var server = StartServer();
            var client = StartClient();

            var cache = CreateCache(client);

            using (var streamer = client.GetDataStreamer<int, int>(cache.Name))
            {
                // Add to the buffer for the initial server.
                streamer.Add(1, 1);

                // Start new server, stop old one.
                StartServer();
                server.Dispose();
                
                streamer.Add(2, 2);
                streamer.Flush();
                
                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(2, cache[2]);

                streamer.Add(3, 3);
                streamer.Flush();
                
                Assert.AreEqual(3, cache[3]);
            }
        }

        [Test]
        public void TestStreamerDoesNotLoseDataOnDisposeWhenNewNodeEntersAndOriginalNodeLeaves()
        {
            var server = StartServer();
            var client = StartClient();

            var cache = CreateCache(client);

            using (var streamer = client.GetDataStreamer<int, int>(cache.Name))
            {
                streamer.Add(1, 1);

                StartServer();
                server.Dispose();
                
                streamer.Add(2, 2);
            }
            
            Assert.AreEqual(1, cache[1]);
            Assert.AreEqual(2, cache[2]);
        }

        [Test]
        public void TestStreamerDoesNotLoseDataOnRandomTopologyChanges()
        {
            StartServer();
            var client = StartClient();

            var id = 0;
            var cache = CreateCache(client);

            using (var streamer = client.GetDataStreamer<int, int>(cache.Name))
            {
                var cancel = false;
                
                var adderTask = Task.Factory.StartNew(() =>
                {
                    // ReSharper disable once AccessToModifiedClosure
                    while (!cancel)
                    {
                        id++;
                        
                        // ReSharper disable once AccessToDisposedClosure
                        streamer.Add(id, id);

                        if (id % 2000 == 0)
                        {
                            // Sleep once in a while to reduce streamed data size.
                            Thread.Sleep(100);
                        }
                    }
                });
                
                var nodes = new Stack<IIgnite>();
                
                for (int i = 0; i < 20; i++)
                {
                    if (nodes.Count == 0 || TestUtils.Random.Next(2) == 0)
                    {
                        nodes.Push(StartServer());
                    }
                    else
                    {
                        nodes.Pop().Dispose();
                    }
                }

                cancel = true;
                adderTask.Wait();
            }
            
            Assert.AreEqual(1, cache[1]);
            Assert.AreEqual(id, cache.GetSize());
            Assert.Greater(id, 10000);
        }

        [Test]
        public void TestAbandonedBuffersGetFlushedOnTopologyChange()
        {
            Assert.Fail("TODO");
        }

        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        private static IgniteConfiguration GetServerConfiguration()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AutoGenerateIgniteInstanceName = true
            };
        }

        private static IIgnite StartServer()
        {
            return Ignition.Start(GetServerConfiguration());
        }

        private static ICacheClient<int, int> CreateCache(IIgniteClient client)
        {
            return client.CreateCache<int, int>(new CacheClientConfiguration
            {
                Name = TestUtils.TestName,
                CacheMode = CacheMode.Replicated
            });
        }

        private IIgniteClient StartClient()
        {
            var cfg = new IgniteClientConfiguration("127.0.0.1:10800..10805")
            {
                EnablePartitionAwareness = _enablePartitionAwareness
            };

            return new IgniteClient(cfg);
        }
    }
}