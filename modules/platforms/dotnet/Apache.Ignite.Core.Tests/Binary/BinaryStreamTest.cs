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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for binary streaming.
    /// </summary>
    public class BinaryStreamTest
    {
        private IIgnite igniteServer;
        private CacheClientConfiguration clientCacheConfiguration;
        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var config = TestUtils.GetTestConfiguration();
            try
            {
                igniteServer = Ignition.Start(new IgniteConfiguration(config));
            } catch (Exception e)
            {
                GetLogger().Log(LogLevel.Warn, "Exception occurred starting Ignite using test config: {0}", e);
                Console.WriteLine();
                Ignition.Start();
            }
            this.clientCacheConfiguration = new CacheClientConfiguration()
            {
                Name = "cache1",
                SqlSchema = "persons",
                QueryEntities = new QueryEntity[]
                {
                    new QueryEntity()
                    {
                        ValueTypeName = "Person",
                        Fields = new List<QueryField>
                        {
                            new QueryField
                            {
                                Name = "Name",
                                FieldType = typeof(string),
                            },
                            new QueryField
                            {
                                Name = "Age",
                                FieldType = typeof(int)
                            }
                        }
                    }
                }
            };
        }

        /// <summary>
        /// Tears down the test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that streaming binary objects with a thin client results in those objects being 
        /// available through SQL in the cache's table.
        /// </summary>
        [Test]
        public void TestThinClientBinaryStreamingLeadsToSqlRecord()
        {
            var thinClient = Ignition.StartClient();
            var cacheClientBinary = thinClient.GetOrCreateCache<int, IBinaryObject>(this.clientCacheConfiguration).WithKeepBinary<int, IBinaryObject>();

            // prepare a binary object
            IBinaryObjectBuilder builder = thinClient.GetBinary().GetBuilder("Person");
            var jane = builder
              .SetStringField("Name", "Jane")
              .SetIntField("Age", 43)
              .Build();

            const int key = 1;

            // stream the binary object to the server
            using (var stmr = thinClient.GetDataStreamer<int, IBinaryObject>("cache1"))
            {
                stmr.Add(key, jane);
                stmr.Flush();
            }

            // check the result dimensions
            var fullResultAfterClientStreamer = cacheClientBinary.Query(new SqlFieldsQuery("SELECT * FROM \"PERSONS\".PERSON")).GetAll();
            Assert.IsNotNull(fullResultAfterClientStreamer);
            Assert.AreEqual(1, fullResultAfterClientStreamer.Count);
            Assert.AreEqual(1, fullResultAfterClientStreamer[0].Count);

            // check the actual content of the result
            Assert.AreEqual(1, fullResultAfterClientStreamer[0][0]);

            // check the cache content
            var element = cacheClientBinary[0];
            Assert.AreEqual("Jane", element.GetField<string>("Name"));
            Assert.AreEqual(43, element.GetField<int>("Age"));
        }

        /// <summary>
        /// Tests that streaming binary objects with the server object results in those objects being 
        /// available through SQL in the cache's table.
        /// </summary>
        [Test]
        public void TestIIgniteBinaryStreamingLeadsToSqlRecord()
        {
            var thinClient = Ignition.StartClient();
            var cacheClientBinary = thinClient.GetOrCreateCache<int, IBinaryObject>(this.clientCacheConfiguration).WithKeepBinary<int, IBinaryObject>();

            // prepare a binary object
            IBinaryObjectBuilder builder = thinClient.GetBinary().GetBuilder("Person");
            var jane = builder
              .SetStringField("Name", "Jane")
              .SetIntField("Age", 43)
              .Build();

            const int key = 1;

            // stream the binary object to the server
            using (var stmr = this.igniteServer.GetDataStreamer<int, IBinaryObject>("cache1"))
            {
                stmr.Add(key, jane);
                stmr.Flush();
            }

            // check the result dimensions
            var fullResultAfterClientStreamer = cacheClientBinary.Query(new SqlFieldsQuery("SELECT * FROM \"PERSONS\".PERSON")).GetAll();
            Assert.IsNotNull(fullResultAfterClientStreamer);
            Assert.AreEqual(1, fullResultAfterClientStreamer.Count);
            Assert.AreEqual(1, fullResultAfterClientStreamer[0].Count);

            // check the actual content of the result
            Assert.AreEqual(1, fullResultAfterClientStreamer[0][0]);

            // check the cache content
            var element = cacheClientBinary[key];
            Assert.AreEqual("Jane", element.GetField<string>("Name"));
            Assert.AreEqual(43, element.GetField<int>("Age"));
        }

        /// <summary>
        /// Gets the logger for tests.
        /// </summary>
        private static ListLogger GetLogger()
        {
            return new ListLogger(new ConsoleLogger
            {
                MinLevel = LogLevel.Trace
            })
            {
                EnabledLevels = Enum.GetValues(typeof(LogLevel)).Cast<LogLevel>().ToArray()
            };
        }
    }
}
