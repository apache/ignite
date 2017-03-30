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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using NUnit.Framework;

    /// <summary>
    /// Tests SQL and DML with Serializable types.
    /// </summary>
    public class SqlDmlTest
    {
        /** */
        private IIgnite _ignite;

        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(SimpleSerializable))
            };

            _ignite = Ignition.Start(cfg);
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
        /// Tests the simple serializable.
        /// </summary>
        [Test]
        public void TestSimpleSerializable()
        {
            var cache = _ignite.CreateCache<int, SimpleSerializable>(
                new CacheConfiguration("simple", typeof(SimpleSerializable)));

            cache[1] = new SimpleSerializable(1, "bar1");
            cache[2] = new SimpleSerializable(2, "bar2");

            var res = cache.Query(new SqlQuery(typeof(SimpleSerializable), "where foo = 2")).GetAll().Single();

            Assert.AreEqual(2, res.Key);
            Assert.AreEqual(2, res.Value.Foo);
            Assert.AreEqual("bar2", res.Value.Bar);
        }

        private class SimpleSerializable : ISerializable
        {
            [QuerySqlField]
            public int Foo { get; set; }
            
            [QuerySqlField]
            public string Bar { get; set; }

            public SimpleSerializable(int foo, string bar)
            {
                Foo = foo;
                Bar = bar;
            }

            public SimpleSerializable(SerializationInfo info, StreamingContext context)
            {
                Foo = info.GetInt32("Foo");
                Bar = info.GetString("Bar");
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("Foo", Foo);
                info.AddValue("Bar", Bar);
            }
        }
    }
}
