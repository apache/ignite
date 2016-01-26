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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests LINQ queries.
    /// </summary>
    public class CacheLinqTest
    {
        /** Cache name. */
        private const string CacheName = "cache";

        /** */
        private const int DataSize = 100;

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(new IgniteConfiguration
            {
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration = new BinaryConfiguration(typeof(LinqPerson))
            });

            var cache = GetCache();

            for (var i = 0; i < DataSize; i++)
                cache.Put(i, new LinqPerson(i, "Person_" + i));

            Assert.AreEqual(1, cache[1].Age);
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestEmptyQuery()
        {
            var cache = GetCache();

            var results = cache.ToQueryable().ToArray();

            Assert.AreEqual(DataSize, results.Length);
        }

        [Test]
        public void TestWhere()
        {
            var cache = GetCache();

            Assert.AreEqual(10, cache.ToQueryable()
                .Where(x => x.Value.Age < 10).ToArray().Length);

            Assert.AreEqual(19, cache.ToQueryable()
                .Where(x => x.Value.Age > 10 && x.Value.Age < 30).ToArray().Length);

            Assert.AreEqual(20, cache.ToQueryable()
                .Where(x => x.Value.Age > 10).Where(x => x.Value.Age < 30 || x.Value.Age == 50).ToArray().Length);
        }

        [Test]
        [Ignore("Contains does not work for some reason")]
        public void TestStrings()
        {
            var cache = GetCache();

            var result = cache.ToQueryable().Where(x => x.Value.Name.Contains("Person")).ToArray();

            Assert.AreEqual(DataSize, result.Length);
        }

        private static ICache<int, LinqPerson> GetCache()
        {
            return Ignition.GetIgnite().GetOrCreateCache<int, LinqPerson>(
                new CacheConfiguration(CacheName, typeof (LinqPerson)));
        }

        public class LinqPerson
        {
            public LinqPerson(int age, string name)
            {
                Age = age;
                Name = name;
            }

            [QueryField]
            public int Age { get; set; }

            [QueryField]
            public string Name { get; set; }
        }
    }
}
