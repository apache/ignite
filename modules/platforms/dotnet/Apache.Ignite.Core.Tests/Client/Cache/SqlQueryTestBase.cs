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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Base class for SQL tests.
    /// </summary>
    public class SqlQueryTestBase : ClientTestBase
    {
        /// <summary>
        /// Cache item count.
        /// </summary>
        protected const int Count = 10;

        /// <summary>
        /// Second cache name.
        /// </summary>
        protected const string CacheName2 = CacheName + "2";

        /// <summary>
        /// Initializes a new instance of the <see cref="ScanQueryTest"/> class.
        /// </summary>
        public SqlQueryTestBase() : base(2)
        {
            // No-op.
        }

        /// <summary>
        /// Sets up the test.
        /// </summary>
        public override void TestSetUp()
        {
            InitCache(CacheName);
            InitCache(CacheName2);
        }

        /// <summary>
        /// Initializes the cache.
        /// </summary>
        private static void InitCache(string cacheName)
        {
            var cache = Ignition.GetIgnite().GetOrCreateCache<int, Person>(
                new CacheConfiguration(cacheName, new QueryEntity(typeof(int), typeof(Person))));

            cache.RemoveAll();

            cache.PutAll(Enumerable.Range(1, Count).ToDictionary(x => x, x => new Person(x)));
        }
    }
}