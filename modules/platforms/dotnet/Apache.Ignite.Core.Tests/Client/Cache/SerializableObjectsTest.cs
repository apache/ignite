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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="ISerializable"/> object handling in Thin Client.
    /// </summary>
    public class SerializableObjectsTest : ClientTestBase
    {
        /** */
        private const int EntryCount = 50;

        /** */
        private static readonly int[] Keys = Enumerable.Range(0, EntryCount).ToArray();
        
        /// <summary>
        /// Tests DateTime metadata caching.
        /// </summary>
        [Test]
        public void TestDateTimeMetaCachingOnPut([Values(true, false)] bool scanQuery)
        {
            var requestName = scanQuery ? "ClientCacheScanQuery" : "ClientCacheGetAll";
            var cache = GetPopulatedCache();
            
            var res = scanQuery
                ? cache.Query(new ScanQuery<int, DateTimeTest>()).GetAll()
                : cache.GetAll(Keys);

            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one request is sent to the server:
            // metadata is already cached and should not be requested.
            Assert.AreEqual(new[] {requestName}, requests);

            // Verify results.
            Assert.AreEqual(EntryCount, res.Count);
            Assert.AreEqual(DateTimeTest.DefaultDateTime, res.Min(x => x.Value.Date));
        }

        /// <summary>
        /// Tests DateTime metadata caching.
        /// </summary>
        [Test]
        public void TestDateTimeMetaCachingOnGet([Values(true, false)] bool scanQuery)
        {
            // Retrieve data from a different client which does not yet have cached meta.
            var requestName = scanQuery ? "ClientCacheScanQuery" : "ClientCacheGetAll";
            var cache = GetClient().GetCache<int, DateTimeTest>(GetPopulatedCache().Name);
            
            var res = scanQuery
                ? cache.Query(new ScanQuery<int, DateTimeTest>()).GetAll()
                : cache.GetAll(Keys);
            
            var requests = GetAllServerRequestNames().ToArray();

            // Verify that only one BinaryTypeGet request per type is sent to the server.
            var expectedRequests = new[]
            {
                requestName, 
                "ClientBinaryTypeNameGet",
                "ClientBinaryTypeGet",
                "ClientBinaryTypeNameGet",
                "ClientBinaryTypeGet"
            };
            Assert.AreEqual(expectedRequests, requests);
            
            // Verify results.
            Assert.AreEqual(EntryCount, res.Count);
            Assert.AreEqual(DateTimeTest.DefaultDateTime, res.Min(x => x.Value.Date));
        }
        
        /// <summary>
        /// Gets the populated cache.
        /// </summary>
        private ICacheClient<int, DateTimeTest> GetPopulatedCache()
        {
            var cacheName = TestContext.CurrentContext.Test.Name;
            var cache = Client.GetOrCreateCache<int, DateTimeTest>(cacheName);
            cache.PutAll(GetData().Select(x => new KeyValuePair<int, DateTimeTest>(x.Id, x)));

            ClearLoggers();
            return cache;
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        private static IEnumerable<DateTimeTest> GetData()
        {
            return Keys
                .Select(x => new DateTimeTest
                {
                    Id = x,
                    Date = DateTimeTest.DefaultDateTime.AddHours(x),
                });
        }

        /// <summary>
        /// Test class with DateTime field.
        /// </summary>
        private class DateTimeTest
        {
            /** */
            public static readonly DateTime DefaultDateTime = new DateTime(2002, 2, 2);
            
            /** */
            public int Id { get; set; }
            
            /** */
            public DateTime Date { get; set; }
        }
    }
}