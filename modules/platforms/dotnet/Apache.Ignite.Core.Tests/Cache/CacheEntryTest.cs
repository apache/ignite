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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Cache;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="CacheEntry{TK,TV}"/> tests.
    /// </summary>
    public class CacheEntryTest
    {
        /// <summary>
        /// Tests equality members.
        /// </summary>
        [Test]
        public void TestEquality()
        {
            var entry1 = new CacheEntry<int, int>(1, 2);
            var entry2 = new CacheEntry<int, int>(1, 2);
            var entry3 = new CacheEntry<int, int>(1, 3);

            Assert.AreEqual(entry1, entry2);
            Assert.AreNotEqual(entry1, entry3);

            var boxedEntry1 = (object) entry1;
            var boxedEntry2 = (object) entry2;
            var boxedEntry3 = (object) entry3;

            Assert.IsFalse(ReferenceEquals(boxedEntry1, boxedEntry2));

            Assert.AreEqual(boxedEntry1, boxedEntry2);
            Assert.AreNotEqual(boxedEntry1, boxedEntry3);
        }

        /// <summary>
        /// Tests with hash data structures.
        /// </summary>
        [Test]
        public void TestHashCode()
        {
            var entry1 = new CacheEntry<int, int>(1, 2);
            var entry2 = new CacheEntry<int, int>(1, 2);
            var entry3 = new CacheEntry<int, int>(1, 3);

            var set = new HashSet<object> {entry1};

            Assert.IsTrue(set.Contains(entry1));
            Assert.IsTrue(set.Contains(entry2));
            Assert.IsFalse(set.Contains(entry3));
        }
    }
}
