/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Cache;
    using NUnit.Framework;

    /// <summary>
    /// <see cref="CacheEntry{K,V}"/> tests.
    /// </summary>
    public class GridCacheEntryTest
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
