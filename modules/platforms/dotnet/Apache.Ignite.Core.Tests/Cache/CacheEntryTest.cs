/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
