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

namespace Apache.Ignite.Core.Tests.Collections
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ReadOnlyDictionary{TKey,TValue}"/>
    /// </summary>
    public class ReadOnlyDictionaryTest
    {
        /// <summary>
        /// Tests the disctionary.
        /// </summary>
        [Test]
        public void TestDictionary()
        {
            // Default ctor.
            var data = Enumerable.Range(1, 5).ToDictionary(x => x, x => x.ToString());
            var dict = new ReadOnlyDictionary<int, string>(data);

            Assert.AreEqual(5, dict.Count);
            Assert.IsTrue(dict.IsReadOnly);
            CollectionAssert.AreEqual(data, dict);
            CollectionAssert.AreEqual(data.Keys, dict.Keys);
            CollectionAssert.AreEqual(data.Values, dict.Values);

            Assert.IsTrue(dict.GetEnumerator().MoveNext());
            Assert.IsTrue(((IEnumerable) dict).GetEnumerator().MoveNext());

            Assert.IsTrue(dict.ContainsKey(1));
            Assert.IsTrue(dict.Contains(new KeyValuePair<int, string>(4, "4")));
            Assert.AreEqual("3", dict[3]);

            string val;
            Assert.IsTrue(dict.TryGetValue(2, out val));
            Assert.AreEqual("2", val);

            var arr = new KeyValuePair<int, string>[5];
            dict.CopyTo(arr, 0);
            CollectionAssert.AreEqual(data, arr);

            Assert.Throws<NotSupportedException>(() => dict.Add(1, "2"));
            Assert.Throws<NotSupportedException>(() => dict.Add(new KeyValuePair<int, string>(1, "2")));
            Assert.Throws<NotSupportedException>(() => dict.Clear());
            Assert.Throws<NotSupportedException>(() => dict.Remove(1));
            Assert.Throws<NotSupportedException>(() => dict.Remove(new KeyValuePair<int, string>(1, "2")));
        }
    }
}
