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
