/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    using System.Linq;
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ReadOnlyCollection{T}"/>
    /// </summary>
    public class ReadOnlyCollectionTest
    {
        /// <summary>
        /// Tests the disctionary.
        /// </summary>
        [Test]
        public void TestCollection()
        {
            // Default ctor.
            var data = Enumerable.Range(1, 5).ToArray();
            var col = new ReadOnlyCollection<int>(data);

            Assert.AreEqual(5, col.Count);
            Assert.IsTrue(col.IsReadOnly);
            CollectionAssert.AreEqual(data, col);

            Assert.IsTrue(col.GetEnumerator().MoveNext());
            Assert.IsTrue(((IEnumerable) col).GetEnumerator().MoveNext());

            Assert.IsTrue(col.Contains(4));

            var arr = new int[5];
            col.CopyTo(arr, 0);
            CollectionAssert.AreEqual(data, arr);

            Assert.Throws<NotSupportedException>(() => col.Add(1));
            Assert.Throws<NotSupportedException>(() => col.Clear());
            Assert.Throws<NotSupportedException>(() => col.Remove(1));
        }
    }
}