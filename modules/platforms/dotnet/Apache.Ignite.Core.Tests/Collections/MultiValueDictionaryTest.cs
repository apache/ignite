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
    using Apache.Ignite.Core.Impl.Collections;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="MultiValueDictionary{TKey,TValue}"/>.
    /// </summary>
    public class MultiValueDictionaryTest
    {
        /// <summary>
        /// Tests the dictionary.
        /// </summary>
        [Test]
        public void TestMultiValueDictionary()
        {
            var dict = new MultiValueDictionary<int, int>();

            dict.Add(1, 1);
            dict.Add(1, 2);

            int val;

            Assert.IsTrue(dict.TryRemove(1, out val));
            Assert.AreEqual(2, val);

            Assert.IsTrue(dict.TryRemove(1, out val));
            Assert.AreEqual(1, val);

            Assert.IsFalse(dict.TryRemove(1, out val));

            dict.Add(2, 1);
            dict.Add(2, 2);
            dict.Remove(2, 3);
            dict.Remove(2, 2);

            Assert.IsTrue(dict.TryRemove(2, out val));
            Assert.AreEqual(1, val);
        }
    }
}
