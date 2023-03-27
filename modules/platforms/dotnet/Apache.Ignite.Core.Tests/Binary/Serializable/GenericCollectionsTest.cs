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

namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System.Collections.Generic;
    using NUnit.Framework;

    /// <summary>
    /// Tests Generic collections serializtion/deserialization scenarios.
    /// </summary>
    public class GenericCollectionsTest
    {
        /// <summary>
        /// Tests Dictionary.
        /// </summary>
        [Test]
        public void TestDictionary()
        {
            TestCollection(new Dictionary<int, int> {{1, 1}, {2, 2}});
            TestCollection(new Dictionary<ByteEnum, int> {{ByteEnum.One, 1}, {ByteEnum.Two, 2}});
            TestCollection(new Dictionary<IntEnum, int> {{IntEnum.One, 1}, {IntEnum.Two, 2}});
        }

        /// <summary>
        /// Tests SortedDictionary.
        /// </summary>
        [Test]
        public void TestSortedDictionary()
        {
            TestCollection(new SortedDictionary<int, int> {{1, 1}, {2, 2}});
            TestCollection(new SortedDictionary<ByteEnum, int> {{ByteEnum.One, 1}, {ByteEnum.Two, 2}});
            TestCollection(new SortedDictionary<IntEnum, int> {{IntEnum.One, 1}, {IntEnum.Two, 2}});
        }

        /// <summary>
        /// Tests List.
        /// </summary>
        [Test]
        public void TestList()
        {
            TestCollection(new List<int> {1, 2});
            TestCollection(new List<ByteEnum> {ByteEnum.One, ByteEnum.Two});
            TestCollection(new List<IntEnum> {IntEnum.One, IntEnum.Two});
        }

        /// <summary>
        /// Tests LinkedList.
        /// </summary>
        [Test]
        public void TestLinkedList()
        {
            TestCollection(new LinkedList<int>(new List<int> { 1, 2 }));
            TestCollection(new LinkedList<ByteEnum>(new List<ByteEnum> {ByteEnum.One, ByteEnum.Two}));
            TestCollection(new LinkedList<IntEnum>(new List<IntEnum> {IntEnum.One, IntEnum.Two}));
        }

        /// <summary>
        /// Tests HashSet.
        /// </summary>
        [Test]
        public void TestHashSet()
        {
            TestCollection(new HashSet<int> {1, 2});
            TestCollection(new HashSet<ByteEnum> {ByteEnum.One, ByteEnum.Two});
            TestCollection(new HashSet<IntEnum> {IntEnum.One, IntEnum.Two});
        }

        /// <summary>
        /// Tests SortedSet.
        /// </summary>
        [Test]
        public void TestSortedSet()
        {
            TestCollection(new SortedSet<int> {1, 2});
            TestCollection(new SortedSet<ByteEnum> {ByteEnum.One, ByteEnum.Two});
            TestCollection(new SortedSet<IntEnum> {IntEnum.One, IntEnum.Two});
        }

        private static void TestCollection<T>(ICollection<T> collection)
        {
            var res = TestUtils.SerializeDeserialize(collection);
            Assert.AreEqual(collection, res);
        }

        private enum ByteEnum : byte
        {
            One = 1,
            Two = 2,
        }

        private enum IntEnum 
        {
            One = 1,
            Two = 2,
        }
    }
}
