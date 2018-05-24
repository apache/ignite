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

namespace Apache.Ignite.Core.Tests.Binary
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Equality comparers test.
    /// </summary>
    public class BinaryEqualityComparerTest
    {
        /// <summary>
        /// Tests simple equality.
        /// </summary>
        [Test]
        public void TestSimpleEquality()
        {
            var obj = GetBinaryObject(1, "x", 0);

            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(null, null));
            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj, obj));

            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj, null));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(null, obj));

            Assert.AreEqual(0, BinaryArrayEqualityComparer.GetHashCode(null));
            Assert.AreNotEqual(0, BinaryArrayEqualityComparer.GetHashCode(obj));
        }

        /// <summary>
        /// Tests hash code on a stream.
        /// </summary>
        [Test]
        public void TestStreamHashCode()
        {
            var ms = new BinaryHeapStream(10);

            Assert.AreEqual(1, BinaryArrayEqualityComparer.GetHashCode(ms, 0, 0));

            ms.WriteByte(1);
            Assert.AreEqual(31 + 1, BinaryArrayEqualityComparer.GetHashCode(ms, 0, 1));

            ms.WriteByte(3);
            Assert.AreEqual((31 + 1) * 31 + 3, BinaryArrayEqualityComparer.GetHashCode(ms, 0, 2));
        }

        /// <summary>
        /// Tests binary objects comparisons.
        /// </summary>
        [Test]
        public void TestBinaryObjects()
        {
            var obj1 = GetBinaryObject(1, "foo", 11);
            var obj2 = GetBinaryObject(1, "bar", 11);
            var obj3 = GetBinaryObject(2, "foo", 11);
            var obj4 = GetBinaryObject(2, "bar", 11);
            var obj5 = GetBinaryObject(1, "foo", 11);
            var obj6 = GetBinaryObject(1, "foo", 12);

            // Equals.
            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj1, obj1));
            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj1, obj5));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj1, obj2));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj1, obj3));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj1, obj4));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj1, obj6));

            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj2, obj2));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj2, obj5));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj2, obj3));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj2, obj4));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj2, obj6));

            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj3, obj3));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj3, obj5));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj3, obj4));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj3, obj6));

            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj4, obj4));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj4, obj5));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj4, obj6));

            Assert.IsTrue(BinaryArrayEqualityComparer.Equals(obj5, obj5));
            Assert.IsFalse(BinaryArrayEqualityComparer.Equals(obj5, obj6));

            // BinaryObject.GetHashCode.
            Assert.AreEqual(1934949494, obj1.GetHashCode());
            Assert.AreEqual(-2013102781, obj2.GetHashCode());
            Assert.AreEqual(1424415317, obj3.GetHashCode());
            Assert.AreEqual(1771330338, obj4.GetHashCode());
            Assert.AreEqual(obj1.GetHashCode(), obj5.GetHashCode());
            Assert.AreEqual(1934979285, BinaryArrayEqualityComparer.GetHashCode(obj6));

            // Comparer.GetHashCode.
            Assert.AreEqual(2001751043, BinaryArrayEqualityComparer.GetHashCode(GetBinaryObject(0, null, 0)));
            Assert.AreEqual(194296580, BinaryArrayEqualityComparer.GetHashCode(GetBinaryObject(1, null, 0)));
            Assert.AreEqual(1934949494, BinaryArrayEqualityComparer.GetHashCode(obj1));
            Assert.AreEqual(-2013102781, BinaryArrayEqualityComparer.GetHashCode(obj2));
            Assert.AreEqual(1424415317, BinaryArrayEqualityComparer.GetHashCode(obj3));
            Assert.AreEqual(1771330338, BinaryArrayEqualityComparer.GetHashCode(obj4));
            Assert.AreEqual(BinaryArrayEqualityComparer.GetHashCode(obj1), BinaryArrayEqualityComparer.GetHashCode(obj5));
            Assert.AreEqual(1934979285, BinaryArrayEqualityComparer.GetHashCode(obj6));

            // GetHashCode consistency.
            foreach (var obj in new[] {obj1, obj2, obj3, obj4, obj5, obj6})
                Assert.AreEqual(obj.GetHashCode(), BinaryArrayEqualityComparer.GetHashCode(obj));
        }

        /// <summary>
        /// Gets the binary object.
        /// </summary>
        private static IBinaryObject GetBinaryObject(int id, string name, int raw)
        {
            var marsh = new Marshaller(new BinaryConfiguration(typeof(Foo)));

            var bytes = marsh.Marshal(new Foo {Id = id, Name = name, Raw = raw});

            return marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);
        }

        private class Foo : IBinarizable
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Raw { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("id", Id);
                writer.WriteString("name", Name);

                writer.GetRawWriter().WriteInt(Raw);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Id = reader.ReadInt("id");
                Name = reader.ReadString("name");

                Raw = reader.GetRawReader().ReadInt();
            }
        }
    }
}
