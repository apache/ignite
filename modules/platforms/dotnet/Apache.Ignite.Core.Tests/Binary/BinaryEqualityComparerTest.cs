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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Equality comparers test.
    /// </summary>
    public class BinaryEqualityComparerTest
    {
        /// <summary>
        /// Tests common public methods logic.
        /// </summary>
        [Test]
        [SuppressMessage("ReSharper", "ReturnValueOfPureMethodIsNotUsed")]
        public void TestPublicMethods()
        {
            var cmps = new IEqualityComparer<IBinaryObject>[]
            {
                new BinaryArrayEqualityComparer()
                //new BinaryFieldEqualityComparer()
            };

            var obj = GetBinaryObject(1, "x");

            foreach (var cmp in cmps)
            {
                Assert.IsTrue(cmp.Equals(null, null));
                Assert.IsTrue(cmp.Equals(obj, obj));

                Assert.IsFalse(cmp.Equals(obj, null));
                Assert.IsFalse(cmp.Equals(null, obj));

                Assert.AreEqual(0, cmp.GetHashCode(null));
                Assert.AreNotEqual(0, cmp.GetHashCode(obj));
            }
        }

        /// <summary>
        /// Tests the custom comparer.
        /// </summary>
        [Test]
        public void TestCustomComparer()
        {
            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(
                new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    BinaryConfiguration = new BinaryConfiguration
                    {
                        TypeConfigurations = new[]
                        {
                            new BinaryTypeConfiguration(typeof(Foo))
                            {
                                EqualityComparer = new MyComparer()
                            }
                        }
                    }
                }));

            Assert.AreEqual("Unsupported IEqualityComparer<IBinaryObject> implementation: " +
                            "Apache.Ignite.Core.Tests.Binary.BinaryEqualityComparerTest+MyComparer. " +
                            "Only predefined implementations are supported.", ex.Message);
        }

        /// <summary>
        /// Tests the array comparer.
        /// </summary>
        [Test]
        public void TestArrayComparer()
        {
            var cmp = (IBinaryEqualityComparer) new BinaryArrayEqualityComparer();

            var ms = new BinaryHeapStream(10);

            Assert.AreEqual(1, cmp.GetHashCode(ms, 0, 0, null, 0, null, null));

            ms.WriteByte(1);
            Assert.AreEqual(31 + 1, cmp.GetHashCode(ms, 0, 1, null, 0, null, null));

            ms.WriteByte(3);
            Assert.AreEqual((31 + 1) * 31 + 3, cmp.GetHashCode(ms, 0, 2, null, 0, null, null));
        }

        /// <summary>
        /// Tests public methods of array comparer.
        /// </summary>
        [Test]
        public void TestArrayComparerPublic()
        {
            var cmp = new BinaryArrayEqualityComparer();

            var obj1 = GetBinaryObject(1, "foo");
            var obj2 = GetBinaryObject(1, "bar");
            var obj3 = GetBinaryObject(2, "foo");
            var obj4 = GetBinaryObject(2, "bar");
            var obj5 = GetBinaryObject(1, "foo");

            // Equals.
            Assert.IsTrue(cmp.Equals(obj1, obj1));
            Assert.IsTrue(cmp.Equals(obj1, obj5));
            Assert.IsFalse(cmp.Equals(obj1, obj2));
            Assert.IsFalse(cmp.Equals(obj1, obj3));
            Assert.IsFalse(cmp.Equals(obj1, obj4));

            Assert.IsTrue(cmp.Equals(obj2, obj2));
            Assert.IsFalse(cmp.Equals(obj2, obj5));
            Assert.IsFalse(cmp.Equals(obj2, obj3));
            Assert.IsFalse(cmp.Equals(obj2, obj4));

            Assert.IsTrue(cmp.Equals(obj3, obj3));
            Assert.IsFalse(cmp.Equals(obj3, obj5));
            Assert.IsFalse(cmp.Equals(obj3, obj4));

            Assert.IsTrue(cmp.Equals(obj4, obj4));
            Assert.IsFalse(cmp.Equals(obj4, obj5));

            Assert.IsTrue(cmp.Equals(obj5, obj5));

            // GetHashCode.
            Assert.AreEqual(1, cmp.GetHashCode(GetBinaryObject(0, null)));
        }

        /// <summary>
        /// Tests the field comparer.
        /// </summary>
        [Test]
        public void TestFieldComparer()
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof(Foo))
                    {
                        EqualityComparer = new BinaryFieldEqualityComparer("Name", "Id")
                    }
                }
            });

            var val = new Foo {Id = 58, Name = "John"};
            var binObj = marsh.Unmarshal<IBinaryObject>(marsh.Marshal(val), BinaryMode.ForceBinary);
            var expHash = val.Name.GetHashCode() * 31 + val.Id.GetHashCode();
            Assert.AreEqual(expHash, binObj.GetHashCode());

            val = new Foo {Id = 95};
            binObj = marsh.Unmarshal<IBinaryObject>(marsh.Marshal(val), BinaryMode.ForceBinary);
            expHash = val.Id.GetHashCode();
            Assert.AreEqual(expHash, binObj.GetHashCode());
        }

        /// <summary>
        /// Tests the field comparer validation.
        /// </summary>
        [Test]
        public void TestFieldComparerValidation()
        {
            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(
                new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    BinaryConfiguration = new BinaryConfiguration
                    {
                        TypeConfigurations = new[]
                        {
                            new BinaryTypeConfiguration(typeof(Foo))
                            {
                                EqualityComparer = new BinaryFieldEqualityComparer()
                            }
                        }
                    }
                }));

            Assert.AreEqual("BinaryFieldEqualityComparer.FieldNames can not be null or empty.", ex.Message);
        }

        /// <summary>
        /// Gets the binary object.
        /// </summary>
        private static IBinaryObject GetBinaryObject(int id, string name)
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof(Foo))
                    {
                        EqualityComparer = new BinaryArrayEqualityComparer()
                    }
                }
            });

            var bytes = marsh.Marshal(new Foo {Id = id, Name = name});

            return marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);
        }

        private class Foo
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        private class MyComparer : IEqualityComparer<IBinaryObject>
        {
            public bool Equals(IBinaryObject x, IBinaryObject y)
            {
                return true;
            }

            public int GetHashCode(IBinaryObject obj)
            {
                return 0;
            }
        }
    }
}
