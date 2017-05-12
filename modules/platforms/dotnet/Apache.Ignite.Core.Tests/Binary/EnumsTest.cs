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
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests enums serialization.
    /// </summary>
    public class EnumsTest
    {
        /// <summary>
        /// Tests direct enum value serialization.
        /// </summary>
        [Test]
        public void TestDirectValue()
        {
            CheckValue(ByteEnum.Foo);
            CheckValue(ByteEnum.Bar);

            CheckValue(SByteEnum.Foo);
            CheckValue(SByteEnum.Bar);

            CheckValue(ShortEnum.Foo);
            CheckValue(ShortEnum.Bar);
            
            CheckValue(UShortEnum.Foo);
            CheckValue(UShortEnum.Bar);
            
            CheckValue(IntEnum.Foo);
            CheckValue(IntEnum.Bar);
            
            CheckValue(UIntEnum.Foo);
            CheckValue(UIntEnum.Bar);

            CheckValue(LongEnum.Foo, false);
            CheckValue(LongEnum.Bar, false);
            
            CheckValue(ULongEnum.Foo, false);
            CheckValue(ULongEnum.Bar, false);
        }

        /// <summary>
        /// Checks the enum value serialization.
        /// </summary>
        private static void CheckValue<T>(T val, bool isBinaryEnum = true)
        {
            var marsh = new Marshaller(null) {CompactFooter = false};
            var bytes = marsh.Marshal(val);
            var res = marsh.Unmarshal<T>(bytes);
            var binRes = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            Assert.AreEqual(val, res);
            Assert.AreEqual(val, binRes.Deserialize<T>());

            if (isBinaryEnum)
            {
                Assert.AreEqual(TypeCaster<int>.Cast(val), binRes.EnumValue);
            }
            else
            {
                Assert.AreEqual(val, binRes.GetField<T>("value__"));
            }

            // Check array.
            var arr = new[] {val, val};
            var arrRes = TestUtils.SerializeDeserialize(arr);

            Assert.AreEqual(arr, arrRes);
        }

        /// <summary>
        /// Tests enums as a field in binarizable object.
        /// </summary>
        [Test]
        public void TestBinarizableField()
        {
            // TODO
        }

        /// <summary>
        /// Tests enums as a field in ISerializable object.
        /// </summary>
        [Test]
        public void TestSerializableField()
        {
            // TODO
        }

        private enum ByteEnum : byte
        {
            Foo = byte.MinValue,
            Bar = byte.MaxValue
        }

        private enum SByteEnum : sbyte
        {
            Foo = sbyte.MinValue,
            Bar = sbyte.MaxValue
        }

        private enum ShortEnum : short
        {
            Foo = short.MinValue,
            Bar = short.MaxValue
        }

        private enum UShortEnum : ushort
        {
            Foo = ushort.MinValue,
            Bar = ushort.MaxValue
        }

        private enum IntEnum
        {
            Foo = int.MinValue,
            Bar = int.MaxValue
        }

        private enum UIntEnum : uint
        {
            Foo = uint.MinValue,
            Bar = uint.MaxValue
        }

        private enum LongEnum : long
        {
            Foo = long.MinValue,
            Bar = long.MaxValue
        }

        private enum ULongEnum : ulong
        {
            Foo = ulong.MinValue,
            Bar = ulong.MaxValue
        }
    }
}
