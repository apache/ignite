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

            CheckValue(LongEnum.Foo);
            CheckValue(LongEnum.Bar);
            
            CheckValue(ULongEnum.Foo);
            CheckValue(ULongEnum.Bar);
        }

        /// <summary>
        /// Checks the enum value serialization.
        /// </summary>
        private static void CheckValue<T>(T val)
        {
            var marsh = new Marshaller(null) {CompactFooter = false};
            var bytes = marsh.Marshal(val);
            var res = marsh.Unmarshal<T>(bytes);
            var binRes = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            Assert.AreEqual(val, res);
            Assert.AreEqual(val, binRes.Deserialize<T>());

            if (binRes is BinaryEnum)
            {
                Assert.AreEqual(TypeCaster<int>.Cast(val), binRes.EnumValue);
            }
            else
            {
                Assert.AreEqual(val, binRes.GetField<T>("value__"));
            }

            // TODO: Check array
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
            Foo,
            Bar
        }

        private enum SByteEnum : sbyte
        {
            Foo = -1,
            Bar = 1
        }

        private enum ShortEnum : short
        {
            Foo = -1,
            Bar = 1
        }

        private enum UShortEnum : ushort
        {
            Foo,
            Bar
        }

        private enum IntEnum
        {
            Foo = -1,
            Bar = 1
        }

        private enum UIntEnum : uint
        {
            Foo,
            Bar
        }

        private enum LongEnum : long
        {
            Foo = -1,
            Bar = 1
        }

        private enum ULongEnum : ulong
        {
            Foo,
            Bar
        }
    }
}
