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
    using System;
    using System.Runtime.Serialization;
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
                Assert.AreEqual(string.Format("BinaryEnum [typeId={0}, enumValue={1}]",
                    BinaryUtils.GetStringHashCode(typeof(T).FullName), binRes.EnumValue), binRes.ToString());
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
            // Min values.
            var val = new EnumsBinarizable();
            
            var res = TestUtils.SerializeDeserialize(val);
            Assert.AreEqual(val, res);

            // Max values.
            val = new EnumsBinarizable
            {
                Byte = ByteEnum.Bar,
                Int = IntEnum.Bar,
                Long = LongEnum.Bar,
                SByte = SByteEnum.Bar,
                Short = ShortEnum.Bar,
                UInt = UIntEnum.Bar,
                ULong = ULongEnum.Bar,
                UShort = UShortEnum.Bar
            };

            res = TestUtils.SerializeDeserialize(val);
            Assert.AreEqual(val, res);
        }

        /// <summary>
        /// Tests enums as a field in ISerializable object.
        /// </summary>
        [Test]
        public void TestSerializableField()
        {
            // Min values.
            var val = new EnumsSerializable();

            var res = TestUtils.SerializeDeserialize(val);
            Assert.AreEqual(val, res);

            // Max values.
            val = new EnumsSerializable
            {
                Byte = ByteEnum.Bar,
                Int = IntEnum.Bar,
                Long = LongEnum.Bar,
                SByte = SByteEnum.Bar,
                Short = ShortEnum.Bar,
                UInt = UIntEnum.Bar,
                ULong = ULongEnum.Bar,
                UShort = UShortEnum.Bar
            };

            res = TestUtils.SerializeDeserialize(val);
            Assert.AreEqual(val, res);
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

        private class EnumsBinarizable
        {
            public ByteEnum Byte { get; set; }
            public SByteEnum SByte { get; set; }
            public ShortEnum Short { get; set; }
            public UShortEnum UShort { get; set; }
            public IntEnum Int { get; set; }
            public UIntEnum UInt { get; set; }
            public LongEnum Long { get; set; }
            public ULongEnum ULong { get; set; }

            private bool Equals(EnumsBinarizable other)
            {
                return Byte == other.Byte && SByte == other.SByte && Short == other.Short 
                    && UShort == other.UShort && Int == other.Int && UInt == other.UInt 
                    && Long == other.Long && ULong == other.ULong;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((EnumsBinarizable) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (int) Byte;
                    hashCode = (hashCode * 397) ^ (int) SByte;
                    hashCode = (hashCode * 397) ^ (int) Short;
                    hashCode = (hashCode * 397) ^ (int) UShort;
                    hashCode = (hashCode * 397) ^ (int) Int;
                    hashCode = (hashCode * 397) ^ (int) UInt;
                    hashCode = (hashCode * 397) ^ Long.GetHashCode();
                    hashCode = (hashCode * 397) ^ ULong.GetHashCode();
                    return hashCode;
                }
            }
        }

        [Serializable]
        private class EnumsSerializable : EnumsBinarizable, ISerializable
        {
            public EnumsSerializable()
            {
                // No-op.
            }

            protected EnumsSerializable(SerializationInfo info, StreamingContext context)
            {
                Byte = (ByteEnum) info.GetValue("byte", typeof(ByteEnum));
                SByte = (SByteEnum) info.GetValue("sbyte", typeof(SByteEnum));
                Short = (ShortEnum) info.GetValue("short", typeof(ShortEnum));
                UShort = (UShortEnum) info.GetValue("ushort", typeof(UShortEnum));
                Int = (IntEnum) info.GetValue("int", typeof(IntEnum));
                UInt = (UIntEnum) info.GetValue("uint", typeof(UIntEnum));
                Long = (LongEnum) info.GetValue("long", typeof(LongEnum));
                ULong = (ULongEnum) info.GetValue("ulong", typeof(ULongEnum));
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("byte", Byte);
                info.AddValue("sbyte", SByte);
                info.AddValue("short", Short);
                info.AddValue("ushort", UShort);
                info.AddValue("int", Int);
                info.AddValue("uint", UInt);
                info.AddValue("long", Long);
                info.AddValue("ulong", ULong);
            }
        }
    }
}
