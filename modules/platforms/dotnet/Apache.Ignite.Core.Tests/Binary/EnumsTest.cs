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
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
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
            var marsh = GetMarshaller();
            var ignite = Ignition.TryGetIgnite();

            var bytes = marsh.Marshal(val);
            var res = marsh.Unmarshal<T>(bytes);
            var binRes = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            Assert.AreEqual(val, res);
            Assert.AreEqual(val, binRes.Deserialize<T>());

            if (isBinaryEnum)
            {
                Assert.AreEqual(TypeCaster<int>.Cast(val), binRes.EnumValue);

                if (ignite != null)
                {
                    Assert.AreEqual(string.Format("{0} [typeId={1}, enumValue={2}, enumValueName={3}]",
                        typeof(T).FullName, binRes.GetBinaryType().TypeId, binRes.EnumValue, val), binRes.ToString());

                    var expectedEnumNames = Enum.GetValues(typeof(T)).OfType<T>().Select(x => x.ToString()).ToList();
                    var actualEnumNames = binRes.GetBinaryType().GetEnumValues().Select(v => v.EnumName).ToList();
                    
                    CollectionAssert.AreEquivalent(expectedEnumNames, actualEnumNames);
                }
                else
                {
                    Assert.AreEqual(string.Format("BinaryEnum [typeId={0}, enumValue={1}]",
                        BinaryUtils.GetStringHashCodeLowerCase(typeof(T).FullName), binRes.EnumValue), binRes.ToString());
                }
            }
            else
            {
                Assert.AreEqual(val, binRes.GetField<T>("value__"));
            }

            // Check array.
            CheckSerializeDeserialize(new[] {val, val});

            // Check caching.
            if (ignite != null)
            {
                var cache = ignite.GetOrCreateCache<int, T>(typeof(T).FullName);
                var binCache = cache.WithKeepBinary<int, IBinaryObject>();

                cache[1] = val;
                res = cache[1];
                binRes = binCache[1];

                Assert.AreEqual(val, res);
                Assert.AreEqual(val, binRes.Deserialize<T>());

                var arrCache = ignite.GetOrCreateCache<int, T[]>(typeof(T[]).FullName);
                arrCache[1] = new[] {val, val};
                Assert.AreEqual(new[] { val, val }, arrCache[1]);
            }
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        private static Marshaller GetMarshaller()
        {
            var ignite = Ignition.TryGetIgnite();

            return ignite != null
                ? ((Ignite) ignite).Marshaller
                : new Marshaller(null) {CompactFooter = false};
        }

        /// <summary>
        /// Serializes and deserializes a value.
        /// </summary>
        private static void CheckSerializeDeserialize<T>(T val, bool cacheOnly = false)
        {
            if (!cacheOnly)
            {
                var marsh = GetMarshaller();

                var res = marsh.Unmarshal<T>(marsh.Marshal(val));
                Assert.AreEqual(val, res);
            }

            var ignite = Ignition.TryGetIgnite();

            if (ignite != null)
            {
                var cache = ignite.GetOrCreateCache<int, T>(typeof(T).FullName);

                cache.Put(1, val);
                var res = cache.Get(1);

                Assert.AreEqual(val, res);
            }
        }

        /// <summary>
        /// Convert to IBinaryObject.
        /// </summary>
        private static IBinaryObject ToBinary<T>(T val)
        {
            var marsh = GetMarshaller();

            return marsh.Unmarshal<IBinaryObject>(marsh.Marshal(val), BinaryMode.ForceBinary);
        }

        /// <summary>
        /// Tests enums as a field in binarizable object.
        /// </summary>
        [Test]
        public void TestBinarizableField()
        {
            // Min values.
            var val = new EnumsBinarizable();
            
            CheckSerializeDeserialize(val);

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

            CheckSerializeDeserialize(val);
        }

        /// <summary>
        /// Tests enums as a BinaryObject field in binarizable object.
        /// </summary>
        [Test]
        public void TestBinarizableFieldAsBinaryObject()
        {
            // Null values.
            var val = new EnumsBinaryForm();
            
            CheckSerializeDeserialize(val);

            // Max values.
            val = new EnumsBinaryForm
            {
                Byte = ToBinary(ByteEnum.Bar),
                Int = ToBinary(IntEnum.Bar),
                Long = ToBinary(LongEnum.Bar),
                SByte = ToBinary(SByteEnum.Bar),
                Short = ToBinary(ShortEnum.Bar),
                UInt = ToBinary(UIntEnum.Bar),
                ULong = ToBinary(ULongEnum.Bar),
                UShort = ToBinary(UShortEnum.Bar)
            };

            CheckSerializeDeserialize(val, true);
        }

        /// <summary>
        /// Tests enums as a nullable field in binarizable object.
        /// </summary>
        [Test]
        public void TestBinarizableNullableField()
        {
            // Default values.
            var val = new EnumsBinarizableNullable();
            
            CheckSerializeDeserialize(val);

            // Max values.
            val = new EnumsBinarizableNullable
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

            CheckSerializeDeserialize(val);
        }

        /// <summary>
        /// Tests enums as a field in ISerializable object.
        /// </summary>
        [Test]
        public void TestSerializableField()
        {
            // Min values.
            var val = new EnumsSerializable();

            CheckSerializeDeserialize(val);

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

            CheckSerializeDeserialize(val);
        }

        /// <summary>
        /// Tests enums as a nullable field in ISerializable object.
        /// </summary>
        [Test]
        public void TestSerializableNullableField()
        {
            // Default values.
            var val = new EnumsSerializableNullable();

            CheckSerializeDeserialize(val);

            // Max values.
            val = new EnumsSerializableNullable
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

            CheckSerializeDeserialize(val);
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

        private class EnumsBinaryForm
        {
            public IBinaryObject Byte { get; set; }
            public IBinaryObject SByte { get; set; }
            public IBinaryObject Short { get; set; }
            public IBinaryObject UShort { get; set; }
            public IBinaryObject Int { get; set; }
            public IBinaryObject UInt { get; set; }
            public IBinaryObject Long { get; set; }
            public IBinaryObject ULong { get; set; }

            private bool Equals(EnumsBinaryForm other)
            {
                return Equals(Byte, other.Byte) && Equals(SByte, other.SByte) && Equals(Short, other.Short) &&
                       Equals(UShort, other.UShort) && Equals(Int, other.Int) && Equals(UInt, other.UInt) &&
                       Equals(Long, other.Long) && Equals(ULong, other.ULong);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((EnumsBinaryForm) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (Byte != null ? Byte.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (SByte != null ? SByte.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (Short != null ? Short.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (UShort != null ? UShort.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (Int != null ? Int.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (UInt != null ? UInt.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (Long != null ? Long.GetHashCode() : 0);
                    hashCode = (hashCode*397) ^ (ULong != null ? ULong.GetHashCode() : 0);
                    return hashCode;
                }
            }
        }

        private class EnumsBinarizableNullable
        {
            public ByteEnum? Byte { get; set; }
            public SByteEnum? SByte { get; set; }
            public ShortEnum? Short { get; set; }
            public UShortEnum? UShort { get; set; }
            public IntEnum? Int { get; set; }
            public UIntEnum? UInt { get; set; }
            public LongEnum? Long { get; set; }
            public ULongEnum? ULong { get; set; }

            private bool Equals(EnumsBinarizableNullable other)
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
                return Equals((EnumsBinarizableNullable) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Byte.GetHashCode();
                    hashCode = (hashCode*397) ^ SByte.GetHashCode();
                    hashCode = (hashCode*397) ^ Short.GetHashCode();
                    hashCode = (hashCode*397) ^ UShort.GetHashCode();
                    hashCode = (hashCode*397) ^ Int.GetHashCode();
                    hashCode = (hashCode*397) ^ UInt.GetHashCode();
                    hashCode = (hashCode*397) ^ Long.GetHashCode();
                    hashCode = (hashCode*397) ^ ULong.GetHashCode();
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

        [Serializable]
        private class EnumsSerializableNullable : EnumsBinarizableNullable, ISerializable
        {
            public EnumsSerializableNullable()
            {
                // No-op.
            }

            protected EnumsSerializableNullable(SerializationInfo info, StreamingContext context)
            {
                Byte = (ByteEnum?) info.GetValue("byte", typeof(ByteEnum));
                SByte = (SByteEnum?) info.GetValue("sbyte", typeof(SByteEnum));
                Short = (ShortEnum?) info.GetValue("short", typeof(ShortEnum));
                UShort = (UShortEnum?) info.GetValue("ushort", typeof(UShortEnum));
                Int = (IntEnum?) info.GetValue("int", typeof(IntEnum));
                UInt = (UIntEnum?) info.GetValue("uint", typeof(UIntEnum));
                Long = (LongEnum?) info.GetValue("long", typeof(LongEnum));
                ULong = (ULongEnum?) info.GetValue("ulong", typeof(ULongEnum));
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
