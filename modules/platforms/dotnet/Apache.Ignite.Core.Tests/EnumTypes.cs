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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Linq;

    public enum ByteEnum : byte
    {
        Foo = byte.MinValue,
        Bar = byte.MaxValue
    }
    
    public enum SByteEnum : sbyte
    {
        Foo = sbyte.MinValue,
        Bar = sbyte.MaxValue
    }
    
    public enum ShortEnum : short
    {
        Foo = short.MinValue,
        Bar = short.MaxValue
    }
    
    public enum UShortEnum : ushort
    {
        Foo = ushort.MinValue,
        Bar = ushort.MaxValue
    }
    
    public enum IntEnum
    {
        Foo = int.MinValue,
        Bar = int.MaxValue
    }
    
    public enum UIntEnum : uint
    {
        Foo = uint.MinValue,
        Bar = uint.MaxValue
    }
    
    public enum LongEnum : long
    {
        Foo = long.MinValue,
        Bar = long.MaxValue
    }
    
    public enum ULongEnum : ulong
    {
        Foo = ulong.MinValue,
        Bar = ulong.MaxValue
    }
    
    /// <summary>
    /// Holds enums declared as System.Enum.
    /// </summary>
    #pragma warning disable 659
    public class EnumsHolder
    {
        public Enum EnmByteRaw { get; set; } = ByteEnum.Bar;
        public Enum EnmUByteRaw { get; set; } = SByteEnum.Foo;
        public Enum EnmShortRaw { get; set; } = ShortEnum.Bar;
        public Enum EnmUShortRaw { get; set; } = UShortEnum.Foo;
        public Enum EnmIntRaw { get; set; } = IntEnum.Bar;
        public Enum EnmUIntRaw { get; set; } = UIntEnum.Foo;
        public Enum EnmLongRaw { get; set; } = LongEnum.Bar;
        public Enum EnmULongRaw { get; set; } = ULongEnum.Bar;
        public Enum[] EnmRawArr { get; set; } = new Enum[]
        {
            ByteEnum.Bar, SByteEnum.Foo, ShortEnum.Bar,
            UShortEnum.Foo, IntEnum.Bar, UIntEnum.Foo, LongEnum.Bar, ULongEnum.Foo
        };

        public EnumsHolder change()
        {
            EnmByteRaw = EnmByteRaw.Equals(ByteEnum.Bar) ? ByteEnum.Foo : ByteEnum.Bar;
            EnmByteRaw = EnmUByteRaw.Equals(SByteEnum.Bar) ? SByteEnum.Foo : SByteEnum.Bar;
            EnmShortRaw = EnmShortRaw.Equals(ShortEnum.Bar) ? ShortEnum.Foo : ShortEnum.Bar;
            EnmUShortRaw = EnmUShortRaw.Equals(UShortEnum.Bar) ? UShortEnum.Foo : UShortEnum.Bar;
            EnmIntRaw = EnmIntRaw.Equals(IntEnum.Bar) ? IntEnum.Foo : IntEnum.Bar;
            EnmUIntRaw = EnmUIntRaw.Equals(UIntEnum.Bar) ? UIntEnum.Foo : UIntEnum.Bar;
            EnmLongRaw = EnmLongRaw.Equals(LongEnum.Bar) ? LongEnum.Foo : LongEnum.Bar;
            EnmULongRaw = EnmULongRaw.Equals(ULongEnum.Bar) ? ULongEnum.Foo : ULongEnum.Bar;

            return this;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;

            var other = (EnumsHolder) obj;

            return Equals(EnmByteRaw, other.EnmByteRaw) && Equals(EnmUByteRaw, other.EnmUByteRaw) &&
                   Equals(EnmShortRaw, other.EnmShortRaw) && Equals(EnmUShortRaw, other.EnmUShortRaw) &&
                   Equals(EnmIntRaw, other.EnmIntRaw) && Equals(EnmUIntRaw, other.EnmUIntRaw) &&
                   Equals(EnmLongRaw, other.EnmLongRaw) && Equals(EnmULongRaw, other.EnmULongRaw) &&
                   Enumerable.SequenceEqual(EnmRawArr, other.EnmRawArr);
        }
    }
}