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
            // TODO: check serialization and metadata.
            Assert.AreEqual(ByteEnum.Foo, TestUtils.SerializeDeserialize(ByteEnum.Foo));
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
