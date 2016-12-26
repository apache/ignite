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

namespace Apache.Ignite.Core.Tests.Binary.IO
{
    using System;
    using System.IO;
    using System.Text;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Memory;
    using NUnit.Framework;

    /// <summary>
    /// Tests binary streams.
    /// </summary>
    public class BinaryStreamsTest
    {
        /// <summary>
        /// Tests the platform memory stream.
        /// </summary>
        [Test]
        public void TestPlatformMemoryStream()
        {
            var stream = new PlatformMemoryStream(GetMemory());
            TestStream(stream, false, () => stream.SynchronizeOutput());
        }

        /// <summary>
        /// Tests the platform big endian memory stream.
        /// </summary>
        [Test]
        public void TestPlatformBigEndianMemoryStream()
        {
            var stream = new PlatformBigEndianMemoryStream(GetMemory());
            TestStream(stream, false, () => stream.SynchronizeOutput());
        }

        /// <summary>
        /// Tests the binary heap stream.
        /// </summary>
        [Test]
        public void TestBinaryHeapStream()
        {
            TestStream(new BinaryHeapStream(1), true, () => { });
        }

        /// <summary>
        /// Gets the memory.
        /// </summary>
        private static PlatformMemory GetMemory()
        {
            return new PlatformMemoryPool().Allocate(10);
        }

        /// <summary>
        /// Tests the stream.
        /// </summary>
        private static unsafe void TestStream(IBinaryStream stream, bool sameArr, Action flush)
        {
            Action seek = () => Assert.AreEqual(0, stream.Seek(0, SeekOrigin.Begin));

            Action<Action, Func<object>, object> check = (write, read, expectedResult) =>
            {
                seek();
                write();
                flush();
                seek();
                Assert.AreEqual(expectedResult, read());
            };

            // Arrays.
            Assert.AreEqual(sameArr, stream.IsSameArray(stream.GetArray()));
            Assert.IsFalse(stream.IsSameArray(new byte[1]));
            Assert.IsFalse(stream.IsSameArray(stream.GetArrayCopy()));

            // byte*
            byte* bytes = stackalloc byte[10];
            *bytes = 1;
            *(bytes + 1) = 2;

            stream.Write(bytes, 2);
            Assert.AreEqual(2, stream.Position);

            var proc = new SumStreamProcessor();
            Assert.AreEqual(0, stream.Apply(proc, 0));
            Assert.AreEqual(1, stream.Apply(proc, 1));
            Assert.AreEqual(3, stream.Apply(proc, 2));

            flush();

            seek();
            Assert.AreEqual(sameArr ? 256 : 2, stream.Remaining);
            byte* bytes2 = stackalloc byte[2];
            stream.Read(bytes2, 2);
            Assert.AreEqual(1, *bytes2);
            Assert.AreEqual(2, *(bytes2 + 1));

            // char*
            seek();
            char* chars = stackalloc char[10];
            *chars = 'a';
            *(chars + 1) = 'b';

            Assert.AreEqual(2, stream.WriteString(chars, 2, 2, Encoding.ASCII));
            flush();

            seek();
            stream.Read(bytes2, 2);
            Assert.AreEqual('a', *bytes2);
            Assert.AreEqual('b', *(bytes2 + 1));

            // Others.
            check(() => stream.Write(new byte[] {3, 4, 5}, 1, 2), () => stream.ReadByteArray(2), new byte[] {4, 5});

            check(() => stream.WriteBool(true), () => stream.ReadBool(), true);
            check(() => stream.WriteBoolArray(new[] {true, false}), () => stream.ReadBoolArray(2), 
                new[] {true, false});

            check(() => stream.WriteByte(4), () => stream.ReadByte(), 4);
            check(() => stream.WriteByteArray(new byte[] {4, 5, 6}), () => stream.ReadByteArray(3), 
                new byte[] {4, 5, 6});

            check(() => stream.WriteChar('x'), () => stream.ReadChar(), 'x');
            check(() => stream.WriteCharArray(new[] {'a', 'b'}), () => stream.ReadCharArray(2), new[] {'a', 'b'});

            check(() => stream.WriteDouble(4), () => stream.ReadDouble(), 4d);
            check(() => stream.WriteDoubleArray(new[] {4d}), () => stream.ReadDoubleArray(1), new[] {4d});

            check(() => stream.WriteFloat(4), () => stream.ReadFloat(), 4f);
            check(() => stream.WriteFloatArray(new[] {4f}), () => stream.ReadFloatArray(1), new[] {4f});

            check(() => stream.WriteInt(4), () => stream.ReadInt(), 4);
            check(() => stream.WriteInt(0, 4), () => stream.ReadInt(), 4);
            check(() => stream.WriteIntArray(new[] {4}), () => stream.ReadIntArray(1), new[] {4});

            check(() => stream.WriteLong(4), () => stream.ReadLong(), 4L);
            check(() => stream.WriteLongArray(new[] {4L}), () => stream.ReadLongArray(1), new[] {4L});

            check(() => stream.WriteShort(4), () => stream.ReadShort(), (short)4);
            check(() => stream.WriteShortArray(new short[] {4}), () => stream.ReadShortArray(1), new short[] {4});
        }

        private class SumStreamProcessor : IBinaryStreamProcessor<int, int>
        {
            public unsafe int Invoke(byte* data, int arg)
            {
                int res = 0;

                for (var i = 0; i < arg; i++)
                    res += *(data + i);

                return res;
            }
        }
    }
}
