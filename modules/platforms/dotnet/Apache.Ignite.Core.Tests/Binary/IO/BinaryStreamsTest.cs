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

            // Arrays.
            Assert.AreEqual(sameArr, stream.IsSameArray(stream.GetArray()));
            Assert.IsFalse(stream.IsSameArray(new byte[1]));
            Assert.IsFalse(stream.IsSameArray(stream.GetArrayCopy()));

            // byte*
            byte* bytes = stackalloc byte[10];
            *bytes = 1;
            *(bytes + 1) = 2;

            stream.Write(bytes, 2);
            flush();

            seek();
            byte* bytes2 = stackalloc byte[2];
            stream.Read(bytes2, 2);
            Assert.AreEqual(1, *bytes2);
            Assert.AreEqual(2, *(bytes2 + 1));

            // byte[]
            seek();
            stream.Write(new byte[] {3, 4, 5}, 1, 2);
            flush();
            seek();
            Assert.AreEqual(new byte[] {4,5}, stream.ReadByteArray(2));

        }
    }
}
