/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests.Buffers
{
    using Internal.Buffers;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="PooledArrayBufferWriter"/>.
    /// </summary>
    public class PooledArrayBufferWriterTests
    {
        [Test]
        public void TestBufferWriterPrependsMessageLength()
        {
            using var bufferWriter = new PooledArrayBufferWriter();

            // With payload.
            var writer = bufferWriter.GetMessageWriter();

            writer.Write(1);
            writer.Write("A");
            writer.Flush();

            var res = bufferWriter.GetWrittenMemory().ToArray();

            var expectedBytes = new byte[]
            {
                // 4 bytes BE length + 1 byte int + 1 byte fixstr + 1 byte char.
                0, 0, 0, 3, 1, 0xa1, (byte)'A'
            };

            CollectionAssert.AreEqual(expectedBytes, res);
        }

        [Test]
        public void TestBufferWriterPrependsPrefixAndMessageLength()
        {
            using var bufferWriter = new PooledArrayBufferWriter();

            var writer = bufferWriter.GetMessageWriter();
            writer.Write(1);
            writer.Write("A");
            writer.Flush();

            var prefixWriter = bufferWriter.GetPrefixWriter(3);
            prefixWriter.Write(7);
            prefixWriter.Write(8);
            prefixWriter.Write(9);
            prefixWriter.Flush();

            var res = bufferWriter.GetWrittenMemory().ToArray();

            var expectedBytes = new byte[]
            {
                // 4 bytes BE length + 3 bytes prefix + 1 byte int + 1 byte fixstr + 1 byte char.
                0, 0, 0, 6, 7, 8, 9, 1, 0xa1, (byte)'A'
            };

            CollectionAssert.AreEqual(expectedBytes, res);
        }

        [Test]
        public void TestEmptyBufferWriterPrependsZeroMessageLength()
        {
            using var bufferWriter = new PooledArrayBufferWriter();

            var res = bufferWriter.GetWrittenMemory().ToArray();

            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 0 }, res);
        }

        [Test]
        public void TestEmptyBufferWriterPrependsPrefix()
        {
            using var bufferWriter = new PooledArrayBufferWriter();

            var writer = bufferWriter.GetPrefixWriter(2);
            writer.Write(7);
            writer.Write(8);
            writer.Flush();

            var res = bufferWriter.GetWrittenMemory().ToArray();

            CollectionAssert.AreEqual(new byte[] { 0, 0, 0, 2, 7, 8 }, res);
        }
    }
}
