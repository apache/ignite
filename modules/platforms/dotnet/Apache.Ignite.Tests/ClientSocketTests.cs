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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Net;
    using System.Text;
    using System.Threading.Tasks;
    using Internal;
    using Internal.Buffers;
    using Internal.Proto;
    using MessagePack;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientSocket"/>.
    /// </summary>
    public class ClientSocketTests : IgniteTestsBase
    {
        [Test]
        public async Task TestConnectAndSendRequestReturnsResponse()
        {
            using var socket = await ClientSocket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, ServerPort));

            using var requestWriter = new PooledArrayBufferWriter();

            WriteString(requestWriter.GetMessageWriter(), "non-existent-table");

            using var response = await socket.DoOutInOpAsync(ClientOp.TableGet, requestWriter);
            Assert.IsTrue(response.GetReader().IsNil);

            void WriteString(MessagePackWriter writer, string str)
            {
                writer.WriteString(Encoding.UTF8.GetBytes(str));
                writer.Flush();
            }
        }

        [Test]
        public async Task TestConnectAndSendRequestWithInvalidOpCodeThrowsError()
        {
            using var socket = await ClientSocket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, ServerPort));

            using var requestWriter = new PooledArrayBufferWriter();
            requestWriter.GetMessageWriter().Write(123);

            var ex = Assert.ThrowsAsync<IgniteClientException>(
                async () => await socket.DoOutInOpAsync((ClientOp)1234567, requestWriter));

            Assert.AreEqual("Unexpected operation code: 1234567", ex!.Message);
        }

        [Test]
        public async Task TestDisposedSocketThrowsExceptionOnSend()
        {
            var socket = await ClientSocket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, ServerPort));

            socket.Dispose();

            using var requestWriter = new PooledArrayBufferWriter();
            requestWriter.GetMessageWriter().Write(123);

            Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await socket.DoOutInOpAsync(ClientOp.SchemasGet, requestWriter));

            // Multiple dispose is allowed.
            socket.Dispose();
        }

        [Test]
        public void TestConnectWithoutServerThrowsException()
        {
            Assert.CatchAsync(async () => await ClientSocket.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 569)));
        }
    }
}
