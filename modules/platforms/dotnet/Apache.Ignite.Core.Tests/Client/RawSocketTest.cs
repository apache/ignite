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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests the thin client mode with a raw socket.
    /// </summary>
    public class RawSocketTest
    {
        /// <summary>
        /// Tests the socket handshake connection.
        /// </summary>
        [Test]
        public void TestHandshake()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SqlConnectorConfiguration = new SqlConnectorConfiguration()
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var marsh = ((Ignite) ignite).Marshaller;

                // Create cache.
                var cacheCfg = new CacheConfiguration("foo", new QueryEntity(typeof(int), typeof(string)));
                var cache = ignite.CreateCache<int, string>(cacheCfg);
                cache[1] = "bar";

                // Connect socket.
                var sock = GetSocket(SqlConnectorConfiguration.DefaultPort);
                Assert.IsTrue(sock.Connected);

                DoHandshake(sock);

                // Cache get.
                SendRequest(sock, stream =>
                {
                    stream.WriteByte(0); // Flags
                    stream.WriteShort(1);  // OP_GET
                    var cacheId = BinaryUtils.GetStringHashCode(cache.Name);
                    stream.WriteInt(cacheId);
                    stream.WriteByte(0);  // Flags (withSkipStore, etc)

                    var writer = marsh.StartMarshal(stream);

                    writer.WriteObject(1);  // Key
                }, 1);

                var msg = ReceiveMessage(sock, 1);
                
                using (var stream = new BinaryHeapStream(msg))
                {
                    var reader = marsh.StartUnmarshal(stream);

                    reader.ReadByte(); // Flags

                    var res = reader.ReadObject<string>();

                    Assert.AreEqual(cache[1], res);
                }
            }
        }

        /// <summary>
        /// Does the handshake.
        /// </summary>
        /// <param name="sock">The sock.</param>
        private static void DoHandshake(Socket sock)
        {
            var sentBytes = SendRequest(sock, stream =>
            {
                // Handshake.
                stream.WriteByte(1);

                // Protocol version.
                stream.WriteShort(2);
                stream.WriteShort(1);
                stream.WriteShort(0);

                // Client type: platform.
                stream.WriteByte(2);
            });

            Assert.AreEqual(12, sentBytes);

            // ACK.
            var ack = ReceiveMessage(sock);

            Assert.AreEqual(1, ack.Length);
            Assert.AreEqual(1, ack[0]);
        }

        /// <summary>
        /// Receives the message.
        /// </summary>
        private static byte[] ReceiveMessage(Socket sock, int? requestId = null)
        {
            var buf = new byte[requestId == null ? 4 : 8];
            sock.Receive(buf);

            using (var stream = new BinaryHeapStream(buf))
            {
                var size = stream.ReadInt();

                if (requestId != null)
                {
                    var id = stream.ReadInt();
                    Assert.AreEqual(requestId, id);
                    size -= 4;
                }

                buf = new byte[size];
                sock.Receive(buf);
                return buf;
            }
        }

        /// <summary>
        /// Sends the request.
        /// </summary>
        private static int SendRequest(Socket sock, Action<BinaryHeapStream> writeAction, int? requestId = null)
        {
            using (var stream = new BinaryHeapStream(128))
            {
                stream.WriteInt(0);  // Reserve message size.

                if (requestId != null)
                {
                    stream.WriteInt(requestId.Value);
                }

                writeAction(stream);

                stream.WriteInt(0, stream.Position - 4);  // Write message size.

                return sock.Send(stream.GetArray(), stream.Position, SocketFlags.None);
            }
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        private static Socket GetSocket(int port)
        {
            var endPoint = new IPEndPoint(IPAddress.Loopback, port);
            var sock = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(endPoint);
            return sock;
        }
    }
}
