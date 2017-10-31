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

namespace Apache.Ignite.Core.Impl.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    /// </summary>
    internal class ClientSocket : IDisposable
    {
        /** Current version. */
        private static readonly ClientProtocolVersion CurrentProtocolVersion = new ClientProtocolVersion(1, 0, 0);

        /** Handshake opcode. */
        private const byte OpHandshake = 1;

        /** Client type code. */
        private const byte ClientType = 2;

        /** Unerlying socket. */
        private readonly Socket _socket;

        /** */
        private long _requestId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket" /> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        /// <param name="version">Protocol version.</param>
        public ClientSocket(IgniteClientConfiguration clientConfiguration, ClientProtocolVersion? version = null)
        {
            Debug.Assert(clientConfiguration != null);

            _socket = Connect(clientConfiguration);

            Handshake(_socket, version ?? CurrentProtocolVersion);
        }

        /// <summary>
        /// Performs a send-receive operation.
        /// </summary>
        public T DoOutInOp<T>(ClientOp opId, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc, Func<ClientStatus, string, T> errorFunc = null)
        {
            var requestId = Interlocked.Increment(ref _requestId);

            var resBytes = SendReceive(_socket, stream =>
            {
                stream.WriteShort((short) opId);
                stream.WriteLong(requestId);

                if (writeAction != null)
                {
                    writeAction(stream);
                }
            });

            using (var stream = new BinaryHeapStream(resBytes))
            {
                var resRequestId = stream.ReadLong();
                Debug.Assert(requestId == resRequestId);

                var statusCode = (ClientStatus) stream.ReadInt();

                if (statusCode == ClientStatus.Success)
                {
                    return readFunc != null ? readFunc(stream) : default(T);
                }

                var msg = BinaryUtils.Marshaller.StartUnmarshal(stream).ReadString();

                if (errorFunc != null)
                {
                    return errorFunc(statusCode, msg);
                }

                throw new IgniteClientException(msg, null, (int) statusCode);
            }
        }

        /// <summary>
        /// Performs client protocol handshake.
        /// </summary>
        private static void Handshake(Socket sock, ClientProtocolVersion version)
        {
            var res = SendReceive(sock, stream =>
            {
                // Handshake.
                stream.WriteByte(OpHandshake);

                // Protocol version.
                stream.WriteShort(version.Major);
                stream.WriteShort(version.Minor);
                stream.WriteShort(version.Maintenance);

                // Client type: platform.
                stream.WriteByte(ClientType);
            }, 20);

            using (var stream = new BinaryHeapStream(res))
            {
                var success = stream.ReadBool();

                if (success)
                {
                    return;
                }

                var serverVersion =
                    new ClientProtocolVersion(stream.ReadShort(), stream.ReadShort(), stream.ReadShort());

                var errMsg = BinaryUtils.Marshaller.Unmarshal<string>(stream);

                throw new IgniteClientException(string.Format(
                    "Client handhsake failed: '{0}'. Client version: {1}. Server version: {2}",
                    errMsg, version, serverVersion));
            }
        }

        /// <summary>
        /// Sends the request and receives a response.
        /// </summary>
        private static byte[] SendReceive(Socket sock, Action<IBinaryStream> writeAction, int bufSize = 128)
        {
            int messageLen;
            var buf = WriteMessage(writeAction, bufSize, out messageLen);

            lock (sock)
            {
                var sent = sock.Send(buf, messageLen, SocketFlags.None);
                Debug.Assert(sent == messageLen);

                buf = new byte[4];
                var received = sock.Receive(buf);
                Debug.Assert(received == buf.Length);

                using (var stream = new BinaryHeapStream(buf))
                {
                    var size = stream.ReadInt();
                    
                    buf = new byte[size];
                    received = sock.Receive(buf);

                    while (received < size)
                    {
                        received += sock.Receive(buf, received, size - received, SocketFlags.None);
                    }

                    return buf;
                }
            }
        }

        /// <summary>
        /// Writes the message to a byte array.
        /// </summary>
        private static byte[] WriteMessage(Action<IBinaryStream> writeAction, int bufSize, out int messageLen)
        {
            using (var stream = new BinaryHeapStream(bufSize))
            {
                stream.WriteInt(0); // Reserve message size.

                writeAction(stream);

                stream.WriteInt(0, stream.Position - 4); // Write message size.

                messageLen = stream.Position;

                return stream.GetArray();
            }
        }

        /// <summary>
        /// Connects the socket.
        /// </summary>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", 
            Justification = "Socket is returned from this method.")]
        private static Socket Connect(IgniteClientConfiguration cfg)
        {
            List<Exception> errors = null;

            foreach (var ipEndPoint in GetEndPoints(cfg))
            {
                try
                {
                    var socket = new Socket(ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
                    {
                        SendBufferSize = cfg.SocketSendBufferSize,
                        ReceiveBufferSize = cfg.SocketReceiveBufferSize,
                        NoDelay = cfg.TcpNoDelay
                    };

                    socket.Connect(ipEndPoint);

                    return socket;
                }
                catch (SocketException e)
                {
                    if (errors == null)
                    {
                        errors = new List<Exception>();
                    }

                    errors.Add(e);
                }
            }

            if (errors == null)
            {
                throw new IgniteException("Failed to resolve client host: " + cfg.Host);
            }

            throw new AggregateException("Failed to establish Ignite thin client connection, " +
                                         "examine inner exceptions for details.", errors);
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private static IEnumerable<IPEndPoint> GetEndPoints(IgniteClientConfiguration cfg)
        {
            var host = cfg.Host;

            if (host == null)
            {
                throw new IgniteException("IgniteClientConfiguration.Host cannot be null.");
            }

            // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
            IPAddress ip;

            if (IPAddress.TryParse(host, out ip))
            {
                return new[] {new IPEndPoint(ip, cfg.Port)};
            }

            return Dns.GetHostEntry(host).AddressList.Select(x => new IPEndPoint(x, cfg.Port));
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _socket.Dispose();
        }
    }
}
