﻿/*
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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    /// </summary>
    internal sealed class ClientSocket : IDisposable
    {
        /** Version 1.0.0. */
        private static readonly ClientProtocolVersion Ver100 = new ClientProtocolVersion(1, 0, 0);

        /** Version 1.1.0. */
        private static readonly ClientProtocolVersion Ver110 = new ClientProtocolVersion(1, 1, 0);

        /** Current version. */
        private static readonly ClientProtocolVersion CurrentProtocolVersion = Ver110;

        /** Handshake opcode. */
        private const byte OpHandshake = 1;

        /** Client type code. */
        private const byte ClientType = 2;

        /** Underlying socket. */
        private readonly Socket _socket;

        /** Underlying socket stream. */
        private readonly Stream _stream;

        /** Operation timeout. */
        private readonly TimeSpan _timeout;

        /** Request timeout checker. */
        private readonly Timer _timeoutCheckTimer;

        /** Callback checker guard. */
        private volatile bool _checkingTimeouts;

        /** Current async operations, map from request id. */
        private readonly ConcurrentDictionary<long, Request> _requests
            = new ConcurrentDictionary<long, Request>();

        /** Request id generator. */
        private long _requestId;

        /** Socket failure exception. */
        private volatile Exception _exception;

        /** Locker. */
        private readonly object _sendRequestSyncRoot = new object();

        /** Background socket receiver trigger. */
        private readonly ManualResetEventSlim _listenerEvent = new ManualResetEventSlim();

        /** Dispose locker. */
        private readonly object _disposeSyncRoot = new object();

        /** Disposed flag. */
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket" /> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        /// <param name="version">Protocol version.</param>
        public ClientSocket(IgniteClientConfiguration clientConfiguration, ClientProtocolVersion? version = null)
        {
            Debug.Assert(clientConfiguration != null);

            _timeout = clientConfiguration.SocketTimeout;

            _socket = Connect(clientConfiguration);
            _stream = GetSocketStream(_socket, clientConfiguration);

            Validate(clientConfiguration);

            Handshake(clientConfiguration, version ?? CurrentProtocolVersion);

            // Check periodically if any request has timed out.
            if (_timeout > TimeSpan.Zero)
            {
                // Minimum Socket timeout is 500ms.
                _timeoutCheckTimer = new Timer(CheckTimeouts, null, _timeout, TimeSpan.FromMilliseconds(500));
            }

            // Continuously and asynchronously wait for data from server.
            TaskRunner.Run(WaitForMessages);
        }

        /// <summary>
        /// Validate configuration.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        private static void Validate(IgniteClientConfiguration cfg)
        {
            if (cfg.UserName != null)
            {
                if (cfg.UserName.Length == 0)
                    throw new IgniteClientException("IgniteClientConfiguration.Username cannot be empty.");

                if (cfg.Password == null)
                    throw new IgniteClientException("IgniteClientConfiguration.Password cannot be null when Username is set.");
            }

            if (cfg.Password != null)
            {
                if (cfg.Password.Length == 0)
                    throw new IgniteClientException("IgniteClientConfiguration.Password cannot be empty.");

                if (cfg.UserName == null)
                    throw new IgniteClientException("IgniteClientConfiguration.UserName cannot be null when Password is set.");
            }
        }

        /// <summary>
        /// Performs a send-receive operation.
        /// </summary>
        public T DoOutInOp<T>(ClientOp opId, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            // Encode.
            var reqMsg = WriteMessage(writeAction, opId);
            
            // Send.
            var response = SendRequest(ref reqMsg);

            // Decode.
            return DecodeResponse(response, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs a send-receive operation asynchronously.
        /// </summary>
        public Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            // Encode.
            var reqMsg = WriteMessage(writeAction, opId);

            // Send.
            var task = SendRequestAsync(ref reqMsg);

            // Decode.
            return task.ContWith(responseTask => DecodeResponse(responseTask.Result, readFunc, errorFunc));
        }

        /// <summary>
        /// Starts waiting for the new message.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void WaitForMessages()
        {
            try
            {
                // Null exception means active socket.
                while (_exception == null)
                {
                    // Do not call Receive if there are no async requests pending.
                    while (_requests.IsEmpty)
                    {
                        // Wait with a timeout so we check for disposed state periodically.
                        _listenerEvent.Wait(1000);

                        if (_exception != null)
                        {
                            return;
                        }

                        _listenerEvent.Reset();
                    }

                    var msg = ReceiveMessage();
                    HandleResponse(msg);
                }
            }
            catch (Exception ex)
            {
                // Socket failure (connection dropped, etc).
                // Close socket and all pending requests.
                // Note that this does not include request decoding exceptions (failed request, invalid data, etc).
                _exception = ex;
                Dispose();
            }
        }

        /// <summary>
        /// Handles the response.
        /// </summary>
        private void HandleResponse(byte[] response)
        {
            var stream = new BinaryHeapStream(response);
            var requestId = stream.ReadLong();

            Request req;
            if (!_requests.TryRemove(requestId, out req))
            {
                // Response with unknown id.
                throw new IgniteClientException("Invalid thin client response id: " + requestId);
            }

            req.CompletionSource.TrySetResult(stream);
        }

        /// <summary>
        /// Decodes the response that we got from <see cref="HandleResponse"/>.
        /// </summary>
        private static T DecodeResponse<T>(BinaryHeapStream stream, Func<IBinaryStream, T> readFunc, 
            Func<ClientStatusCode, string, T> errorFunc)
        {
            var statusCode = (ClientStatusCode)stream.ReadInt();

            if (statusCode == ClientStatusCode.Success)
            {
                return readFunc != null ? readFunc(stream) : default(T);
            }

            var msg = BinaryUtils.Marshaller.StartUnmarshal(stream).ReadString();

            if (errorFunc != null)
            {
                return errorFunc(statusCode, msg);
            }

            throw new IgniteClientException(msg, null, statusCode);
        }

        /// <summary>
        /// Performs client protocol handshake.
        /// </summary>
        private void Handshake(IgniteClientConfiguration clientConfiguration, ClientProtocolVersion version)
        {
            bool auth = version.CompareTo(Ver110) >= 0 && clientConfiguration.UserName != null;

            // Send request.
            int messageLen;
            var buf = WriteMessage(stream =>
            {
                // Handshake.
                stream.WriteByte(OpHandshake);

                // Protocol version.
                stream.WriteShort(version.Major);
                stream.WriteShort(version.Minor);
                stream.WriteShort(version.Maintenance);

                // Client type: platform.
                stream.WriteByte(ClientType);

                // Authentication data.
                if (auth)
                {
                    var writer = BinaryUtils.Marshaller.StartMarshal(stream);

                    writer.WriteString(clientConfiguration.UserName);
                    writer.WriteString(clientConfiguration.Password);

                    BinaryUtils.Marshaller.FinishMarshal(writer);
                }
            }, 12, out messageLen);
            
            _stream.Write(buf, 0, messageLen);

            // Decode response.
            var res = ReceiveMessage();

            using (var stream = new BinaryHeapStream(res))
            {
                // Read input.
                var success = stream.ReadBool();

                if (success)
                {
                    return;
                }

                var serverVersion =
                    new ClientProtocolVersion(stream.ReadShort(), stream.ReadShort(), stream.ReadShort());

                var errMsg = BinaryUtils.Marshaller.Unmarshal<string>(stream);

                ClientStatusCode errCode = ClientStatusCode.Fail;

                if (stream.Remaining > 0)
                {
                    errCode = (ClientStatusCode) stream.ReadInt();
                }

                // Authentication error is handled immediately.
                if (errCode == ClientStatusCode.AuthenticationFailed)
                {
                    throw new IgniteClientException(errMsg, null, ClientStatusCode.AuthenticationFailed);
                }

                // Re-try if possible.
                bool retry = serverVersion.CompareTo(version) < 0 && serverVersion.Equals(Ver100);

                if (retry)
                {
                    Handshake(clientConfiguration, serverVersion);
                }
                else
                {
                    throw new IgniteClientException(string.Format(
                        "Client handshake failed: '{0}'. Client version: {1}. Server version: {2}",
                        errMsg, version, serverVersion), null, errCode);
                }
            }
        }

        /// <summary>
        /// Receives a message from socket.
        /// </summary>
        private byte[] ReceiveMessage()
        {
            var size = GetInt(ReceiveBytes(4));
            var msg = ReceiveBytes(size);
            return msg;
        }

        /// <summary>
        /// Receives the data filling provided buffer entirely.
        /// </summary>
        private byte[] ReceiveBytes(int size)
        {
            Debug.Assert(size > 0);

            // Socket.Receive can return any number of bytes, even 1.
            // We should repeat Receive calls until required amount of data has been received.
            var buf = new byte[size];
            var received = _stream.Read(buf,0, size);

            while (received < size)
            {
                var res = _stream.Read(buf, received, size - received);

                if (res == 0)
                {
                    // Disconnected.
                    _exception = _exception ?? new SocketException((int) SocketError.ConnectionAborted);
                    Dispose();
                    CheckException();
                }

                received += res;
            }

            return buf;
        }

        /// <summary>
        /// Sends the request synchronously.
        /// </summary>
        private BinaryHeapStream SendRequest(ref RequestMessage reqMsg)
        {
            // Do not enter lock when disposed.
            CheckException();

            // If there are no pending async requests, we can execute this operation synchronously,
            // which is more efficient.
            var lockTaken = false;
            try
            {
                Monitor.TryEnter(_sendRequestSyncRoot, 0, ref lockTaken);
                if (lockTaken)
                {
                    CheckException();

                    if (_requests.IsEmpty)
                    {
                        _stream.Write(reqMsg.Buffer, 0, reqMsg.Length);

                        var respMsg = ReceiveMessage();
                        var response = new BinaryHeapStream(respMsg);
                        var responseId = response.ReadLong();
                        Debug.Assert(responseId == reqMsg.Id);

                        return response;
                    }
                }
            }
            finally
            {
                if (lockTaken)
                {
                    Monitor.Exit(_sendRequestSyncRoot);
                }
            }

            // Fallback to async mechanism.
            return SendRequestAsync(ref reqMsg).Result;
        }

        /// <summary>
        /// Sends the request asynchronously and returns a task for corresponding response.
        /// </summary>
        private Task<BinaryHeapStream> SendRequestAsync(ref RequestMessage reqMsg)
        {
            // Do not enter lock when disposed.
            CheckException();

            lock (_sendRequestSyncRoot)
            {
                CheckException();

                // Register.
                var req = new Request();
                var added = _requests.TryAdd(reqMsg.Id, req);
                Debug.Assert(added);

                // Send.
                _stream.Write(reqMsg.Buffer, 0, reqMsg.Length);
                _listenerEvent.Set();
                return req.CompletionSource.Task;
            }
        }

        /// <summary>
        /// Writes the message to a byte array.
        /// </summary>
        private static byte[] WriteMessage(Action<IBinaryStream> writeAction, int bufSize, out int messageLen)
        {
            var stream = new BinaryHeapStream(bufSize);

            stream.WriteInt(0); // Reserve message size.
            writeAction(stream);
            stream.WriteInt(0, stream.Position - 4); // Write message size.

            messageLen = stream.Position;
            return stream.GetArray();
        }

        /// <summary>
        /// Writes the message to a byte array.
        /// </summary>
        private RequestMessage WriteMessage(Action<IBinaryStream> writeAction, ClientOp opId)
        {
            var requestId = Interlocked.Increment(ref _requestId);
            var stream = new BinaryHeapStream(256);

            stream.WriteInt(0); // Reserve message size.
            stream.WriteShort((short) opId);
            stream.WriteLong(requestId);
            writeAction(stream);
            stream.WriteInt(0, stream.Position - 4); // Write message size.

            return new RequestMessage(requestId, stream.GetArray(), stream.Position);
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
                        NoDelay = cfg.TcpNoDelay,
                        Blocking = true,
                        SendTimeout = (int) cfg.SocketTimeout.TotalMilliseconds,
                        ReceiveTimeout = (int) cfg.SocketTimeout.TotalMilliseconds
                    };

                    if (cfg.SocketSendBufferSize != IgniteClientConfiguration.DefaultSocketBufferSize)
                    {
                        socket.SendBufferSize = cfg.SocketSendBufferSize;
                    }

                    if (cfg.SocketReceiveBufferSize != IgniteClientConfiguration.DefaultSocketBufferSize)
                    {
                        socket.ReceiveBufferSize = cfg.SocketReceiveBufferSize;
                    }

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
        /// Gets the socket stream.
        /// </summary>
        private static Stream GetSocketStream(Socket socket, IgniteClientConfiguration cfg)
        {
            var stream = new NetworkStream(socket)
            {
                ReadTimeout = (int) cfg.SocketTimeout.TotalMilliseconds,
                WriteTimeout = (int) cfg.SocketTimeout.TotalMilliseconds
            };

            if (cfg.SslStreamFactory == null)
            {
                return stream;
            }

            return cfg.SslStreamFactory.Create(stream, cfg.Host);
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
        /// Checks if any of the current requests timed out.
        /// </summary>
        private void CheckTimeouts(object _)
        {
            if (_checkingTimeouts)
            {
                return;
            }

            _checkingTimeouts = true;

            try
            {
                if (_exception != null)
                {
                    _timeoutCheckTimer.Dispose();
                }

                foreach (var pair in _requests)
                {
                    var req = pair.Value;

                    if (req.Duration > _timeout)
                    {
                        Console.WriteLine(req.Duration);
                        req.CompletionSource.TrySetException(new SocketException((int)SocketError.TimedOut));

                        _requests.TryRemove(pair.Key, out req);
                    }
                }
            }
            finally
            {
                _checkingTimeouts = false;
            }
        }

        /// <summary>
        /// Gets the int from buffer.
        /// </summary>
        private static unsafe int GetInt(byte[] buf)
        {
            fixed (byte* b = buf)
            {
                return BinaryHeapStream.ReadInt0(b);
            }
        }

        /// <summary>
        /// Checks the exception.
        /// </summary>
        private void CheckException()
        {
            var ex = _exception;

            if (ex != null)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Closes the socket and completes all pending requests with an error.
        /// </summary>
        private void EndRequestsWithError()
        {
            var ex = _exception;
            Debug.Assert(ex != null);

            while (!_requests.IsEmpty)
            {
                foreach (var reqId in _requests.Keys.ToArray())
                {
                    Request req;
                    if (_requests.TryRemove(reqId, out req))
                    {
                        req.CompletionSource.TrySetException(ex);
                    }
                }
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            lock (_disposeSyncRoot)
            {
                if (_isDisposed)
                {
                    return;
                }

                _exception = _exception ?? new ObjectDisposedException(typeof(ClientSocket).FullName);
                EndRequestsWithError();
                _stream.Dispose();
                _socket.Dispose();
                _listenerEvent.Set();
                _listenerEvent.Dispose();

                if (_timeoutCheckTimer != null)
                {
                    _timeoutCheckTimer.Dispose();
                }

                _isDisposed = true;
            }
        }

        /// <summary>
        /// Represents a request.
        /// </summary>
        private class Request
        {
            /** */
            private readonly TaskCompletionSource<BinaryHeapStream> _completionSource;

            /** */
            private readonly DateTime _startTime;

            /// <summary>
            /// Initializes a new instance of the <see cref="Request"/> class.
            /// </summary>
            public Request()
            {
                _completionSource = new TaskCompletionSource<BinaryHeapStream>();
                _startTime = DateTime.Now;
            }

            /// <summary>
            /// Gets the completion source.
            /// </summary>
            public TaskCompletionSource<BinaryHeapStream> CompletionSource
            {
                get { return _completionSource; }
            }

            /// <summary>
            /// Gets the duration.
            /// </summary>
            public TimeSpan Duration
            {
                get { return DateTime.Now - _startTime; }
            }
        }

        /// <summary>
        /// Represents a request message.
        /// </summary>
        private struct RequestMessage
        {
            /** */
            public readonly long Id;

            /** */
            public readonly byte[] Buffer;

            /** */
            public readonly int Length;

            /// <summary>
            /// Initializes a new instance of the <see cref="RequestMessage"/> struct.
            /// </summary>
            public RequestMessage(long id, byte[] buffer, int length)
            {
                Id = id;
                Length = length;
                Buffer = buffer;
            }
        }
    }
}
