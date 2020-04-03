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
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Log;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Wrapper over framework socket for Ignite thin client operations.
    /// </summary>
    internal sealed class ClientSocket : IDisposable
    {
        /** Version 1.0.0. */
        public static readonly ClientProtocolVersion Ver100 = new ClientProtocolVersion(1, 0, 0);

        /** Version 1.1.0. */
        public static readonly ClientProtocolVersion Ver110 = new ClientProtocolVersion(1, 1, 0);

        /** Version 1.2.0. */
        public static readonly ClientProtocolVersion Ver120 = new ClientProtocolVersion(1, 2, 0);

        /** Version 1.3.0. */
        public static readonly ClientProtocolVersion Ver130 = new ClientProtocolVersion(1, 3, 0);

        /** Version 1.4.0. */
        public static readonly ClientProtocolVersion Ver140 = new ClientProtocolVersion(1, 4, 0);

        /** Version 1.5.0. */
        // This version is reserved for IEP-34 Thin client: transactions support
        // ReSharper disable once UnusedMember.Global
        public static readonly ClientProtocolVersion Ver150 = new ClientProtocolVersion(1, 5, 0);

        /** Version 1.6.0. */
        public static readonly ClientProtocolVersion Ver160 = new ClientProtocolVersion(1, 6, 0);

        /** Version 1.7.0. */
        public static readonly ClientProtocolVersion Ver170 = new ClientProtocolVersion(1, 7, 0);

        /** Current version. */
        public static readonly ClientProtocolVersion CurrentProtocolVersion = Ver170;

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

        /** Server protocol version. */
        public ClientProtocolVersion ServerVersion { get; private set; }

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

        /** Topology version update callback. */
        private readonly Action<AffinityTopologyVersion> _topVerCallback;

        /** Logger. */
        private readonly ILogger _logger;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSocket" /> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        /// <param name="endPoint">The end point to connect to.</param>
        /// <param name="host">The host name (required for SSL).</param>
        /// <param name="version">Protocol version.</param>
        /// <param name="topVerCallback">Topology version update callback.</param>
        /// <param name="marshaller">Marshaller.</param>
        public ClientSocket(IgniteClientConfiguration clientConfiguration, EndPoint endPoint, string host,
            ClientProtocolVersion? version, Action<AffinityTopologyVersion> topVerCallback,
            Marshaller marshaller)
        {
            Debug.Assert(clientConfiguration != null);
            Debug.Assert(endPoint != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(host));
            Debug.Assert(topVerCallback != null);
            Debug.Assert(marshaller != null);

            _topVerCallback = topVerCallback;
            _marsh = marshaller;
            _timeout = clientConfiguration.SocketTimeout;
            _logger = (clientConfiguration.Logger ?? NoopLogger.Instance).GetLogger(GetType());

            _socket = Connect(clientConfiguration, endPoint, _logger);
            _stream = GetSocketStream(_socket, clientConfiguration, host);

            ServerVersion = version ?? CurrentProtocolVersion;

            Validate(clientConfiguration);

            Handshake(clientConfiguration, ServerVersion);

            // Check periodically if any request has timed out.
            if (_timeout > TimeSpan.Zero)
            {
                // Minimum Socket timeout is 500ms.
                _timeoutCheckTimer = new Timer(CheckTimeouts, null, _timeout, TimeSpan.FromMilliseconds(500));
            }

            // Continuously and asynchronously wait for data from server.
            // TaskCreationOptions.LongRunning actually means a new thread.
            TaskRunner.Run(WaitForMessages, TaskCreationOptions.LongRunning);
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
        public T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
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
        public Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            // Encode.
            var reqMsg = WriteMessage(writeAction, opId);

            // Send.
            var task = SendRequestAsync(ref reqMsg);

            // Decode.
            // NOTE: ContWith explicitly uses TaskScheduler.Default,
            // which runs DecodeResponse (and any user continuations) on a thread pool thread,
            // so that WaitForMessages thread does not do anything except reading from the socket.
            return task.ContWith(responseTask => DecodeResponse(responseTask.Result, readFunc, errorFunc));
        }

        /// <summary>
        /// Gets the current remote EndPoint.
        /// </summary>
        public EndPoint RemoteEndPoint { get { return _socket.RemoteEndPoint; } }

        /// <summary>
        /// Gets the current local EndPoint.
        /// </summary>
        public EndPoint LocalEndPoint { get { return _socket.LocalEndPoint; } }

        /// <summary>
        /// Gets the ID of the connected server node.
        /// </summary>
        public Guid? ServerNodeId { get; private set; }

        /// <summary>
        /// Gets a value indicating whether this socket is disposed.
        /// </summary>
        public bool IsDisposed
        {
            get { return _isDisposed; }
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
                if (_exception != null)
                {
                    return;
                }

                // Response with unknown id.
                throw new IgniteClientException("Invalid thin client response id: " + requestId);
            }

            if (req != null)
            {
                req.CompletionSource.TrySetResult(stream);
            }
        }

        /// <summary>
        /// Decodes the response that we got from <see cref="HandleResponse"/>.
        /// </summary>
        private T DecodeResponse<T>(BinaryHeapStream stream, Func<ClientResponseContext, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc)
        {
            ClientStatusCode statusCode;

            if (ServerVersion >= Ver140)
            {
                var flags = (ClientFlags) stream.ReadShort();

                if ((flags & ClientFlags.AffinityTopologyChanged) == ClientFlags.AffinityTopologyChanged)
                {
                    var topVer = new AffinityTopologyVersion(stream.ReadLong(), stream.ReadInt());
                    if (_topVerCallback != null)
                    {
                        _topVerCallback(topVer);
                    }
                }

                statusCode = (flags & ClientFlags.Error) == ClientFlags.Error
                    ? (ClientStatusCode) stream.ReadInt()
                    : ClientStatusCode.Success;
            }
            else
            {
                statusCode = (ClientStatusCode) stream.ReadInt();
            }

            if (statusCode == ClientStatusCode.Success)
            {
                return readFunc != null 
                    ? readFunc(new ClientResponseContext(stream, _marsh, ServerVersion)) 
                    : default(T);
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
            bool auth = version >= Ver110 && clientConfiguration.UserName != null;
            bool features = version >= Ver170;

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

                // Writing features.
                if (features)
                {
                    // TODO: Implement client-side features.
                    var featureBytes = new byte[0];

                    BinaryUtils.Marshaller.Marshal(stream, w => w.WriteByteArray(featureBytes));
                }

                // Authentication data.
                if (auth)
                {
                    BinaryUtils.Marshaller.Marshal(stream, writer =>
                    {
                        writer.WriteString(clientConfiguration.UserName);
                        writer.WriteString(clientConfiguration.Password);
                    });
                }
            }, 12, out messageLen);

            SocketWrite(buf, messageLen);

            // Decode response.
            var res = ReceiveMessage();

            using (var stream = new BinaryHeapStream(res))
            {
                // Read input.
                var success = stream.ReadBool();

                if (success)
                {
                    if (version >= Ver170)
                    {
                        // TODO: Implement features support
                        BinaryUtils.Marshaller.Unmarshal<byte[]>(stream);
                    }

                    if (version >= Ver140)
                    {
                        ServerNodeId = BinaryUtils.Marshaller.Unmarshal<Guid>(stream);
                    }

                    ServerVersion = version;
                    
                    _logger.Debug("Handshake completed on {0}, protocol version = {1}", 
                        _socket.RemoteEndPoint, version);

                    return;
                }

                ServerVersion =
                    new ClientProtocolVersion(stream.ReadShort(), stream.ReadShort(), stream.ReadShort());

                var errMsg = BinaryUtils.Marshaller.Unmarshal<string>(stream);

                ClientStatusCode errCode = ClientStatusCode.Fail;

                if (stream.Remaining > 0)
                {
                    errCode = (ClientStatusCode) stream.ReadInt();
                }

                _logger.Debug("Handshake failed on {0}, requested protocol version = {1}, " +
                              "server protocol version = {2}, status = {3}, message = {4}",
                    _socket.RemoteEndPoint, version, ServerVersion, errCode, errMsg);

                // Authentication error is handled immediately.
                if (errCode == ClientStatusCode.AuthenticationFailed)
                {
                    throw new IgniteClientException(errMsg, null, ClientStatusCode.AuthenticationFailed);
                }

                // Retry if server version is different and falls within supported version range.
                var retry = ServerVersion != version &&
                            ServerVersion >= Ver100 &&
                            ServerVersion <= CurrentProtocolVersion;

                if (retry)
                {
                    _logger.Debug("Retrying handshake on {0} with protocol version {1}", 
                        _socket.RemoteEndPoint, ServerVersion);
                    
                    Handshake(clientConfiguration, ServerVersion);
                }
                else
                {
                    throw new IgniteClientException(string.Format(
                        "Client handshake failed: '{0}'. Client version: {1}. Server version: {2}",
                        errMsg, version, ServerVersion), null, errCode);
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
            var received = SocketRead(buf, 0, size);

            while (received < size)
            {
                var res = SocketRead(buf, received, size - received);

                if (res == 0)
                {
                    // Disconnected.
                    _logger.Debug("Connection lost on {0} (failed to read data from socket)", _socket.RemoteEndPoint);
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
                        SocketWrite(reqMsg.Buffer, reqMsg.Length);

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
                SocketWrite(reqMsg.Buffer, reqMsg.Length);
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
        private RequestMessage WriteMessage(Action<ClientRequestContext> writeAction, ClientOp opId)
        {
            ClientUtils.ValidateOp(opId, ServerVersion);
            
            var requestId = Interlocked.Increment(ref _requestId);
            
            // Potential perf improvements:
            // * ArrayPool<T>
            // * Write to socket stream directly (not trivial because of unknown size) 
            var stream = new BinaryHeapStream(256);

            stream.WriteInt(0); // Reserve message size.
            stream.WriteShort((short) opId);
            stream.WriteLong(requestId);

            if (writeAction != null)
            {
                var ctx = new ClientRequestContext(stream, _marsh, ServerVersion);
                writeAction(ctx);
                ctx.FinishMarshal();
            }

            stream.WriteInt(0, stream.Position - 4); // Write message size.

            return new RequestMessage(requestId, stream.GetArray(), stream.Position);
        }

        /// <summary>
        /// Writes to the socket. All socket writes should go through this method.
        /// </summary>
        private void SocketWrite(byte[] buf, int len)
        {
            try
            {
                _stream.Write(buf, 0, len);
            }
            catch (Exception e)
            {
                _exception = e;
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Reads from the socket. All socket reads should go through this method.
        /// </summary>
        private int SocketRead(byte[] buf, int pos, int len)
        {
            try
            {
                return _stream.Read(buf, pos, len);
            }
            catch (Exception e)
            {
                _exception = e;
                Dispose();
                throw;
            }
        }

        /// <summary>
        /// Connects the socket.
        /// </summary>
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Socket is returned from this method.")]
        private static Socket Connect(IgniteClientConfiguration cfg, EndPoint endPoint, ILogger logger)
        {
            var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
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

            logger.Debug("Socket connection attempt: {0}", endPoint);

            socket.Connect(endPoint);
            
            logger.Debug("Socket connection established: {0} -> {1}", socket.LocalEndPoint, socket.RemoteEndPoint);

            return socket;
        }

        /// <summary>
        /// Gets the socket stream.
        /// </summary>
        private static Stream GetSocketStream(Socket socket, IgniteClientConfiguration cfg, string host)
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

            return cfg.SslStreamFactory.Create(stream, host);
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

                    if (req != null && req.Duration > _timeout)
                    {
                        _requests[pair.Key] = null;

                        req.CompletionSource.TrySetException(new SocketException((int)SocketError.TimedOut));
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
                    if (_requests.TryRemove(reqId, out req) && req != null)
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
