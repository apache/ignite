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
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Socket wrapper with failover functionality: reconnects on failure.
    /// </summary>
    internal class ClientFailoverSocket : IClientSocket
    {
        /** Underlying socket. */
        private ClientSocket _socket;

        /** Current endpoint index. */
        private int _endPointIndex;

        /** Config. */
        private readonly IgniteClientConfiguration _config;

        /** Endpoints. */
        private readonly List<EndPoint> _endPoints;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Disposed flag. */
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        public ClientFailoverSocket(IgniteClientConfiguration config)
        {
            Debug.Assert(config != null);
            
            _config = config;
            _endPoints = GetEndPoints(config);

            // Choose random endpoint for load balancing.
            _endPointIndex = new Random().Next(_endPoints.Count - 1);

            Connect();
        }

        /** <inheritdoc /> */
        public T DoOutInOp<T>(ClientOp opId, Action<IBinaryStream> writeAction, Func<IBinaryStream, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return GetSocket().DoOutInOp(opId, writeAction, readFunc, errorFunc);
        }

        /** <inheritdoc /> */
        public Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<IBinaryStream> writeAction, Func<IBinaryStream, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return GetSocket().DoOutInOpAsync(opId, writeAction, readFunc, errorFunc);
        }

        /** <inheritdoc /> */
        public EndPoint CurrentEndPoint { get { return _socket.CurrentEndPoint; } }

        /// <summary>
        /// Checks the disposed state.
        /// </summary>
        private ClientSocket GetSocket()
        {
            lock (_syncRoot)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException("ClientFailoverSocket");
                }

                if (_socket == null)
                {
                    Connect();
                }

                return _socket;
            }
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (_syncRoot)
            {
                _disposed = true;
                _socket.Dispose();
                _socket = null;
            }
        }

        /// <summary>
        /// Connects the socket.
        /// </summary>
        private void Connect()
        {
            List<Exception> errors = null;

            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (_endPointIndex + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

                try
                {
                    _socket = new ClientSocket(_config, endPoint, OnSocketError);
                    _endPointIndex = idx;

                    return;
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

            throw new AggregateException("Failed to establish Ignite thin client connection, " +
                                         "examine inner exceptions for details.", errors);
        }

        /// <summary>
        /// Called when socket error occurs.
        /// </summary>
        private void OnSocketError()
        {
            if (_config.ReconnectDisabled)
            {
                return;
            }

            // Reconnect on next operation.
            lock (_syncRoot)
            {
                _endPointIndex++;
                _socket = null;
            }
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private static List<EndPoint> GetEndPoints(IgniteClientConfiguration cfg)
        {
            var host = cfg.Host;

            if (host == null && cfg.EndPoints.Count == 0)
            {
                throw new IgniteException("IgniteClientConfiguration does not contain any endpoints: " +
                                          "Host is null, EndPoints is empty.");
            }

            var res = new List<EndPoint>(cfg.EndPoints.Count + (host == null ? 0 : 4));

            if (host != null)
            {
                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                IPAddress ip;

                if (IPAddress.TryParse(host, out ip))
                {
                    res.Add(new IPEndPoint(ip, cfg.Port));
                }
                else
                {
                    foreach (var x in Dns.GetHostEntry(host).AddressList)
                    {
                        res.Add(new IPEndPoint(x, cfg.Port));
                    }
                }
            }

            res.AddRange(cfg.EndPoints);

            if (res.Count == 0)
            {
                throw new IgniteException("Failed to resolve client host: " + host);
            }

            return res;
        }
    }
}
