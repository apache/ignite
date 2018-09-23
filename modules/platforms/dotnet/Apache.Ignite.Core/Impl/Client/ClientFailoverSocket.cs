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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Socket wrapper with failover functionality: reconnects on failure.
    /// </summary>
    internal class ClientFailoverSocket : IClientSocket
    {
        private static readonly string[] HostSeparators = {":"};
        private static readonly string[] PortsSeparators = {".."};

        /** Underlying socket. */
        private ClientSocket _socket;

        /** Current endpoint index. */
        // TODO: Have a global static starting index (take mod of it) so we have global round-robin
        private int _endPointIndex;

        /** Config. */
        private readonly IgniteClientConfiguration _config;

        /** Endpoints with corresponding hosts. */
        private readonly List<KeyValuePair<IPEndPoint, string>> _endPoints;

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

#pragma warning disable 618 // Type or member is obsolete
            if (config.Host == null && (config.Endpoints == null || config.Endpoints.Count == 0))
            {
                throw new IgniteClientException("Invalid IgniteClientConfiguration: Host is null, " +
                                                "Endpoints is null or empty. Nowhere to connect.");
            }
#pragma warning restore 618

            _endPoints = GetIpEndPoints(config).ToList();

            if (_endPoints.Count == 0)
            {
                throw new IgniteClientException("Failed to resolve all specified hosts.");
            }

            // Choose random endpoint for load balancing.
            // TODO: use global static counter
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
        public ClientProtocolVersion ServerVersion
        {
            get { return GetSocket().ServerVersion; }
        }

        /** <inheritdoc /> */
        public EndPoint RemoteEndPoint
        {
            get
            {
                lock (_syncRoot)
                {
                    return _socket != null ? _socket.RemoteEndPoint : null;
                }
            }
        }

        /** <inheritdoc /> */
        public EndPoint LocalEndPoint
        {
            get
            {
                lock (_syncRoot)
                {
                    return _socket != null ? _socket.LocalEndPoint : null;
                }
            }
        }

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
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            lock (_syncRoot)
            {
                _disposed = true;

                if (_socket != null)
                {
                    _socket.Dispose();
                    _socket = null;
                }
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
                    _socket = new ClientSocket(_config, endPoint.Key, endPoint.Value, OnSocketError);
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
        private static IEnumerable<KeyValuePair<IPEndPoint, string>> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null);  // Checked by GetEndpoints.

                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                IPAddress ip;

                if (IPAddress.TryParse(host, out ip))
                {
                    yield return new KeyValuePair<IPEndPoint, string>(new IPEndPoint(ip, e.Port), host);
                }
                else
                {
                    foreach (var x in Dns.GetHostEntry(host).AddressList)
                    {
                        yield return new KeyValuePair<IPEndPoint, string>(new IPEndPoint(x, e.Port), host);
                    }
                }
            }
        }

        /// <summary>
        /// Gets the client endpoints.
        /// </summary>
        private static IEnumerable<Endpoint> GetEndpoints(IgniteClientConfiguration cfg)
        {
#pragma warning disable 618 // Type or member is obsolete
            if (cfg.Host != null)
            {
                yield return new Endpoint(cfg.Host, cfg.Port);
            }
#pragma warning restore 618

            if (cfg.Endpoints != null)
            {
                foreach (var endpoint in cfg.Endpoints)
                {
                    yield return ParseEndpoint(endpoint);
                }
            }
        }

        private static Endpoint ParseEndpoint(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new IgniteClientException(
                    "IgniteClientConfiguration.Endpoints[...] can't be null or whitespace.");
            }

            var parts = endpoint.Split(HostSeparators, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length == 1)
            {
                return new Endpoint(endpoint);
            }

            if (parts.Length == 2)
            {
                var host = parts[0];
                var port = parts[1];

                var ports = port.Split(PortsSeparators, StringSplitOptions.RemoveEmptyEntries);

                if (ports.Length == 1)
                {
                    return new Endpoint(host, ParsePort(endpoint, port));
                }

                if (ports.Length == 2)
                {
                    var minPort = ParsePort(endpoint, ports[0]);
                    var maxPort = ParsePort(endpoint, ports[1]);

                    if (maxPort < minPort)
                    {
                        throw new IgniteClientException(
                            "Invalid format of IgniteClientConfiguration.Endpoint, port range is empty: " + endpoint);
                    }

                    return new Endpoint(host, minPort, maxPort - minPort);
                }
            }

            throw new IgniteClientException("Unrecognized format of IgniteClientConfiguration.Endpoint: " + endpoint);
        }

        private static int ParsePort(string endpoint, string portString)
        {
            int port;

            if (int.TryParse(portString, out port))
            {
                return port;
            }

            throw new IgniteClientException(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: " + endpoint);
        }
    }
}
