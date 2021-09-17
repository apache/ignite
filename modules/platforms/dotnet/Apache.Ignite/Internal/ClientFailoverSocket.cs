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

namespace Apache.Ignite.Internal
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Buffers;
    using Log;
    using Proto;

    /// <summary>
    /// Client socket wrapper with reconnect/failover functionality.
    /// </summary>
    internal sealed class ClientFailoverSocket : IDisposable
    {
        /** Current global endpoint index for Round-robin. */
        private static long _endPointIndex;

        /** Logger. */
        private readonly IIgniteLogger? _logger;

        /** Endpoints with corresponding hosts - from configuration. */
        private readonly IReadOnlyList<SocketEndpoint> _endPoints;

        /** <see cref="_socket"/> lock. */
        [SuppressMessage(
            "Microsoft.Design",
            "CA2213:DisposableFieldsShouldBeDisposed",
            Justification = "WaitHandle is not used in SemaphoreSlim, no need to dispose.")]
        private readonly SemaphoreSlim _socketLock = new(1);

        /** Primary socket. */
        private ClientSocket? _socket;

        /** Disposed flag. */
        private volatile bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="configuration">Client configuration.</param>
        public ClientFailoverSocket(IgniteClientConfiguration configuration)
        {
            if (configuration.Endpoints.Count == 0)
            {
                throw new IgniteClientException(
                    $"Invalid {nameof(IgniteClientConfiguration)}: " +
                    $"{nameof(IgniteClientConfiguration.Endpoints)} is empty. Nowhere to connect.");
            }

            _logger = configuration.Logger;
            _endPoints = GetIpEndPoints(configuration).ToList();

            Configuration = new(configuration); // Defensive copy.
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        public IgniteClientConfiguration Configuration { get; }

        /// <summary>
        /// Connects the socket.
        /// </summary>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task ConnectAsync()
        {
            await GetSocketAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Performs an in-out operation.
        /// </summary>
        /// <param name="clientOp">Client op code.</param>
        /// <param name="request">Request data.</param>
        /// <returns>Response data.</returns>
        public async Task<PooledBuffer> DoOutInOpAsync(ClientOp clientOp, PooledArrayBufferWriter? request = null)
        {
            var socket = await GetSocketAsync().ConfigureAwait(false);

            return await socket.DoOutInOpAsync(clientOp, request).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _socketLock.Wait();

            try
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;

                _socket?.Dispose();
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Gets the primary socket. Reconnects if necessary.
        /// </summary>
        /// <returns>Client socket.</returns>
        private async ValueTask<ClientSocket> GetSocketAsync()
        {
            await _socketLock.WaitAsync().ConfigureAwait(false);

            try
            {
                ThrowIfDisposed();

                if (_socket == null || _socket.IsDisposed)
                {
                    _socket = await GetNextSocketAsync().ConfigureAwait(false);
                }

                return _socket;
            }
            finally
            {
                _socketLock.Release();
            }
        }

        /// <summary>
        /// Throws if disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(ClientFailoverSocket));
            }
        }

        /// <summary>
        /// Gets next connected socket, or connects a new one.
        /// </summary>
        private async ValueTask<ClientSocket> GetNextSocketAsync()
        {
            List<Exception>? errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (startIdx + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

                if (endPoint.Socket is { IsDisposed: false })
                {
                    return endPoint.Socket;
                }

                try
                {
                    return await ConnectAsync(endPoint).ConfigureAwait(false);
                }
                catch (SocketException e)
                {
                    errors ??= new List<Exception>();

                    errors.Add(e);
                }
            }

            throw new AggregateException(
                "Failed to establish Ignite thin client connection, examine inner exceptions for details.", errors);
        }

        /// <summary>
        /// Connects to the given endpoint.
        /// </summary>
        private async Task<ClientSocket> ConnectAsync(SocketEndpoint endPoint)
        {
            var socket = await ClientSocket.ConnectAsync(endPoint.EndPoint, _logger).ConfigureAwait(false);

            endPoint.Socket = socket;

            return socket;
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null, "host != null");  // Checked by GetEndpoints.

                for (var port = e.Port; port <= e.PortRange + e.Port; port++)
                {
                    foreach (var ip in GetIps(e.Host))
                    {
                        yield return new SocketEndpoint(new IPEndPoint(ip, port), e.Host);
                    }
                }
            }
        }

        /// <summary>
        /// Gets IP address list from a given host.
        /// When host is an IP already - parses it. Otherwise, resolves DNS name to IPs.
        /// </summary>
        private IEnumerable<IPAddress> GetIps(string host, bool suppressExceptions = false)
        {
            try
            {
                IPAddress ip;

                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                return IPAddress.TryParse(host, out ip) ? new[] {ip} : Dns.GetHostEntry(host).AddressList;
            }
            catch (SocketException e)
            {
                _logger?.Debug(e, "Failed to parse host: " + host);

                if (suppressExceptions)
                {
                    return Enumerable.Empty<IPAddress>();
                }

                throw;
            }
        }
    }
}
