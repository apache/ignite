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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Client.Cache;

    /// <summary>
    /// Socket wrapper with reconnect/failover functionality: reconnects on failure.
    /// </summary>
    internal class ClientFailoverSocket : IClientSocket
    {
        /** Underlying socket. */
        private ClientSocket _socket;

        /** Current global endpoint index for Round-robin. */
        private static long _endPointIndex;

        /** Config. */
        private readonly IgniteClientConfiguration _config;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Endpoints with corresponding hosts. */
        private readonly List<SocketEndpoint> _endPoints;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Disposed flag. */
        private bool _disposed;

        /** Current affinity topology version. */
        private AffinityTopologyVersion? _affinityTopologyVersion;

        /** Map from node ID to connected socket. */
        private volatile Dictionary<Guid, ClientSocket> _nodeSocketMap;

        /** Map from cache ID to partition mapping. */
        private volatile ClientCacheTopologyPartitionMap _distributionMap;

        /** Distribution map locker. */
        private readonly object _distributionMapSyncRoot = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientFailoverSocket"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="marsh"></param>
        public ClientFailoverSocket(IgniteClientConfiguration config, Marshaller marsh)
        {
            Debug.Assert(config != null);
            Debug.Assert(marsh != null);

            _config = config;
            _marsh = marsh;

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

            Connect();
        }

        /** <inheritdoc /> */
        public T DoOutInOp<T>(ClientOp opId, Action<IBinaryStream> writeAction, Func<IBinaryStream, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return GetSocket().DoOutInOp(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs a send-receive operation with affinity awareness.
        /// </summary>
        public T DoOutInOpAffinity<T, TKey>(
            ClientOp opId,
            Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc,
            int cacheId,
            TKey key,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            var socket = GetAffinitySocket(cacheId, key) ?? GetSocket();

            return socket.DoOutInOp(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs an async send-receive operation with affinity awareness.
        /// </summary>
        public Task<T> DoOutInOpAffinityAsync<T, TKey>(
            ClientOp opId,
            Action<IBinaryStream> writeAction,
            Func<IBinaryStream, T> readFunc,
            int cacheId,
            TKey key,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            var socket = GetAffinitySocket(cacheId, key) ?? GetSocket();

            return socket.DoOutInOpAsync(opId, writeAction, readFunc, errorFunc);
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
                ThrowIfDisposed();

                if (_socket == null || (_socket.IsDisposed && !_config.ReconnectDisabled))
                {
                    Connect();
                }

                return _socket;
            }
        }

        private ClientSocket GetAffinitySocket<TKey>(int cacheId, TKey key)
        {
            if (!_config.EnableAffinityAwareness)
            {
                return null;
            }

            UpdateDistributionMap(cacheId);

            var distributionMap = _distributionMap;
            var socketMap = _nodeSocketMap;
            ClientCachePartitionMap cachePartMap;

            if (socketMap == null || !distributionMap.CachePartitionMap.TryGetValue(cacheId, out cachePartMap))
            {
                return null;
            }

            if (cachePartMap == null)
            {
                return null;
            }

            var partition = GetPartition(key, cachePartMap.PartitionNodeIds.Count, cachePartMap.KeyConfiguration);
            var nodeId = cachePartMap.PartitionNodeIds[partition];

            ClientSocket socket;
            if (socketMap.TryGetValue(nodeId, out socket) && !socket.IsDisposed)
            {
                return socket;
            }

            return null;
        }

        /// <summary>
        /// Throws if disposed.
        /// </summary>
        /// <exception cref="ObjectDisposedException"></exception>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException("ClientFailoverSocket");
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

                if (_nodeSocketMap != null)
                {
                    foreach (var socket in _nodeSocketMap.Values)
                    {
                        socket.Dispose();
                    }

                    _nodeSocketMap = null;
                }
            }
        }

        /// <summary>
        /// Connects the socket.
        /// </summary>
        private void Connect()
        {
            List<Exception> errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);
            _socket = null;

            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (startIdx + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

                if (endPoint.Socket != null && !endPoint.Socket.IsDisposed)
                {
                    _socket = endPoint.Socket;
                    break;
                }

                try
                {
                    _socket = new ClientSocket(_config, endPoint.EndPoint, endPoint.Host, null,
                        OnAffinityTopologyVersionChange);

                    endPoint.Socket = _socket;

                    break;
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

            if (_socket == null && errors != null)
            {
                throw new AggregateException("Failed to establish Ignite thin client connection, " +
                                             "examine inner exceptions for details.", errors);
            }

            if (_config.EnableAffinityAwareness)
            {
                InitSocketMap();
            }
        }

        /// <summary>
        /// Updates current Affinity Topology Version.
        /// </summary>
        private void OnAffinityTopologyVersionChange(AffinityTopologyVersion affinityTopologyVersion)
        {
            _affinityTopologyVersion = affinityTopologyVersion;
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private static IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null);  // Checked by GetEndpoints.

                // GetHostEntry accepts IPs, but TryParse is a more efficient shortcut.
                IPAddress ip;

                if (IPAddress.TryParse(host, out ip))
                {
                    for (var i = 0; i <= e.PortRange; i++)
                    {
                        yield return new SocketEndpoint(new IPEndPoint(ip, e.Port + i), host);
                    }
                }
                else
                {
                    for (var i = 0; i <= e.PortRange; i++)
                    {
                        foreach (var x in Dns.GetHostEntry(host).AddressList)
                        {
                            yield return new SocketEndpoint(new IPEndPoint(x, e.Port + i), host);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Returns a value indicating whether distribution map is up to date.
        /// </summary>
        /// <returns></returns>
        private bool IsDistributionMapUpToDate(ClientCacheTopologyPartitionMap map = null)
        {
            map = map ?? _distributionMap;

            if (map == null || _affinityTopologyVersion == null)
            {
                return false;
            }

            return map.AffinityTopologyVersion >= _affinityTopologyVersion.Value;
        }

        /// <summary>
        /// Updates the partition mapping.
        /// </summary>
        private void UpdateDistributionMap(int cacheId)
        {
            if (IsDistributionMapUpToDate())
                return; // Up to date.

            lock (_distributionMapSyncRoot)
            {
                if (IsDistributionMapUpToDate())
                    return; // Up to date.

                DoOutInOp(
                    ClientOp.CachePartitions,
                    s => WriteDistributionMapRequest(cacheId, s),
                    s => ReadDistributionMapResponse(s));
            }
        }

        private object ReadDistributionMapResponse(IBinaryStream s)
        {
            var affinityTopologyVersion = new AffinityTopologyVersion(s.ReadLong(), s.ReadInt());
            var size = s.ReadInt();
            var mapping = new Dictionary<int, ClientCachePartitionMap>();

            for (int i = 0; i < size; i++)
            {
                var grp = new ClientCacheAffinityAwarenessGroup(s);

                if (grp.PartitionMap == null)
                {
                    // Partition awareness is not applicable for these caches.
                    foreach (var cache in grp.Caches)
                    {
                        mapping[cache.Key] = null;
                    }

                    continue;
                }

                // Count partitions to avoid reallocating array.
                int maxPartNum = 0;
                foreach (var partMap in grp.PartitionMap)
                {
                    foreach (var part in partMap.Value)
                    {
                        if (part > maxPartNum)
                        {
                            maxPartNum = part;
                        }
                    }
                }

                // Populate partition array.
                var partNodeIds = new Guid[maxPartNum + 1];
                foreach (var partMap in grp.PartitionMap)
                {
                    foreach (var part in partMap.Value)
                    {
                        partNodeIds[part] = partMap.Key;
                    }
                }

                foreach (var cache in grp.Caches)
                {
                    mapping[cache.Key] = new ClientCachePartitionMap(cache.Key, partNodeIds, cache.Value);
                }
            }

            _distributionMap = new ClientCacheTopologyPartitionMap(mapping, affinityTopologyVersion);

            return null;
        }

        private void WriteDistributionMapRequest(int cacheId, IBinaryStream s)
        {
            if (_distributionMap != null)
            {
                // Map exists: request update for all caches.
                var mapContainsCacheId = _distributionMap.CachePartitionMap.ContainsKey(cacheId);
                var count = _distributionMap.CachePartitionMap.Count;
                if (!mapContainsCacheId)
                {
                    count++;
                }

                s.WriteInt(count);

                foreach (var cachePartitionMap in _distributionMap.CachePartitionMap)
                {
                    s.WriteInt(cachePartitionMap.Key);
                }

                if (!mapContainsCacheId)
                {
                    s.WriteInt(cacheId);
                }
            }
            else
            {
                // Map does not exist yet: request update for specified cache only.
                s.WriteInt(1);
                s.WriteInt(cacheId);
            }
        }

        private int GetPartition<TKey>(TKey key, int partitionCount, IDictionary<int, int> keyConfiguration)
        {
            var keyHash = BinaryHashCodeUtils.GetHashCode(key, _marsh, keyConfiguration);
            return ClientRendezvousAffinityFunction.GetPartitionForKey(keyHash, partitionCount);
        }

        private void InitSocketMap()
        {
            var map = new Dictionary<Guid, ClientSocket>();

            foreach (var endPoint in _endPoints)
            {
                if (endPoint.Socket == null || endPoint.Socket.IsDisposed)
                {
                    try
                    {
                        var socket = new ClientSocket(_config, endPoint.EndPoint, endPoint.Host, null,
                            OnAffinityTopologyVersionChange);

                        endPoint.Socket = socket;
                    }
                    catch (SocketException)
                    {
                        continue;
                    }
                }

                var nodeId = endPoint.Socket.ServerNodeId;
                if (nodeId != null)
                {
                    map[nodeId.Value] = endPoint.Socket;
                }
            }

            _nodeSocketMap = map;
        }
    }
}
