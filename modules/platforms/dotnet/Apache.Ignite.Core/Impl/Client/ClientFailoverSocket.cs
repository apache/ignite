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
    using Apache.Ignite.Core.Impl.Log;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Socket wrapper with reconnect/failover functionality: reconnects on failure.
    /// </summary>
    internal class ClientFailoverSocket : IDisposable
    {
        /** Unknown topology version. */
        private const long UnknownTopologyVersion = -1;
        
        /** Underlying socket. */
        private ClientSocket _socket;

        /** Current global endpoint index for Round-robin. */
        private static long _endPointIndex;

        /** Config. */
        private readonly IgniteClientConfiguration _config;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Endpoints with corresponding hosts - from config. */
        private readonly List<SocketEndpoint> _endPoints;

        /** Map from node ID to connected socket. */
        private volatile Dictionary<Guid, ClientSocket> _nodeSocketMap = new Dictionary<Guid, ClientSocket>();

        /** Discovered nodes map. Represents current topology. */
        private volatile Dictionary<Guid, ClientDiscoveryNode> _discoveryNodes;

        /** Main socket lock. */
        private readonly object _socketLock = new object();

        /** Topology change lock. */
        private readonly object _topologyUpdateLock = new object();

        /** Disposed flag. */
        private volatile bool _disposed;

        /** Current affinity topology version. Store as object to make volatile. */
        private volatile object _affinityTopologyVersion;

        /** Topology version that <see cref="_discoveryNodes"/> corresponds to. */
        private long _discoveryTopologyVersion = UnknownTopologyVersion;

        /** Map from cache ID to partition mapping. */
        private volatile ClientCacheTopologyPartitionMap _distributionMap;

        /** Enable discovery flag. */
        private volatile bool _enableDiscovery = true;

        /** Distribution map locker. */
        private readonly object _distributionMapSyncRoot = new object();

        /** Logger. */
        private readonly ILogger _logger;

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

            _logger = (_config.Logger ?? NoopLogger.Instance).GetLogger(GetType());
            
            Connect();
        }

        /// <summary>
        /// Performs a send-receive operation.
        /// </summary>
        public T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction, 
            Func<ClientResponseContext, T> readFunc,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return GetSocket().DoOutInOp(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs a send-receive operation with partition awareness.
        /// </summary>
        public T DoOutInOpAffinity<T, TKey>(
            ClientOp opId,
            Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc,
            int cacheId,
            TKey key,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            var socket = GetAffinitySocket(cacheId, key) ?? GetSocket();

            return socket.DoOutInOp(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs an async send-receive operation with partition awareness.
        /// </summary>
        public Task<T> DoOutInOpAffinityAsync<T, TKey>(
            ClientOp opId,
            Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc,
            int cacheId,
            TKey key,
            Func<ClientStatusCode, string, T> errorFunc = null)
        {
            var socket = GetAffinitySocket(cacheId, key) ?? GetSocket();

            return socket.DoOutInOpAsync(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Performs an async send-receive operation.
        /// </summary>
        public Task<T> DoOutInOpAsync<T>(ClientOp opId, Action<ClientRequestContext> writeAction, 
            Func<ClientResponseContext, T> readFunc, Func<ClientStatusCode, string, T> errorFunc = null)
        {
            return GetSocket().DoOutInOpAsync(opId, writeAction, readFunc, errorFunc);
        }

        /// <summary>
        /// Gets the current protocol version.
        /// Only used for tests.
        /// </summary>
        public ClientProtocolVersion CurrentProtocolVersion
        {
            get { return GetSocket().ServerVersion; }
        }

        /// <summary>
        /// Gets the remote endpoint.
        /// </summary>
        public EndPoint RemoteEndPoint
        {
            get
            {
                var socket = GetSocket();
                return socket != null ? socket.RemoteEndPoint : null;
            }
        }

        /// <summary>
        /// Gets the local endpoint.
        /// </summary>
        public EndPoint LocalEndPoint
        {
            get
            {
                var socket = GetSocket();
                return socket != null ? socket.LocalEndPoint : null;
            }
        }

        /// <summary>
        /// Gets active connections.
        /// </summary>
        public IEnumerable<IClientConnection> GetConnections()
        {
            var map = _nodeSocketMap;

            foreach (var socket in map.Values)
            {
                if (!socket.IsDisposed)
                {
                    yield return new ClientConnection(socket.LocalEndPoint, socket.RemoteEndPoint,
                        socket.ServerNodeId.GetValueOrDefault());
                }
            }

            foreach (var socketEndpoint in _endPoints)
            {
                var socket = socketEndpoint.Socket;

                if (socket == null || socket.IsDisposed)
                {
                    continue;
                }

                if (socket.ServerNodeId != null && map.ContainsKey(socket.ServerNodeId.Value))
                {
                    continue;
                }
                
                yield return new ClientConnection(socket.LocalEndPoint, socket.RemoteEndPoint,
                    socket.ServerNodeId.GetValueOrDefault());
            }
        }

        /// <summary>
        /// Checks the disposed state.
        /// </summary>
        private ClientSocket GetSocket()
        {
            lock (_socketLock)
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
            ThrowIfDisposed();
            
            if (!_config.EnablePartitionAwareness)
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
            lock (_socketLock)
            lock (_topologyUpdateLock)
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

                foreach (var socketEndpoint in _endPoints)
                {
                    if (socketEndpoint.Socket != null)
                    {
                        socketEndpoint.Socket.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Gets next connected socket, or connects a new one.
        /// </summary>
        private ClientSocket GetNextSocket()
        {
            List<Exception> errors = null;
            var startIdx = (int) Interlocked.Increment(ref _endPointIndex);

            // Check socket map first, if available: it includes all cluster nodes.
            var map = _nodeSocketMap;
            foreach (var socket in map.Values)
            {
                if (!socket.IsDisposed)
                {
                    return socket;
                }
            }

            // Fall back to initially known endpoints.
            for (var i = 0; i < _endPoints.Count; i++)
            {
                var idx = (startIdx + i) % _endPoints.Count;
                var endPoint = _endPoints[idx];

                if (endPoint.Socket != null && !endPoint.Socket.IsDisposed)
                {
                    return endPoint.Socket;
                }

                try
                {
                    return Connect(endPoint);
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
        /// Connects the socket.
        /// </summary>
        private void Connect()
        {
            _socket = GetNextSocket();

            if (_config.EnablePartitionAwareness && !_socket.Features.HasOp(ClientOp.CachePartitions))
            {
                _config.EnablePartitionAwareness = false;

                _logger.Warn("Partition awareness has been disabled: server protocol version {0} " +
                             "is lower than required {1}",
                    _socket.ServerVersion,
                    ClientFeatures.GetMinVersion(ClientOp.CachePartitions)
                );
            }

            if (!_socket.Features.HasFeature(ClientBitmaskFeature.ClusterGroupGetNodesEndpoints))
            {
                _enableDiscovery = false;

                _logger.Warn("Automatic server node discovery is not supported by the server");
            }
        }

        /// <summary>
        /// Connects to the given endpoint.
        /// </summary>
        private ClientSocket Connect(SocketEndpoint endPoint)
        {
            var socket = new ClientSocket(_config, endPoint.EndPoint, endPoint.Host,
                _config.ProtocolVersion, OnAffinityTopologyVersionChange, _marsh);

            endPoint.Socket = socket;

            return socket;
        }

        /// <summary>
        /// Updates current Affinity Topology Version.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Thread root must catch all exceptions to avoid crashing the process.")]
        private void OnAffinityTopologyVersionChange(AffinityTopologyVersion affinityTopologyVersion)
        {
            _affinityTopologyVersion = affinityTopologyVersion;

            if (_discoveryTopologyVersion < affinityTopologyVersion.Version &&_config.EnablePartitionAwareness)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    try
                    {
                        lock (_topologyUpdateLock)
                        {
                            if (!_disposed)
                            {
                                DiscoverEndpoints();
                                InitSocketMap();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.Log(LogLevel.Error, e, "Failed to update topology information");
                    }
                });
            }
        }

        /// <summary>
        /// Gets the endpoints: all combinations of IP addresses and ports according to configuration.
        /// </summary>
        private IEnumerable<SocketEndpoint> GetIpEndPoints(IgniteClientConfiguration cfg)
        {
            foreach (var e in Endpoint.GetEndpoints(cfg))
            {
                var host = e.Host;
                Debug.Assert(host != null);  // Checked by GetEndpoints.

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
                _logger.Debug(e, "Failed to parse host: " + host);

                if (suppressExceptions)
                {
                    return Enumerable.Empty<IPAddress>();
                }

                throw;
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

            return map.AffinityTopologyVersion >= (AffinityTopologyVersion) _affinityTopologyVersion;
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
                    s => WriteDistributionMapRequest(cacheId, s.Stream),
                    s => ReadDistributionMapResponse(s.Stream));
            }
        }

        private object ReadDistributionMapResponse(IBinaryStream s)
        {
            var affinityTopologyVersion = new AffinityTopologyVersion(s.ReadLong(), s.ReadInt());
            var size = s.ReadInt();
            var mapping = new Dictionary<int, ClientCachePartitionMap>();

            for (int i = 0; i < size; i++)
            {
                var grp = new ClientCachePartitionAwarenessGroup(s);

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
                    mapping[cache.Key] = new ClientCachePartitionMap(partNodeIds, cache.Value);
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
            var map = new Dictionary<Guid, ClientSocket>(_nodeSocketMap);

            var defaultSocket = _socket;
            if (defaultSocket != null && !defaultSocket.IsDisposed && defaultSocket.ServerNodeId != null)
            {
                map[defaultSocket.ServerNodeId.Value] = defaultSocket;
            }

            if (_discoveryNodes != null)
            {
                // Discovery enabled: make sure we have connection to all nodes in the cluster.
                foreach (var node in _discoveryNodes.Values)
                {
                    ClientSocket socket;
                    if (map.TryGetValue(node.Id, out socket) && !socket.IsDisposed)
                    {
                        continue;
                    }

                    socket = TryConnect(node);

                    if (socket != null)
                    {
                        map[node.Id] = socket;
                    }
                }

                // Dispose and remove any connections not in current topology.
                var toRemove = new List<Guid>();
                
                foreach (var pair in map)
                {
                    if (!_discoveryNodes.ContainsKey(pair.Key))
                    {
                        pair.Value.Dispose();
                        toRemove.Add(pair.Key);
                    }
                }

                foreach (var nodeId in toRemove)
                {
                    map.Remove(nodeId);
                }
            }
            else
            {
                // Discovery disabled: fall back to endpoints from config.
                foreach (var endPoint in _endPoints)
                {
                    if (endPoint.Socket == null || endPoint.Socket.IsDisposed)
                    {
                        try
                        {
                            Connect(endPoint);
                        }
                        catch (SocketException)
                        {
                            continue;
                        }
                    }

                    // ReSharper disable once PossibleNullReferenceException (Connect ensures that).
                    var nodeId = endPoint.Socket.ServerNodeId;
                    if (nodeId != null)
                    {
                        map[nodeId.Value] = endPoint.Socket;
                    }
                }
            }
            
            _nodeSocketMap = map;
        }

        private ClientSocket TryConnect(ClientDiscoveryNode node)
        {
            foreach (var addr in node.Addresses)
            {
                foreach (var ip in GetIps(addr, true))
                {
                    try
                    {
                        var ipEndpoint = new IPEndPoint(ip, node.Port);

                        var socket = new ClientSocket(_config, ipEndpoint, addr,
                            _config.ProtocolVersion, OnAffinityTopologyVersionChange, _marsh);

                        if (socket.ServerNodeId == node.Id)
                        {
                            return socket;
                        }

                        _logger.Debug(
                            "Autodiscovery connection succeeded, but node id does not match: {0}, {1}. " +
                            "Expected node id: {2}. Actual node id: {3}. Connection dropped.",
                            addr, node.Port, node.Id, socket.ServerNodeId);
                    }
                    catch (SocketException socketEx)
                    {
                        // Ignore: failure to connect is expected.
                        _logger.Debug(socketEx, "Autodiscovery connection failed: {0}, {1}", addr, node.Port);
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Updates endpoint info.
        /// </summary>
        private void DiscoverEndpoints()
        {
            if (!_enableDiscovery)
            {
                return;
            }
            
            var newVer = GetTopologyVersion();

            if (newVer <= _discoveryTopologyVersion)
            {
                return;
            }

            var discoveryNodes = _discoveryNodes == null
                ? new Dictionary<Guid, ClientDiscoveryNode>()
                : new Dictionary<Guid, ClientDiscoveryNode>(_discoveryNodes);

            _discoveryTopologyVersion = GetServerEndpoints(
                _discoveryTopologyVersion, newVer, discoveryNodes);
            
            _discoveryNodes = discoveryNodes;
        }

        /// <summary>
        /// Gets all server endpoints.
        /// </summary>
        private long GetServerEndpoints(long startTopVer, long endTopVer, IDictionary<Guid, ClientDiscoveryNode> dict)
        {
            return DoOutInOp(ClientOp.ClusterGroupGetNodesEndpoints,
                ctx =>
                {
                    ctx.Writer.WriteLong(startTopVer);
                    ctx.Writer.WriteLong(endTopVer);
                },
                ctx =>
                {
                    var s = ctx.Stream;

                    var topVer = s.ReadLong();

                    var addedCnt = s.ReadInt();

                    for (var i = 0; i < addedCnt; i++)
                    {
                        var id = BinaryUtils.ReadGuid(s);
                        var port = s.ReadInt();
                        var addresses = ctx.Reader.ReadStringCollection();
                        
                        dict[id] = new ClientDiscoveryNode(id, port, addresses);
                    }

                    var removedCnt = s.ReadInt();

                    for (var i = 0; i < removedCnt; i++)
                    {
                        dict.Remove(BinaryUtils.ReadGuid(s));
                    }
                    
                    return topVer;
                });
        }

        /// <summary>
        /// Gets current topology version.
        /// </summary>
        private long GetTopologyVersion()
        {
            var ver = _affinityTopologyVersion;
            
            return ver == null ? UnknownTopologyVersion : ((AffinityTopologyVersion) ver).Version;
        }
    }
}
