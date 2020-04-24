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

        /** Endpoints with corresponding hosts. */
        private volatile List<SocketEndpoint> _endPoints;

        /** Locker. */
        private readonly object _syncRoot = new object();

        /** Disposed flag. */
        private bool _disposed;

        /** Current affinity topology version. Store as object to make volatile. */
        private volatile object _affinityTopologyVersion;

        /** Map from node ID to connected socket. */
        private volatile Dictionary<Guid, ClientSocket> _nodeSocketMap;

        /** Map from cache ID to partition mapping. */
        private volatile ClientCacheTopologyPartitionMap _distributionMap;

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
                var socket = _socket;
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
                var socket = _socket;
                return socket != null ? socket.LocalEndPoint : null;
            }
        }
        
        /// <summary>
        /// Gets active connections.
        /// </summary>
        public IEnumerable<IClientConnection> GetConnections()
        {
            foreach (var socketEndpoint in _endPoints)
            {
                var socket = socketEndpoint.Socket;
                
                if (socket != null && !socket.IsDisposed)
                {
                    yield return new ClientConnection(socket.LocalEndPoint, socket.RemoteEndPoint, 
                        socket.ServerNodeId.GetValueOrDefault());
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
                    _socket = new ClientSocket(_config, endPoint.EndPoint, endPoint.Host, 
                        _config.ProtocolVersion, OnAffinityTopologyVersionChange, _marsh);

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

            if (_socket != null)
            {
                if (_config.EnablePartitionAwareness &&
                    _socket.ServerVersion < ClientOp.CachePartitions.GetMinVersion())
                {
                    _config.EnablePartitionAwareness = false;

                    _logger.Warn("Partition awareness has been disabled: server protocol version {0} " +
                                 "is lower than required {1}",
                        _socket.ServerVersion,
                        ClientOp.CachePartitions.GetMinVersion()
                    );
                }

                // TODO: Use feature flags.
                if (_config.EnableDiscovery &&
                    _socket.ServerVersion < ClientOp.ClusterGroupGetNodesEndpoints.GetMinVersion())
                {
                    _config.EnableDiscovery = false;
                    
                    _logger.Warn("Automatic server node discovery has been disabled: server protocol version {0} " +
                                 "is lower than required {1}",
                        _socket.ServerVersion,
                        ClientOp.ClusterGroupGetNodesEndpoints.GetMinVersion()
                    );
                }
                
                DiscoverEndpoints(UnknownTopologyVersion, UnknownTopologyVersion);
            }
        }

        /// <summary>
        /// Updates current Affinity Topology Version.
        /// </summary>
        private void OnAffinityTopologyVersionChange(AffinityTopologyVersion affinityTopologyVersion)
        {
            var oldTopologyVersion = GetTopologyVersion();
            _affinityTopologyVersion = affinityTopologyVersion;

            // TODO: By default, without partition awareness, we only connect one socket, and fail over as needed.
            // Only with PartitionAwareness we maintain many connections.
            // Should we keep this behavior?
            if (_config.EnablePartitionAwareness)
            {
                // TODO: Why re-init the map on every topology change?
                InitSocketMap();
            }

            // Re-discover nodes when major topology version has changed.
            var newTopologyVersion = affinityTopologyVersion.Version;
            
            if (_config.EnableDiscovery && oldTopologyVersion < newTopologyVersion)
            {
                DiscoverEndpoints(oldTopologyVersion, newTopologyVersion);
            }
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
            var map = new Dictionary<Guid, ClientSocket>();

            // TODO: Make sure we don't connect to the same node twice.
            foreach (var endPoint in _endPoints)
            {
                if (endPoint.Socket == null || endPoint.Socket.IsDisposed)
                {
                    try
                    {
                        var socket = new ClientSocket(_config, endPoint.EndPoint, endPoint.Host, 
                            _config.ProtocolVersion, OnAffinityTopologyVersionChange, _marsh);

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
        
        /// <summary>
        /// Updates endpoint info.
        /// </summary>
        private void DiscoverEndpoints(long startTopVer, long endTopVer)
        {
            if (!_config.EnableDiscovery)
            {
                return;
            }
            
            // TODO: perform async request!
            var res = GetServerEndpoints(startTopVer, endTopVer);
            var endPoints = _endPoints.ToList();
            
            foreach (var addedNode in res.JoinedNodes)
            {
                // TODO: More efficient check - use socketMap.
                if (endPoints.Any(e => e.Socket != null && e.Socket.ServerNodeId == addedNode.Id))
                {
                    // Already connected to that node.
                    continue;
                }

                foreach (var endpoint in addedNode.Endpoints)
                {
                    try
                    {
                        IPAddress ip;
                        if (IPAddress.TryParse(endpoint.Address, out ip))
                        {
                            var ipEndPoint = new IPEndPoint(ip, endpoint.Port);
                            var socket = new ClientSocket(_config, ipEndPoint, endpoint.Host,
                                _config.ProtocolVersion, OnAffinityTopologyVersionChange, _marsh);

                            if (socket.ServerNodeId != addedNode.Id)
                            {
                                _logger.Debug(
                                    "Autodiscovery connection succeeded, but node id does not match: {0}, {1}, {2}. " +
                                    "Expected node id: {3}. Actual node id: {4}. Connection dropped.",
                                    endpoint.Address, endpoint.Host, endpoint.Port, addedNode.Id, socket.ServerNodeId);
                                
                                socket.Dispose();
                                
                                continue;
                            }

                            var socketEndPoint = new SocketEndpoint(ipEndPoint, endpoint.Host)
                            {
                                Socket = socket
                            };

                            endPoints.Add(socketEndPoint);
                        }
                    }
                    catch (SocketException socketException)
                    {
                        // Ignore: failure to connect is expected.
                        _logger.Debug(socketException, "Autodiscovery connection failed: {0}, {1}, {2}", 
                            endpoint.Address, endpoint.Host, endpoint.Port);
                    }
                    catch (Exception e)
                    {
                        _logger.Debug(e, "Autodiscovery connection failed: {0}, {1}, {2}", 
                            endpoint.Address, endpoint.Host, endpoint.Port);
                    }
                }

                _endPoints = endPoints;
            }
        }

        /// <summary>
        /// Gets all server endpoints.
        /// </summary>
        private ClientDiscoveryResult GetServerEndpoints(long startTopVer, long endTopVer)
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
                    var addedNodes = new List<ClientDiscoveryNode>(addedCnt);

                    for (var i = 0; i < addedCnt; i++)
                    {
                        var id = BinaryUtils.ReadGuid(s);
                        var cnt = s.ReadInt();
                        var endpoints = new List<ClientDiscoveryEndpoint>(cnt);
                        
                        for (var j = 0; j < cnt; j++)
                        {
                            var addr = ctx.Reader.ReadString();
                            var host = ctx.Reader.ReadString();
                            var port = s.ReadInt();

                            endpoints.Add(new ClientDiscoveryEndpoint(addr, host, port));
                        }
                        
                        addedNodes.Add(new ClientDiscoveryNode(id, endpoints));
                    }

                    var removedCnt = s.ReadInt();
                    var removedNodeIds = new List<Guid>(removedCnt);

                    for (int i = 0; i < removedCnt; i++)
                    {
                        removedNodeIds.Add(BinaryUtils.ReadGuid(s));
                    }
                    
                    return new ClientDiscoveryResult(topVer, addedNodes, removedNodeIds);
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
