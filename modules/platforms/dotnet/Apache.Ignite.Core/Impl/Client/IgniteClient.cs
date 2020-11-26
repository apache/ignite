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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Net;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Platform;
    using Apache.Ignite.Core.Impl.Client.Cache;
    using Apache.Ignite.Core.Impl.Client.Cluster;
    using Apache.Ignite.Core.Impl.Client.Compute;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Plugin;

    /// <summary>
    /// Thin client implementation.
    /// </summary>
    internal class IgniteClient : IIgniteInternal, IIgniteClient
    {
        /** Socket. */
        private readonly ClientFailoverSocket _socket;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Binary processor. */
        private readonly IBinaryProcessor _binProc;

        /** Binary. */
        private readonly IBinary _binary;

        /** Configuration. */
        private readonly IgniteClientConfiguration _configuration;

        /** Node info cache. */
        private readonly ConcurrentDictionary<Guid, IClientClusterNode> _nodes =
            new ConcurrentDictionary<Guid, IClientClusterNode>();
        
        /** Cluster. */
        private readonly ClientCluster _cluster;

        /** Compute. */
        private readonly ComputeClient _compute;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClient"/> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        public IgniteClient(IgniteClientConfiguration clientConfiguration)
        {
            Debug.Assert(clientConfiguration != null);

            _configuration = new IgniteClientConfiguration(clientConfiguration);

            _marsh = new Marshaller(_configuration.BinaryConfiguration)
            {
                Ignite = this
            };

            _socket = new ClientFailoverSocket(_configuration, _marsh);

            _binProc = _configuration.BinaryProcessor ?? new BinaryProcessorClient(_socket);

            _binary = new Binary(_marsh);
            
            _cluster = new ClientCluster(this);
            
            _compute = new ComputeClient(this, ComputeClientFlags.None, TimeSpan.Zero, null);
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        public ClientFailoverSocket Socket
        {
            get { return _socket; }
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _socket.Dispose();
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> GetCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return new CacheClient<TK, TV>(this, name);
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            DoOutOp(ClientOp.CacheGetOrCreateWithName, ctx => ctx.Writer.WriteString(name));

            return GetCache<TK, TV>(name);
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> GetOrCreateCache<TK, TV>(CacheClientConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            DoOutOp(ClientOp.CacheGetOrCreateWithConfiguration,
                ctx => ClientCacheConfigurationSerializer.Write(ctx.Stream, configuration, ctx.Features));

            return GetCache<TK, TV>(configuration.Name);
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> CreateCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            DoOutOp(ClientOp.CacheCreateWithName, ctx => ctx.Writer.WriteString(name));

            return GetCache<TK, TV>(name);
        }

        /** <inheritDoc /> */
        public ICacheClient<TK, TV> CreateCache<TK, TV>(CacheClientConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            DoOutOp(ClientOp.CacheCreateWithConfiguration,
                ctx => ClientCacheConfigurationSerializer.Write(ctx.Stream, configuration, ctx.Features));

            return GetCache<TK, TV>(configuration.Name);
        }

        /** <inheritDoc /> */
        public ICollection<string> GetCacheNames()
        {
            return DoOutInOp(ClientOp.CacheGetNames, null, ctx => ctx.Reader.ReadStringCollection());
        }

        /** <inheritDoc /> */
        public IClientCluster GetCluster()
        {
            return _cluster;
        }

        /** <inheritDoc /> */
        public void DestroyCache(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            DoOutOp(ClientOp.CacheDestroy, ctx => ctx.Stream.WriteInt(BinaryUtils.GetCacheId(name)));
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public IIgnite GetIgnite()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IBinary GetBinary()
        {
            return _binary;
        }

        /** <inheritDoc /> */
        public CacheAffinityImpl GetAffinity(string cacheName)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheAffinityManager GetAffinityManager(string cacheName)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetCacheConfiguration(int cacheId)
        {
            throw GetClientNotSupportedException();
        }

        public object GetJavaThreadLocal()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IgniteClientConfiguration GetConfiguration()
        {
            // Return a copy to allow modifications by the user.
            return new IgniteClientConfiguration(_configuration);
        }

        /** <inheritDoc /> */
        public EndPoint RemoteEndPoint
        {
            get { return _socket.RemoteEndPoint; }
        }

        /** <inheritDoc /> */
        public EndPoint LocalEndPoint
        {
            get { return _socket.LocalEndPoint; }
        }

        /** <inheritDoc /> */
        public IEnumerable<IClientConnection> GetConnections()
        {
            return _socket.GetConnections();
        }

        /** <inheritDoc /> */
        public IComputeClient GetCompute()
        {
            return _compute;
        }

        /** <inheritDoc /> */
        public IBinaryProcessor BinaryProcessor
        {
            get { return _binProc; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public IgniteConfiguration Configuration
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public HandleRegistry HandleRegistry
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public ClusterNodeImpl GetNode(Guid? id)
        {
            throw GetClientNotSupportedException();
        }

        /// <summary>
        /// Gets client node from the internal cache.
        /// </summary>
        /// <param name="id">Node Id.</param>
        /// <returns>Client node.</returns>
        public IClientClusterNode GetClientNode(Guid id)
        {
            IClientClusterNode result;
            if (!_nodes.TryGetValue(id, out result))
            {
                throw new ArgumentException(string.Format(
                    CultureInfo.InvariantCulture, "Unable to find node with id='{0}'", id));
            }
            return result;
        }

        /// <summary>
        /// Check whether <see cref="IgniteClient">Ignite Client</see> contains a node. />
        /// </summary>
        /// <param name="id">Node id.</param>
        /// <returns>True if contains, False otherwise.</returns>
        public bool ContainsNode(Guid id)
        {
            return _nodes.ContainsKey(id);
        }

        /** <inheritDoc /> */
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public PluginProcessor PluginProcessor
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public PlatformCacheManager PlatformCacheManager
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName, bool keepBinary)
        {
            throw GetClientNotSupportedException();
        }

        /// <summary>
        /// Saves the node information from stream to internal cache.
        /// </summary>
        /// <param name="reader">Reader.</param>
        public void SaveClientClusterNode(IBinaryRawReader reader)
        {
            var node = new ClientClusterNode(reader);
            _nodes[node.Id] = node;
        }

        /// <summary>
        /// Gets the client not supported exception.
        /// </summary>
        public static NotSupportedException GetClientNotSupportedException(string info = null)
        {
            var msg = "Operation is not supported in thin client mode.";

            if (info != null)
            {
                msg += " " + info;
            }

            return new NotSupportedException(msg);
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<ClientRequestContext> writeAction,
            Func<ClientResponseContext, T> readFunc)
        {
            return _socket.DoOutInOp(opId, writeAction, readFunc);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private void DoOutOp(ClientOp opId, Action<ClientRequestContext> writeAction = null)
        {
            DoOutInOp<object>(opId, writeAction, null);
        }
    }
}
