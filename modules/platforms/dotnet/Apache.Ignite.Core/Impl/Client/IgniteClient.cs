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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Client;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Plugin;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Thin client implementation
    /// </summary>
    internal class IgniteClient : IIgniteInternal
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Binary processor. */
        private readonly IBinaryProcessor _binProc;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClient"/> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        public IgniteClient(IgniteClientConfiguration clientConfiguration)
        {
            Debug.Assert(clientConfiguration != null);

            _socket = new ClientSocket(clientConfiguration);

            _marsh = new Marshaller(clientConfiguration.BinaryConfiguration)
            {
                Ignite = this
            };

            _binProc = clientConfiguration.BinaryProcessor ?? new BinaryProcessorClient(_socket);
        }

        /// <summary>
        /// Gets the socket.
        /// </summary>
        public ClientSocket Socket
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
        public string Name
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public ICluster GetCluster()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICompute GetCompute()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return new CacheClient<TK, TV>(this, name);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void DestroyCache(string name)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IBinary GetBinary()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICacheAffinity GetAffinity(string name)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ITransactions GetTransactions()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IMessaging GetMessaging()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IEvents GetEvents()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IServices GetServices()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IAtomicLong GetAtomicLong(string name, long initialValue, bool create)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IAtomicSequence GetAtomicSequence(string name, long initialValue, bool create)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IAtomicReference<T> GetAtomicReference<T>(string name, T initialValue, bool create)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IgniteConfiguration GetConfiguration()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICollection<string> GetCacheNames()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ILogger Logger
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler Stopping
        {
            add { throw GetClientNotSupportedException(); }
            remove { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler Stopped
        {
            add { throw GetClientNotSupportedException(); }
            remove { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler ClientDisconnected
        {
            add { throw GetClientNotSupportedException(); }
            remove { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler<ClientReconnectEventArgs> ClientReconnected
        {
            add { throw GetClientNotSupportedException(); }
            remove { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public T GetPlugin<T>(string name) where T : class
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void ResetLostPartitions(IEnumerable<string> cacheNames)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void ResetLostPartitions(params string[] cacheNames)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public ICollection<IMemoryMetrics> GetMemoryMetrics()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IMemoryMetrics GetMemoryMetrics(string memoryPolicyName)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public void SetActive(bool isActive)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public bool IsActive()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IPersistentStoreMetrics GetPersistentStoreMetrics()
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public IBinaryProcessor BinaryProcessor
        {
            get { return _binProc; }
        }

        /** <inheritDoc /> */
        public IgniteConfiguration Configuration
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public HandleRegistry HandleRegistry
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public ClusterNodeImpl GetNode(Guid? id)
        {
            throw GetClientNotSupportedException();
        }

        /** <inheritDoc /> */
        public Marshaller Marshaller
        {
            get { return _marsh; }
        }

        /** <inheritDoc /> */
        public PluginProcessor PluginProcessor
        {
            get { throw GetClientNotSupportedException(); }
        }

        /** <inheritDoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName, bool keepBinary)
        {
            throw GetClientNotSupportedException();
        }

        /// <summary>
        /// Gets the client not supported exception.
        /// </summary>
        public static NotSupportedException GetClientNotSupportedException()
        {
            return new NotSupportedException("Operation is not supported in thin client mode.");
        }
    }
}
