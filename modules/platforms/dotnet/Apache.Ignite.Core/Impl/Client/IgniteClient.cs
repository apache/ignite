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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Thin client implementation
    /// </summary>
    internal class IgniteClient : IIgnite
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClient"/> class.
        /// </summary>
        /// <param name="clientConfiguration">The client configuration.</param>
        public IgniteClient(IgniteClientConfiguration clientConfiguration)
        {
            Debug.Assert(clientConfiguration != null);

            _socket = new ClientSocket(clientConfiguration);
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            _socket.Dispose();
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public ICluster GetCluster()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICompute GetCompute()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return new CacheClient<TK, TV>(_socket, name);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void DestroyCache(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IBinary GetBinary()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICacheAffinity GetAffinity(string name)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ITransactions GetTransactions()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IMessaging GetMessaging()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IEvents GetEvents()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IServices GetServices()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IAtomicLong GetAtomicLong(string name, long initialValue, bool create)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IAtomicSequence GetAtomicSequence(string name, long initialValue, bool create)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IAtomicReference<T> GetAtomicReference<T>(string name, T initialValue, bool create)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IgniteConfiguration GetConfiguration()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICollection<string> GetCacheNames()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ILogger Logger
        {
            get { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler Stopping
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler Stopped
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler ClientDisconnected
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public event EventHandler<ClientReconnectEventArgs> ClientReconnected
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        /** <inheritDoc /> */
        public T GetPlugin<T>(string name) where T : class
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void ResetLostPartitions(IEnumerable<string> cacheNames)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void ResetLostPartitions(params string[] cacheNames)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public ICollection<IMemoryMetrics> GetMemoryMetrics()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IMemoryMetrics GetMemoryMetrics(string memoryPolicyName)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public void SetActive(bool isActive)
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public bool IsActive()
        {
            throw new NotImplementedException();
        }

        /** <inheritDoc /> */
        public IPersistentStoreMetrics GetPersistentStoreMetrics()
        {
            throw new NotImplementedException();
        }
    }
}
