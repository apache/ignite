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

namespace Apache.Ignite.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Plugin;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Main entry point for all Ignite APIs.
    /// You can obtain an instance of <see cref="IIgnite"/> through <see cref="Ignition.GetIgnite()"/>,
    /// or for named grids you can use <see cref="Ignition.GetIgnite(string)"/>. Note that you
    /// can have multiple instances of <see cref="IIgnite"/> running in the same process by giving
    /// each instance a different name.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IIgnite : IDisposable
    {
        /// <summary>
        /// Gets the name of the grid this Ignite instance (and correspondingly its local node) belongs to.
        /// Note that single process can have multiple Ignite instances all belonging to different grids. Grid
        /// name allows to indicate to what grid this particular Ignite instance (i.e. Ignite runtime and its
        /// local node) belongs to.
        /// <p/>
        /// If default Ignite instance is used, then <c>null</c> is returned. Refer to <see cref="Ignition"/> documentation
        /// for information on how to start named grids.
        /// </summary>
        /// <returns>Name of the grid, or <c>null</c> for default grid.</returns>
        string Name { get; }

        /// <summary>
        /// Gets an instance of <see cref="ICluster" /> interface.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        ICluster GetCluster();

        /// <summary>
        /// Gets compute functionality over this grid projection. All operations
        /// on the returned ICompute instance will only include nodes from
        /// this projection.
        /// </summary>
        /// <returns>Compute instance over this grid projection.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        ICompute GetCompute();

        /// <summary>
        /// Gets Ignite version.
        /// </summary>
        /// <returns>Ignite node version.</returns>
        IgniteProductVersion GetVersion();

        /// <summary>
        /// Gets the cache instance for the given name to work with keys and values of specified types.
        /// <para/>
        /// You can get instances of ICache of the same name, but with different key/value types.
        /// These will use the same named cache, but only allow working with entries of specified types.
        /// Attempt to retrieve an entry of incompatible type will result in <see cref="InvalidCastException"/>.
        /// Use <see cref="GetCache{TK,TV}"/> in order to work with entries of arbitrary types.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <returns>Cache instance for given name.</returns>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        ICache<TK, TV> GetCache<TK, TV>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> GetOrCreateCache<TK, TV>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// /// <param name="nearConfiguration">Near cache configuration for client.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration,
            NearCacheConfiguration nearConfiguration);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// /// <param name="nearConfiguration">Near cache configuration for client.</param>
        /// <param name="platformCacheConfiguration">Platform cache configuration. Can be null.
        /// When not null, native .NET cache is created additionally.</param>
        /// <returns>Existing or newly created cache.</returns>
        [IgniteExperimental]
        ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration,
            NearCacheConfiguration nearConfiguration, PlatformCacheConfiguration platformCacheConfiguration);

        /// <summary>
        /// Dynamically starts new cache using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> CreateCache<TK, TV>(string name);

        /// <summary>
        /// Dynamically starts new cache using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration);

        /// <summary>
        /// Dynamically starts new cache using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <param name="nearConfiguration">Near cache configuration for client.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration,
            NearCacheConfiguration nearConfiguration);

        /// <summary>
        /// Dynamically starts new cache using provided configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="configuration">Cache configuration.</param>
        /// <param name="nearConfiguration">Near cache configuration for client.</param>
        /// <param name="platformCacheConfiguration">Platform cache configuration. Can be null.
        /// When not null, native .NET cache is created additionally.</param>
        /// <returns>Existing or newly created cache.</returns>
        [IgniteExperimental]
        ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration,
            NearCacheConfiguration nearConfiguration, PlatformCacheConfiguration platformCacheConfiguration);

        /// <summary>
        /// Destroys dynamically created (with <see cref="CreateCache{TK,TV}(string)"/> or
        /// <see cref="GetOrCreateCache{TK,TV}(string)"/>) cache.
        /// </summary>
        /// <param name="name">The name of the cache to stop.</param>
        void DestroyCache(string name);

        /// <summary>
        /// Gets a new instance of data streamer associated with given cache name. Data streamer
        /// is responsible for loading external data into Ignite. For more information
        /// refer to <see cref="IDataStreamer{K,V}"/> documentation.
        /// </summary>
        /// <param name="cacheName">Cache name (<c>null</c> for default cache).</param>
        /// <returns>Data streamer.</returns>
        IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName);

        /// <summary>
        /// Gets an instance of <see cref="IBinary"/> interface.
        /// </summary>
        /// <returns>Instance of <see cref="IBinary"/> interface</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IBinary GetBinary();

        /// <summary>
        /// Gets affinity service to provide information about data partitioning and distribution.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <returns>Cache data affinity service.</returns>
        ICacheAffinity GetAffinity(string name);

        /// <summary>
        /// Gets Ignite transactions facade.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        ITransactions GetTransactions();

        /// <summary>
        /// Gets messaging facade over all cluster nodes.
        /// </summary>
        /// <returns>Messaging instance over all cluster nodes.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IMessaging GetMessaging();

        /// <summary>
        /// Gets events facade over all cluster nodes.
        /// </summary>
        /// <returns>Events facade over all cluster nodes.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IEvents GetEvents();

        /// <summary>
        /// Gets services facade over all cluster nodes.
        /// </summary>
        /// <returns>Services facade over all cluster nodes.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IServices GetServices();

        /// <summary>
        /// Gets an atomic long with specified name from cache.
        /// Creates new atomic long in cache if it does not exist and <c>create</c> is true.
        /// </summary>
        /// <param name="name">Name of the atomic long.</param>
        /// <param name="initialValue">
        /// Initial value for the atomic long. Ignored if <c>create</c> is false.
        /// </param>
        /// <param name="create">Flag indicating whether atomic long should be created if it does not exist.</param>
        /// <returns>Atomic long instance with specified name,
        /// or null if it does not exist and <c>create</c> flag is not set.</returns>
        /// <exception cref="IgniteException">If atomic long could not be fetched or created.</exception>
        IAtomicLong GetAtomicLong(string name, long initialValue, bool create);

        /// <summary>
        /// Gets an atomic sequence with specified name from cache.
        /// Creates new atomic sequence in cache if it does not exist and <paramref name="create"/> is true.
        /// </summary>
        /// <param name="name">Name of the atomic sequence.</param>
        /// <param name="initialValue">
        /// Initial value for the atomic sequence. Ignored if <paramref name="create"/> is false.
        /// </param>
        /// <param name="create">Flag indicating whether atomic sequence should be created if it does not exist.</param>
        /// <returns>Atomic sequence instance with specified name,
        /// or null if it does not exist and <paramref name="create"/> flag is not set.</returns>
        /// <exception cref="IgniteException">If atomic sequence could not be fetched or created.</exception>
        IAtomicSequence GetAtomicSequence(string name, long initialValue, bool create);

        /// <summary>
        /// Gets an atomic reference with specified name from cache.
        /// Creates new atomic reference in cache if it does not exist and <paramref name="create"/> is true.
        /// </summary>
        /// <param name="name">Name of the atomic reference.</param>
        /// <param name="initialValue">
        /// Initial value for the atomic reference. Ignored if <paramref name="create"/> is false.
        /// </param>
        /// <param name="create">Flag indicating whether atomic reference should be created if it does not exist.</param>
        /// <returns>Atomic reference instance with specified name,
        /// or null if it does not exist and <paramref name="create"/> flag is not set.</returns>
        /// <exception cref="IgniteException">If atomic reference could not be fetched or created.</exception>
        IAtomicReference<T> GetAtomicReference<T>(string name, T initialValue, bool create);

        /// <summary>
        /// Gets the configuration of this Ignite instance.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IgniteConfiguration GetConfiguration();

        /// <summary>
        /// Starts a near cache on local client node if cache with specified was previously started.
        /// This method does not work on server nodes.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="configuration">The configuration.</param>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <returns>Near cache instance.</returns>
        ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration);

        /// <summary>
        /// Starts a near cache on local client node if cache with specified was previously started.
        /// This method does not work on server nodes.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="configuration">The configuration.</param>
        /// <param name="platformConfiguration">Platform cache configuration. Can be null.
        /// When not null, native .NET cache is created additionally.</param>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <returns>Near cache instance.</returns>
        [IgniteExperimental]
        ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration,
            PlatformCacheConfiguration platformConfiguration);

        /// <summary>
        /// Gets existing near cache with the given name or creates a new one.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="configuration">The configuration.</param>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <returns>Near cache instance.</returns>
        ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration);

        /// <summary>
        /// Gets existing near cache with the given name or creates a new one.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="configuration">The configuration.</param>
        /// <param name="platformConfiguration">Platform cache configuration. Can be null.
        /// When not null, native .NET cache is created additionally.</param>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <returns>Near cache instance.</returns>
        [IgniteExperimental]
        ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration,
            PlatformCacheConfiguration platformConfiguration);

        /// <summary>
        /// Gets the collection of names of currently available caches, or empty collection if there are no caches.
        /// </summary>
        /// <returns>Collection of names of currently available caches.</returns>
        ICollection<string> GetCacheNames();

        /// <summary>
        /// Gets the logger.
        /// <para />
        /// See <see cref="IgniteConfiguration.Logger"/> for customization.
        /// </summary>
        ILogger Logger { get; }

        /// <summary>
        /// Occurs when node begins to stop. Node is fully functional at this point.
        /// See also: <see cref="LifecycleEventType.BeforeNodeStop"/>.
        /// </summary>
        event EventHandler Stopping;

        /// <summary>
        /// Occurs when node has stopped. Node can't be used at this point.
        /// See also: <see cref="LifecycleEventType.AfterNodeStop"/>.
        /// </summary>
        event EventHandler Stopped;

        /// <summary>
        /// Occurs when client node disconnects from the cluster. This event can only occur when this instance
        /// runs in client mode (<see cref="IgniteConfiguration.ClientMode"/>).
        /// </summary>
        event EventHandler ClientDisconnected;

        /// <summary>
        /// Occurs when client node reconnects to the cluster. This event can only occur when this instance
        /// runs in client mode (<see cref="IgniteConfiguration.ClientMode"/>).
        /// </summary>
        event EventHandler<ClientReconnectEventArgs> ClientReconnected;

        /// <summary>
        /// Gets the plugin by name.
        /// </summary>
        /// <typeparam name="T">Plugin type</typeparam>
        /// <param name="name">Plugin name.</param>
        /// <exception cref="PluginNotFoundException">When plugin with specified name has not been found.</exception>
        /// <returns>Plugin instance.</returns>
        T GetPlugin<T>(string name) where T : class;

        /// <summary>
        /// Clears partitions' lost state and moves caches to a normal mode.
        /// </summary>
        /// <param name="cacheNames">Names of caches to reset partitions for.</param>
        void ResetLostPartitions(IEnumerable<string> cacheNames);

        /// <summary>
        /// Clears partitions' lost state and moves caches to a normal mode.
        /// </summary>
        /// <param name="cacheNames">Names of caches to reset partitions for.</param>
        void ResetLostPartitions(params string[] cacheNames);

        /// <summary>
        /// Gets a collection of memory metrics, one for each <see cref="MemoryConfiguration.MemoryPolicies"/>.
        /// <para />
        /// Memory metrics should be enabled with <see cref="MemoryPolicyConfiguration.MetricsEnabled"/>.
        /// <para />
        /// Obsolete, use <see cref="GetDataRegionMetrics()"/>.
        /// </summary>
        [Obsolete("Use GetDataRegionMetrics.")]
        ICollection<IMemoryMetrics> GetMemoryMetrics();

        /// <summary>
        /// Gets the memory metrics for the specified memory policy.
        /// <para />
        /// To get metrics for the default memory region,
        /// use <see cref="MemoryConfiguration.DefaultMemoryPolicyName"/>.
        /// <para />
        /// Obsolete, use <see cref="GetDataRegionMetrics(string)"/>.
        /// </summary>
        /// <param name="memoryPolicyName">Name of the memory policy.</param>
        [Obsolete("Use GetDataRegionMetrics.")]
        IMemoryMetrics GetMemoryMetrics(string memoryPolicyName);

        /// <summary>
        /// Changes Ignite grid state to active or inactive.
        /// </summary>
        [Obsolete("Use GetCluster().SetActive instead.")]
        void SetActive(bool isActive);

        /// <summary>
        /// Determines whether this grid is in active state.
        /// </summary>
        /// <returns>
        ///   <c>true</c> if the grid is active; otherwise, <c>false</c>.
        /// </returns>
        [Obsolete("Use GetCluster().IsActive instead.")]
        bool IsActive();

        /// <summary>
        /// Gets the persistent store metrics.
        /// <para />
        /// To enable metrics set <see cref="PersistentStoreConfiguration.MetricsEnabled"/> property
        /// in <see cref="IgniteConfiguration.PersistentStoreConfiguration"/>.
        /// </summary>
        [Obsolete("Use GetDataStorageMetrics.")]
        IPersistentStoreMetrics GetPersistentStoreMetrics();

        /// <summary>
        /// Gets a collection of memory metrics, one for each
        /// <see cref="DataStorageConfiguration.DataRegionConfigurations"/>.
        /// <para />
        /// Metrics should be enabled with <see cref="DataStorageConfiguration.MetricsEnabled"/>.
        /// </summary>
        ICollection<IDataRegionMetrics> GetDataRegionMetrics();

        /// <summary>
        /// Gets the memory metrics for the specified data region.
        /// <para />
        /// To get metrics for the default memory region,
        /// use <see cref="DataStorageConfiguration.DefaultDataRegionName"/>.
        /// </summary>
        /// <param name="dataRegionName">Name of the data region.</param>
        IDataRegionMetrics GetDataRegionMetrics(string dataRegionName);

        /// <summary>
        /// Gets the persistent store metrics.
        /// <para />
        /// To enable metrics set <see cref="DataStorageConfiguration.MetricsEnabled"/> property
        /// in <see cref="IgniteConfiguration.DataStorageConfiguration"/>.
        /// </summary>
        IDataStorageMetrics GetDataStorageMetrics();

        /// <summary>
        /// Adds cache configuration template. Name should contain *.
        /// Template settings are applied to a cache created with <see cref="CreateCache{K,V}(string)"/> if specified
        /// name matches the template name.
        /// </summary>
        /// <param name="configuration">Configuration.</param>
        void AddCacheConfiguration(CacheConfiguration configuration);

        /// <summary>
        /// Gets or creates a distributed reentrant lock (monitor) with default configuration.
        /// </summary>
        /// <param name="name">Lock name.</param>
        /// <returns><see cref="IIgniteLock"/></returns>
        IIgniteLock GetOrCreateLock(string name);

        /// <summary>
        /// Gets or creates a distributed reentrant lock (monitor).
        /// </summary>
        /// <param name="configuration">Lock configuration.</param>
        /// <param name="create">Whether the lock should be created if it does not exist.</param>
        /// <returns><see cref="IIgniteLock"/></returns>
        IIgniteLock GetOrCreateLock(LockConfiguration configuration, bool create);
    }
}
