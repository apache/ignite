/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain
{
    using System;

    using GridGain.Cache;
    using GridGain.Cluster;
    using GridGain.Compute;
    using GridGain.DataCenterReplication;
    using GridGain.Datastream;
    using GridGain.Events;
    using GridGain.Portable;
    using GridGain.Product;
    using GridGain.Services;
    using GridGain.Security;
    using GridGain.Transactions;

    /// <summary>
    /// Main entry point for all GridGain APIs.
    /// You can obtain an instance of <c>IGrid</c> through <see cref="GridFactory.Grid()"/>,
    /// or for named grids you can use <see cref="GridFactory.Grid(string)"/>. Note that you
    /// can have multiple instances of <c>IGrid</c> running in the same process by giving
    /// each instance a different name.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IGrid : IDisposable
    {
        /// <summary>
        /// Gets the name of the grid this grid instance (and correspondingly its local node) belongs to.
        /// Note that single process can have multiple grid instances all belonging to different grids. Grid
        /// name allows to indicate to what grid this particular grid instance (i.e. grid runtime and its
        /// local node) belongs to.
        /// <p/>
        /// If default grid instance is used, then <c>null</c> is returned. Refer to <see cref="GridFactory"/> documentation
        /// for information on how to start named grids.
        /// </summary>
        /// <returns>Name of the grid, or <c>null</c> for default grid.</returns>
        string Name { get; }

        /// <summary>
        /// Gets an instance of <see cref="ICluster" /> interface.
        /// </summary>
        ICluster Cluster { get; }

        /// <summary>
        /// Gets compute functionality over this grid projection. All operations
        /// on the returned ICompute instance will only include nodes from
        /// this projection.
        /// </summary>
        /// <returns>Compute instance over this grid projection.</returns>
        ICompute Compute();

        /// <summary>
        /// Gets compute functionality over specified grid projection. All operations
        /// on the returned ICompute instance will only include nodes from
        /// that projection.
        /// </summary>
        /// <returns>Compute instance over specified grid projection.</returns>
        ICompute Compute(IClusterGroup clusterGroup);

        /// <summary>
        /// Gets the cache instance for the given name to work with keys and values of specified types.
        /// <para/>
        /// You can get instances of ICache of the same name, but with different key/value types.
        /// These will use the same named cache, but only allow working with entries of specified types.
        /// Attempt to retrieve an entry of incompatible type will result in <see cref="InvalidCastException"/>.
        /// Use <see cref="Cache{Object, Object}"/> in order to work with entries of arbitrary types.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <returns>Cache instance for given name.</returns>
        /// <typeparam name="K">Cache key type.</typeparam>
        /// <typeparam name="V">Cache value type.</typeparam>
        ICache<K, V> Cache<K, V>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using template configuration.
        /// </summary>
        /// <typeparam name="K">Cache key type.</typeparam>
        /// <typeparam name="V">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<K, V> GetOrCreateCache<K, V>(string name);

        /// <summary>
        /// Dynamically starts new cache using template configuration.
        /// </summary>
        /// <typeparam name="K">Cache key type.</typeparam>
        /// <typeparam name="V">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<K, V> CreateCache<K, V>(string name);

        /// <summary>
        /// Gets a new instance of data streamer associated with given cache name. Data streamer
        /// is responsible for loading external data into in-memory data grid. For more information
        /// refer to <see cref="IDataStreamer{K,V}"/> documentation.
        /// </summary>
        /// <param name="cacheName">Cache name (<c>null</c> for default cache).</param>
        /// <returns>Data streamer.</returns>
        IDataStreamer<K, V> DataStreamer<K, V>(string cacheName);

        /// <summary>
        /// Gets an instance of <see cref="IPortables"/> interface.
        /// </summary>
        /// <returns>Instance of <see cref="IPortables"/> interface</returns>
        IPortables Portables();

        /// <summary>
        /// Gets affinity service to provide information about data partitioning and distribution.
        /// </summary>
        /// <param name="name">Cache name.</param>
        /// <returns>Cache data affinity service.</returns>
        ICacheAffinity Affinity(string name);

        /// <summary>
        /// Gets grid transactions facade.
        /// </summary>
        ITransactions Transactions { get; }

        /// <summary>
        /// Gets grid security facade.
        /// </summary>
        ISecurity Security { get; }

        /// <summary>
        /// Gets messaging facade over all cluster nodes.
        /// </summary>
        /// <returns>Messaging instance over all cluster nodes.</returns>
        IMessaging Message();

        /// <summary>
        /// Gets messaging facade over nodes within the cluster group.  All operations on the returned 
        /// <see cref="IMessaging"/>> instance will only include nodes from the specified cluster group.
        /// </summary>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <returns>Messaging instance over given cluster group.</returns>
        IMessaging Message(IClusterGroup clusterGroup);

        /// <summary>
        /// Gets events facade over all cluster nodes.
        /// </summary>
        /// <returns>Events facade over all cluster nodes.</returns>
        IEvents Events();

        /// <summary>
        /// Gets events facade over nodes within the cluster group.  All operations on the returned 
        /// <see cref="IEvents"/>> instance will only include nodes from the specified cluster group.
        /// </summary>
        /// <param name="clusterGroup">Cluster group.</param>
        /// <returns>Events instance over given cluster group.</returns>
        IEvents Events(IClusterGroup clusterGroup);

        /// <summary>
        /// Gets information about product and license management capabilities.
        /// </summary>
        IProduct Product { get; }

        /// <summary>
        /// Gets services facade over all cluster nodes.
        /// </summary>
        /// <returns>Services facade over all cluster nodes.</returns>
        IServices Services();

        /// <summary>
        /// Gets an instance of Data Center Replication.
        /// </summary>
        IDataCenterReplication DataCenterReplication { get; }
    }
}
