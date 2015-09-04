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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Main entry point for all Ignite APIs.
    /// You can obtain an instance of <c>IGrid</c> through <see cref="Ignition.GetIgnite()"/>,
    /// or for named grids you can use <see cref="Ignition.GetIgnite(string)"/>. Note that you
    /// can have multiple instances of <c>IGrid</c> running in the same process by giving
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
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        ICache<TK, TV> Cache<TK, TV>(string name);

        /// <summary>
        /// Gets existing cache with the given name or creates new one using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> GetOrCreateCache<TK, TV>(string name);

        /// <summary>
        /// Dynamically starts new cache using template configuration.
        /// </summary>
        /// <typeparam name="TK">Cache key type.</typeparam>
        /// <typeparam name="TV">Cache value type.</typeparam>
        /// <param name="name">Cache name.</param>
        /// <returns>Existing or newly created cache.</returns>
        ICache<TK, TV> CreateCache<TK, TV>(string name);

        /// <summary>
        /// Gets a new instance of data streamer associated with given cache name. Data streamer
        /// is responsible for loading external data into Ignite. For more information
        /// refer to <see cref="IDataStreamer{K,V}"/> documentation.
        /// </summary>
        /// <param name="cacheName">Cache name (<c>null</c> for default cache).</param>
        /// <returns>Data streamer.</returns>
        IDataStreamer<TK, TV> DataStreamer<TK, TV>(string cacheName);

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
        /// Gets  Ignite transactions facade.
        /// </summary>
        ITransactions Transactions { get; }

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
        /// Gets services facade over all cluster nodes.
        /// </summary>
        /// <returns>Services facade over all cluster nodes.</returns>
        IServices Services();
    }
}
