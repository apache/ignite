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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Platform;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Plugin;

    /// <summary>
    /// Internal Ignite interface.
    /// </summary>
    internal interface IIgniteInternal
    {
        /// <summary>
        /// Gets the binary processor.
        /// </summary>
        IBinaryProcessor BinaryProcessor { get; }

        /// <summary>
        /// Configuration.
        /// </summary>
        IgniteConfiguration Configuration { get; }

        /// <summary>
        /// Handle registry.
        /// </summary>
        HandleRegistry HandleRegistry { get; }

        /// <summary>
        /// Gets the node from cache.
        /// </summary>
        /// <param name="id">Node id.</param>
        /// <returns>Cached node.</returns>
        ClusterNodeImpl GetNode(Guid? id);

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        Marshaller Marshaller { get; }

        /// <summary>
        /// Gets the plugin processor.
        /// </summary>
        PluginProcessor PluginProcessor { get; }

        /// <summary>
        /// Gets the platform cache manager.
        /// </summary>
        PlatformCacheManager PlatformCacheManager { get; }

        /// <summary>
        /// Gets the data streamer.
        /// </summary>
        IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName, bool keepBinary);

        /// <summary>
        /// Gets the public Ignite API.
        /// </summary>
        IIgnite GetIgnite();

        /// <summary>
        /// Gets the binary API.
        /// </summary>
        IBinary GetBinary();

        /// <summary>
        /// Gets internal affinity service for a given cache.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        /// <returns>Cache data affinity service.</returns>
        CacheAffinityImpl GetAffinity(string cacheName);

        /// <summary>
        /// Gets internal affinity manager for a given cache.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        /// <returns>Cache affinity manager.</returns>
        CacheAffinityManager GetAffinityManager(string cacheName);

        /// <summary>
        /// Gets cache name by id.
        /// </summary>
        /// <param name="cacheId">Cache id.</param>
        /// <returns>Cache name.</returns>
        CacheConfiguration GetCacheConfiguration(int cacheId);

        /// <summary>
        /// Gets platform-specific thread local value from Java.
        /// </summary>
        /// <returns>Thread local value from Java.</returns>
        object GetJavaThreadLocal();
    }
}
