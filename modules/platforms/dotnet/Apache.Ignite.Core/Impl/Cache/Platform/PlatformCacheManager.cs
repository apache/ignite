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

namespace Apache.Ignite.Core.Impl.Cache.Platform
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Manages <see cref="PlatformCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="PlatformCache{TK,TV}"/> instance.
    /// </summary>
    [DebuggerDisplay("PlatformCacheManager [IgniteInstanceName={_ignite.GetIgnite().Name}]")]
    internal class PlatformCacheManager
    {
        /// <summary>
        /// Holds thread-local key/val pair to be used for updating platform cache.
        /// </summary>
        internal static readonly ThreadLocal<object> ThreadLocalPair = new ThreadLocal<object>();

        /// <summary>
        /// Platform caches per cache id.
        /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can point to the same Ignite cache,
        /// and share one <see cref="PlatformCache{TK,TV}"/> instance.
        /// </summary>
        private readonly CopyOnWriteConcurrentDictionary<int, IPlatformCache> _caches
            = new CopyOnWriteConcurrentDictionary<int, IPlatformCache>();

        /// <summary>
        /// Ignite.
        /// </summary>
        private readonly IIgniteInternal _ignite;

        /// <summary>
        /// Current topology version. Store as object for atomic updates.
        /// </summary>
        private volatile object _affinityTopologyVersion;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformCacheManager"/> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        public PlatformCacheManager(IIgniteInternal ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
            _ignite.GetIgnite().ClientDisconnected += OnClientDisconnected;
        }

        /// <summary>
        /// Gets or creates the platform cache.
        /// </summary>
        public IPlatformCache GetOrCreatePlatformCache(CacheConfiguration cacheConfiguration)
        {
            Debug.Assert(cacheConfiguration != null);

            var cacheId = BinaryUtils.GetCacheId(cacheConfiguration.Name);

            return _caches.GetOrAdd(cacheId, _ => CreatePlatformCache(cacheConfiguration));
        }

        /// <summary>
        /// Gets platform cache when it exists.
        /// </summary>
        public IPlatformCache TryGetPlatformCache(int cacheId)
        {
            IPlatformCache platformCache;
            return _caches.TryGetValue(cacheId, out platformCache) ? platformCache : null;
        }

        /// <summary>
        /// Reads cache entry from a stream and updates the platform cache.
        /// </summary>
        public void Update(int cacheId, IBinaryStream stream, Marshaller marshaller)
        {
            var cache = _caches.GetOrAdd(cacheId,
                _ => CreatePlatformCache(_ignite.GetCacheConfiguration(cacheId)));

            cache.Update(stream, marshaller);
        }

        /// <summary>
        /// Updates platform cache from <see cref="ThreadLocalPair"/>.
        /// </summary>
        public void UpdateFromThreadLocal(int cacheId, int partition, AffinityTopologyVersion affinityTopologyVersion)
        {
            var cache = TryGetPlatformCache(cacheId);

            if (cache != null)
            {
                cache.UpdateFromThreadLocal(partition, affinityTopologyVersion);
            }
        }

        /// <summary>
        /// Stops platform cache.
        /// </summary>
        public void Stop(int cacheId)
        {
            IPlatformCache cache;
            if (_caches.Remove(cacheId, out cache))
            {
                cache.Stop();
            }
        }

        /// <summary>
        /// Called when topology version changes.
        /// </summary>
        public void OnAffinityTopologyVersionChanged(AffinityTopologyVersion affinityTopologyVersion)
        {
            _affinityTopologyVersion = affinityTopologyVersion;
        }

        /// <summary>
        /// Creates platform cache.
        /// </summary>
        private IPlatformCache CreatePlatformCache(CacheConfiguration cacheConfiguration)
        {
            var platformCfg = cacheConfiguration.PlatformCacheConfiguration;
            Debug.Assert(platformCfg != null);

            Func<object> affinityTopologyVersionFunc = () => _affinityTopologyVersion;
            var affinity = _ignite.GetAffinityManager(cacheConfiguration.Name);
            var keepBinary = platformCfg.KeepBinary;

            TypeResolver resolver = null;
            Func<string, string, Type> resolve = (typeName, fieldName) =>
            {
                if (typeName == null)
                {
                    return typeof(object);
                }

                if (resolver == null)
                {
                    resolver = new TypeResolver();
                }

                var resolved = resolver.ResolveType(typeName);

                if (resolved == null)
                {
                    throw new InvalidOperationException(string.Format(
                        "Can not create .NET Platform Cache: {0}.{1} is invalid. Failed to resolve type: '{2}'",
                        typeof(PlatformCacheConfiguration).Name, fieldName, typeName));
                }

                return resolved;
            };

            var keyType = resolve(platformCfg.KeyTypeName, "KeyTypeName");
            var valType = resolve(platformCfg.ValueTypeName, "ValueTypeName");
            var cacheType = typeof(PlatformCache<,>).MakeGenericType(keyType, valType);

            var platformCache = Activator.CreateInstance(
                cacheType,
                affinityTopologyVersionFunc,
                affinity,
                keepBinary);

            return (IPlatformCache) platformCache;
        }

        /// <summary>
        /// Handles client disconnect.
        /// </summary>
        private void OnClientDisconnected(object sender, EventArgs e)
        {
            foreach (var cache in _caches)
            {
                cache.Value.Clear();
            }
        }
    }
}
