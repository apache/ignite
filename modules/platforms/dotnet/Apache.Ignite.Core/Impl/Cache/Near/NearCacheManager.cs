/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Cache.Near
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Manages <see cref="NearCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="NearCache{TK,TV}"/> instance.
    /// </summary>
    [DebuggerDisplay("NearCacheManager [IgniteInstanceName={_ignite.GetIgnite().Name}]")]
    internal class NearCacheManager
    {
        /// <summary>
        /// Near caches per cache id.
        /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can point to the same Ignite cache,
        /// and share one <see cref="NearCache{TK,TV}"/> instance. 
        /// </summary> 
        private readonly CopyOnWriteConcurrentDictionary<int, INearCache> _nearCaches
            = new CopyOnWriteConcurrentDictionary<int, INearCache>();

        /// <summary>
        /// Ignite.
        /// </summary>
        private readonly IIgniteInternal _ignite;

        /// <summary>
        /// Current topology version. Store as object for atomic updates.
        /// </summary>
        private volatile object _affinityTopologyVersion;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="NearCacheManager"/> class. 
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        public NearCacheManager(IIgniteInternal ignite)
        {
            Debug.Assert(ignite != null);

            _ignite = ignite;
            _ignite.GetIgnite().ClientDisconnected += OnClientDisconnected;
        }

        /// <summary>
        /// Gets or creates the near cache.
        /// </summary>
        public INearCache GetOrCreateNearCache(CacheConfiguration cacheConfiguration)
        {
            Debug.Assert(cacheConfiguration != null);

            var cacheId = BinaryUtils.GetCacheId(cacheConfiguration.Name);
            
            return _nearCaches.GetOrAdd(cacheId, _ => CreateNearCache(cacheConfiguration));
        }

        /// <summary>
        /// Gets near cache when it exists.
        /// </summary>
        public INearCache TryGetNearCache(int cacheId)
        {
            INearCache nearCache;
            return _nearCaches.TryGetValue(cacheId, out nearCache) ? nearCache : null;
        }
        
        /// <summary>
        /// Reads cache entry from a stream and updates the near cache.
        /// </summary>
        public void Update(int cacheId, IBinaryStream stream, Marshaller marshaller)
        {
            var nearCache = _nearCaches.GetOrAdd(cacheId, 
                _ => CreateNearCache(_ignite.GetCacheConfiguration(cacheId)));
            
            nearCache.Update(stream, marshaller);
        }

        /// <summary>
        /// Stops near cache.
        /// </summary>
        public void Stop(int cacheId)
        {
            INearCache cache;
            if (_nearCaches.Remove(cacheId, out cache))
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
        /// Creates near cache.
        /// </summary>
        private INearCache CreateNearCache(CacheConfiguration cacheConfiguration)
        {
            Debug.Assert(cacheConfiguration.NearConfiguration != null);
            Debug.Assert(cacheConfiguration.NearConfiguration.PlatformNearConfiguration != null);

            var nearCfg = cacheConfiguration.NearConfiguration.PlatformNearConfiguration;
            
            Func<object> affinityTopologyVersionFunc = () => _affinityTopologyVersion;
            var affinity = _ignite.GetAffinity(cacheConfiguration.Name);
            var keepBinary = nearCfg.KeepBinary;

            TypeResolver resolver = null;
            Func<string, Type> resolve = n =>
            {
                if (n == null)
                {
                    return typeof(object);
                }

                if (resolver == null)
                {
                    resolver = new TypeResolver();
                }

                var resolved = resolver.ResolveType(n);

                if (resolved == null)
                {
                    throw new InvalidOperationException(string.Format("Failed to resolve type: '{0}'", n));
                }

                return resolved;
            };

            var keyType = resolve(nearCfg.KeyTypeName);
            var valType = resolve(nearCfg.ValueTypeName);

            var cacheType = typeof(NearCache<,>).MakeGenericType(keyType, valType);
            var nearCache = Activator.CreateInstance(
                cacheType, 
                affinityTopologyVersionFunc, 
                affinity,
                keepBinary);
            
            return (INearCache) nearCache;
        }
        
        /// <summary>
        /// Handles client disconnect.
        /// </summary>
        private void OnClientDisconnected(object sender, EventArgs e)
        {
            foreach (var cache in _nearCaches)
            {
                cache.Value.Clear();
            }
        }
    }
}
