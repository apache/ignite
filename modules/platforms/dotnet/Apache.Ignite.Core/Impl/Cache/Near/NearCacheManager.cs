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

namespace Apache.Ignite.Core.Impl.Cache.Near
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
    /// Manages <see cref="NearCache{TK,TV}"/> instances.
    /// Multiple <see cref="CacheImpl{TK,TV}"/> instances can exist for a given cache, and all of them share the same
    /// <see cref="NearCache{TK,TV}"/> instance.
    /// </summary>
    [DebuggerDisplay("NearCacheManager [IgniteInstanceName={_ignite.GetIgnite().Name}]")]
    internal class NearCacheManager
    {
        /// <summary>
        /// Holds thread-local key/val pair to be used for updating Near Cache .
        /// </summary>
        internal static readonly ThreadLocal<object> ThreadLocalPair = new ThreadLocal<object>();
        
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
        /// Updates near cache from <see cref="ThreadLocalPair"/>.
        /// </summary>
        public void UpdateFromThreadLocal(int cacheId, int partition, AffinityTopologyVersion affinityTopologyVersion)
        {
            var nearCache = TryGetNearCache(cacheId);

            if (nearCache != null)
            {
                nearCache.UpdateFromThreadLocal(partition, affinityTopologyVersion);
            }
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
            var nearCfg = cacheConfiguration.PlatformNearConfiguration;
            Debug.Assert(nearCfg != null);
            
            Func<object> affinityTopologyVersionFunc = () => _affinityTopologyVersion;
            var affinity = _ignite.GetAffinity(cacheConfiguration.Name);
            var keepBinary = nearCfg.KeepBinary;

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
                        "Can not create .NET Near Cache: {0}.{1} is invalid. Failed to resolve type: '{2}'", 
                        typeof(PlatformNearCacheConfiguration).Name, fieldName, typeName));
                }

                return resolved;
            };

            var keyType = resolve(nearCfg.KeyTypeName, "KeyTypeName");
            var valType = resolve(nearCfg.ValueTypeName, "ValueTypeName");

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
