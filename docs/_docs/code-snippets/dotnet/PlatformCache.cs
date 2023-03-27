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
using System;
using System.Collections.Generic;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Binary;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Eviction;

namespace dotnet_helloworld
{
    public class PlatformCache
    {
        public static void ConfigurePlatformCacheOnServer()
        {
            var ignite = Ignition.Start();
            
            //tag::platformCacheConf[]
            var cacheCfg = new CacheConfiguration("my-cache")
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration()
            };

            var cache = ignite.CreateCache<int, string>(cacheCfg);
            //end::platformCacheConf[]
        }
        
        public static void ConfigurePlatformCacheOnClient()
        {
            var ignite = Ignition.Start();
            
            //tag::platformCacheConfClient[]
            var nearCacheCfg = new NearCacheConfiguration
            {
                // Keep up to 1000 most recently used entries in Near and Platform caches.
                EvictionPolicy = new LruEvictionPolicy
                {
                    MaxSize = 1000
                }
            };
            
            var cache = ignite.CreateNearCache<int, string>("my-cache",
                nearCacheCfg,
                new PlatformCacheConfiguration());
            //end::platformCacheConfClient[]
        }

        public static void AccessPlatformCache()
        {
            var ignite = Ignition.Start();

            //tag::platformCacheAccess[]
            var cache = ignite.GetCache<int, string>("my-cache");
            
            // Get value from platform cache.
            bool hasKey = cache.TryLocalPeek(1, out var val, CachePeekMode.Platform);
            
            // Get platform cache size (current number of entries on local node).
            int size = cache.GetLocalSize(CachePeekMode.Platform);
            
            // Get all values from platform cache.
            IEnumerable<ICacheEntry<int, string>> entries = cache.GetLocalEntries(CachePeekMode.Platform);
            
            //end::platformCacheAccess[]
        }

        public static void AdvancedConfigBinaryMode()
        {
            var ignite = Ignition.Start();
            
            //tag::advancedConfigBinaryMode[]
            var cacheCfg = new CacheConfiguration("people")
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    KeepBinary = true
                }
            };

            var cache = ignite.CreateCache<int, Person>(cacheCfg)
                .WithKeepBinary<int, IBinaryObject>();

            IBinaryObject binaryPerson = cache.Get(1);
            //end::advancedConfigBinaryMode[]
        }
        
        public static void AdvancedConfigKeyValTypes()
        {
            var ignite = Ignition.Start();
            
            //tag::advancedConfigKeyValTypes[]
            var cacheCfg = new CacheConfiguration("people")
            {
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    KeyTypeName = typeof(long).FullName,
                    ValueTypeName = typeof(Guid).FullName
                }
            };

            var cache = ignite.CreateCache<long, Guid>(cacheCfg);
            //end::advancedConfigKeyValTypes[]
        }
    }
}
