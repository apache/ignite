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

using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Eviction;
using Apache.Ignite.Core.Configuration;
using DataPageEvictionMode = Apache.Ignite.Core.Configuration.DataPageEvictionMode;

namespace dotnet_helloworld
{
    class EvictionPolicies
    {
        public static void RandomLRU()
        {
            // tag::randomLRU[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "20GB_Region",
                            InitialSize = 500L * 1024 * 1024,
                            MaxSize = 20L * 1024 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.RandomLru
                        }
                    }
                }
            };
            // end::randomLRU[]
        }

        public static void Random2LRU()
        {
            // tag::random2LRU[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "20GB_Region",
                            InitialSize = 500L * 1024 * 1024,
                            MaxSize = 20L * 1024 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.Random2Lru
                        }
                    }
                }
            };
            // end::random2LRU[]
        }

        public static void LRU()
        {
            // tag::LRU[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "cacheName",
                        OnheapCacheEnabled = true,
                        EvictionPolicy = new LruEvictionPolicy
                        {
                            MaxSize = 100000
                        }
                    }
                }
            };
            // end::LRU[]
        }

        public static void FIFO()
        {
            // tag::FIFO[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "cacheName",
                        OnheapCacheEnabled = true,
                        EvictionPolicy = new FifoEvictionPolicy
                        {
                            MaxSize = 100000
                        }
                    }
                }
            };
            // end::FIFO[]
        }
    }
}
