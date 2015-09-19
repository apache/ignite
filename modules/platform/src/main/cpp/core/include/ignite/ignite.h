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

#ifndef _IGNITE
#define _IGNITE

#include "ignite/cache/cache.h"
#include "ignite/impl/ignite_impl.h"
#include "ignite/ignite_configuration.h"

namespace ignite
{
    /**
     * Main interface to operate with Ignite.
     */
    class IGNITE_IMPORT_EXPORT Ignite
    {
    public:
        /**
         * Default constructor.
         */
        Ignite();

        /**
         * Constructor.
         */
        Ignite(impl::IgniteImpl* impl);
        
        /**
         * Get Ignite instance name.
         *
         * @return Name.
         */
        char* GetName();

        /**
         * Get cache.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = GetCache<K, V>(name, &err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Get cache.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetCache(const char* name, IgniteError* err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->GetCache<K, V>(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }

        /**
         * Get or create cache.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetOrCreateCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = GetOrCreateCache<K, V>(name, &err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Get or create cache.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetOrCreateCache(const char* name, IgniteError* err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->GetOrCreateCache<K, V>(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }

        /**
         * Create cache.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> CreateCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = CreateCache<K, V>(name, &err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Create cache.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> CreateCache(const char* name, IgniteError* err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->CreateCache<K, V>(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }
    private:
        /** Implementation delegate. */
        ignite::common::concurrent::SharedPointer<impl::IgniteImpl> impl;
    };
}

#endif
