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

/**
 * @file
 * Declares ignite::Ignite class.
 */

#ifndef _IGNITE_IGNITE
#define _IGNITE_IGNITE

#include <ignite/impl/ignite_impl.h>

#include <ignite/ignite_configuration.h>
#include <ignite/cache/cache.h>
#include <ignite/cache/cache_affinity.h>
#include <ignite/transactions/transactions.h>
#include <ignite/compute/compute.h>
#include <ignite/cluster/ignite_cluster.h>

namespace ignite
{
    /**
     * Main interface to operate with %Ignite.
     *
     * This class is implemented as a reference to an implementation so copying
     * of this class instance will only create another reference to the same
     * underlying object. Underlying object will be released automatically once all
     * the instances are destructed.
     */
    class IGNITE_IMPORT_EXPORT Ignite
    {
        friend class impl::IgniteImpl;
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
         * Get affinity service to provide information about data partitioning and distribution.
         *
         * @tparam K Cache affinity key type.
         *
         * @param cacheName Cache name.
         * @return Cache data affinity service.
         */
        template<typename K>
        cache::CacheAffinity<K> GetAffinity(const std::string& cacheName)
        {
            IgniteError err;

            cache::CacheAffinity<K> ret(impl.Get()->GetAffinity(cacheName, err));

            IgniteError::ThrowIfNeeded(err);

            return ret;
        }

        /**
         * Get Ignite instance name.
         *
         * @return Name.
         */
        const char* GetName() const;

        /**
         * Get node configuration.
         *
         * This method should only be used on the valid instance.
         *
         * @return Node configuration.
         */
        const IgniteConfiguration& GetConfiguration() const;

        /**
         * Get cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = GetCache<K, V>(name, err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Get cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetCache(const char* name, IgniteError& err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->GetCache(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }

        /**
         * Get or create cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetOrCreateCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = GetOrCreateCache<K, V>(name, err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Get or create cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> GetOrCreateCache(const char* name, IgniteError& err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->GetOrCreateCache(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }

        /**
         * Create cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> CreateCache(const char* name)
        {
            IgniteError err;

            cache::Cache<K, V> res = CreateCache<K, V>(name, err);

            IgniteError::ThrowIfNeeded(err);

            return res;
        }

        /**
         * Create cache.
         *
         * This method should only be used on the valid instance.
         *
         * @param name Cache name.
         * @param err Error;
         * @return Cache.
         */
        template<typename K, typename V>
        cache::Cache<K, V> CreateCache(const char* name, IgniteError& err)
        {
            impl::cache::CacheImpl* cacheImpl = impl.Get()->CreateCache(name, err);

            return cache::Cache<K, V>(cacheImpl);
        }

        /**
         * Check if the Ignite grid is active.
         *
         * @return True if grid is active and false otherwise.
         */
        bool IsActive();

        /**
         * Change Ignite grid state to active or inactive.
         *
         * @param active If true start activation process. If false start
         *    deactivation process.
         */
        void SetActive(bool active);

        /**
         * Get transactions.
         *
         * This method should only be used on the valid instance.
         *
         * @return Transaction class instance.
         */
        transactions::Transactions GetTransactions();

        /**
         * Get cluster.
         *
         * This method should only be called on the valid instance.
         *
         * @return Cluster class instance.
         */
        cluster::IgniteCluster GetCluster();

        /**
         * Gets compute instance over all cluster nodes started in server mode.
         *
         * This method should only be called on the valid instance.
         *
         * @return Compute class instance.
         */
        compute::Compute GetCompute();

        /**
         * Gets compute instance over the specified cluster group. All operations
         * on the returned compute instance will only include nodes from
         * this cluster group.
         *
         * This method should only be called on the valid instance.
         *
         * @param grp Specified cluster group instance.
         * @return Compute class instance over the specified cluster group.
         */
        compute::Compute GetCompute(cluster::ClusterGroup grp);

        /**
         * Get ignite binding.
         *
         * This method should only be used on the valid instance.
         *
         * @return IgniteBinding class instance.
         */
        IgniteBinding GetBinding();

        /**
         * Check if the instance is valid.
         *
         * Invalid instance can be returned if some of the previous
         * operations have resulted in a failure. For example invalid
         * instance can be returned by not-throwing version of method
         * in case of error. Invalid instances also often can be
         * created using default constructor.
         * 
         * @return True if the instance is valid and can be used.
         */
        bool IsValid() const
        {
            return impl.IsValid();
        }

    private:
        /** Implementation delegate. */
        ignite::common::concurrent::SharedPointer<impl::IgniteImpl> impl;
    };
}

#endif //_IGNITE_IGNITE
