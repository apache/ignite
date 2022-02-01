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
 * Declares ignite::thin::IgniteClient class.
 */

#ifndef _IGNITE_THIN_IGNITE_CLIENT
#define _IGNITE_THIN_IGNITE_CLIENT

#include <vector>

#include <ignite/common/concurrent.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/cache/cache_client.h>
#include <ignite/thin/compute/compute_client.h>
#include <ignite/thin/transactions/transactions.h>

namespace ignite
{
    namespace thin
    {
        /**
         * Ignite client class.
         *
         * This is an entry point for Thin C++ Ignite client. Its main purpose is to establish connection to the remote
         * server nodes.
         *
         * This class is implemented as a reference to an implementation so copying of this class instance will only
         * create another reference to the same underlying object. Underlying object will be released automatically once
         * all the instances are destructed.
         */
        class IGNITE_IMPORT_EXPORT IgniteClient
        {
            typedef common::concurrent::SharedPointer<void> SP_Void;
        public:
            /**
             * Default constructor.
             */
            IgniteClient()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~IgniteClient();

            /**
             * Start client.
             *
             * @param cfg Client configuration.
             * @return IgniteClient instance.
             * @throw IgnitError on inability to connect.
             */
            static IgniteClient Start(const IgniteClientConfiguration& cfg);

            /**
             * Get cache.
             *
             * @param name Cache name.
             * @return Cache.
             */
            template<typename K, typename V>
            cache::CacheClient<K, V> GetCache(const char* name)
            {
                return cache::CacheClient<K, V>(InternalGetCache(name));
            }

            /**
             * Get or create cache.
             *
             * @param name Cache name.
             * @return Cache.
             */
            template<typename K, typename V>
            cache::CacheClient<K, V> GetOrCreateCache(const char* name)
            {
                return cache::CacheClient<K, V>(InternalGetOrCreateCache(name));
            }

            /**
             * Create cache.
             *
             * @param name Cache name.
             * @return Cache.
             */
            template<typename K, typename V>
            cache::CacheClient<K, V> CreateCache(const char* name)
            {
                return cache::CacheClient<K, V>(InternalCreateCache(name));
            }

            /**
             * Destroy cache by name.
             *
             * @param name Cache name.
             */
            void DestroyCache(const char* name);

            /**
             * Get names of currently available caches or an empty collection
             * if no caches are available.
             *
             * @param cacheNames Cache names. Output parameter.
             */
            void GetCacheNames(std::vector<std::string>& cacheNames);

            /**
             * Starts transactions.
             */
            transactions::ClientTransactions ClientTransactions()
            {
                return transactions::ClientTransactions(InternalTransactions());
            }

            /**
             * Get client compute API.
             */
            compute::ComputeClient GetCompute()
            {
                return compute::ComputeClient(InternalCompute());
            }

        private:
            /**
             * Get cache.
             * Internal call.
             *
             * @param name Cache name.
             * @return Cache.
             */
            SP_Void InternalGetCache(const char* name);

            /**
             * Get or create cache.
             * Internal call.
             *
             * @param name Cache name.
             * @return Cache.
             */
            SP_Void InternalGetOrCreateCache(const char* name);

            /**
             * Create cache.
             * Internal call.
             *
             * @param name Cache name.
             * @return Cache.
             */
            SP_Void InternalCreateCache(const char* name);

            /**
             * Get transactions.
             *
             * Internal call.
             * @return Transactions impl.
             */
            SP_Void InternalTransactions();

            /**
             * Get compute.
             *
             * Internal call.
             * @return Compute impl.
             */
            SP_Void InternalCompute();

            /**
             * Constructor.
             *
             * @param impl Implementation.
             */
            IgniteClient(SP_Void& impl);

            /** Implementation. */
            SP_Void impl;
        };
    }
}

#endif // _IGNITE_THIN_IGNITE_CLIENT
