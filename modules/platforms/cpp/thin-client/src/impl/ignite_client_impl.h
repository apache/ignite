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

#ifndef _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL
#define _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

#include "impl/data_router.h"
#include "impl/cache/cache_client_impl.h"
#include "impl/transactions/transactions_impl.h"
#include "impl/compute/compute_client_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Ignite client class implementation.
             *
             * This is an entry point for Thin C++ Ignite client. Its main
             * purpose is to establish connection to the remote server node.
             */
            class IgniteClientImpl
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cfg Configuration.
                 */
                IgniteClientImpl(const ignite::thin::IgniteClientConfiguration& cfg);

                /**
                 * Destructor.
                 */
                ~IgniteClientImpl();

                /**
                 * Start client.
                 *
                 * @throw IgnitError on inability to connect.
                 */
                void Start();

                /**
                 * Get cache.
                 *
                 * @param name Cache name.
                 * @return Cache.
                 */
                common::concurrent::SharedPointer<cache::CacheClientImpl> GetCache(const char* name) const;

                /**
                 * Get or create cache.
                 *
                 * @param name Cache name.
                 * @return Cache.
                 */
                common::concurrent::SharedPointer<cache::CacheClientImpl> GetOrCreateCache(const char* name);

                /**
                 * Create cache.
                 *
                 * @param name Cache name.
                 * @return Cache.
                 */
                common::concurrent::SharedPointer<cache::CacheClientImpl> CreateCache(const char* name);

                /**
                 * Get transactions.
                 *
                 * @return Transactions impl.
                 */
                transactions::SP_TransactionsImpl ClientTransactions() const
                {
                    return txImpl;
                }

                /**
                 * Get client compute API.
                 */
                compute::SP_ComputeClientImpl GetCompute()
                {
                    return computeImpl;
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

            private:

                /**
                 * Make cache implementation.
                 *
                 * @param router Data router instance.
                 * @param name Cache name.
                 * @param id Cache ID.
                 * @return Cache implementation.
                 */
                static common::concurrent::SharedPointer<cache::CacheClientImpl> MakeCacheImpl(
                        const SP_DataRouter& router,
                        const transactions::SP_TransactionsImpl& tx,
                        const std::string& name,
                        int32_t id);

                /**
                 * Check cache name.
                 *
                 * @param name Cache name.
                 * @throw IgniteError if the name is not valid.
                 */
                static void CheckCacheName(const char* name);

                /** Configuration. */
                const ignite::thin::IgniteClientConfiguration cfg;

                /** Data router. */
                SP_DataRouter router;

                /** Transactions. */
                transactions::SP_TransactionsImpl txImpl;

                /** Compute. */
                compute::SP_ComputeClientImpl computeImpl;
            };
        }
    }
}
#endif // _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL
