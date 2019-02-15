/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL
#define _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

#include "impl/data_router.h"
#include "impl/cache/cache_client_impl.h"

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
            };
        }
    }
}
#endif // _IGNITE_IMPL_THIN_IGNITE_CLIENT_IMPL
