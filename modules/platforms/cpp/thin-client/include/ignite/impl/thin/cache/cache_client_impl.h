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

#ifndef _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_IMPL
#define _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_IMPL

#include <stdint.h>
#include <string>

#include <ignite/impl/thin/data_router.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class Readable;

            /* Forward declaration. */
            class Writable;

            /* Forward declaration. */
            class WritableKey;

            namespace cache
            {
                /**
                 * Ignite client class implementation.
                 *
                 * This is an entry point for Thin C++ Ignite client. Its main
                 * purpose is to establish connection to the remote server node.
                 */
                class CacheClientImpl
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router instance.
                     * @param name Cache name.
                     * @param id Cache ID.
                     */
                    CacheClientImpl(const SP_DataRouter& router, const std::string& name, int32_t id);

                    /**
                     * Destructor.
                     */
                    ~CacheClientImpl();

                    /**
                     * Put value to cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Put(const WritableKey& key, const Writable& value);

                    /**
                     * Get value from cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Get(const WritableKey& key, Readable& value);

                    /**
                     * Check if the cache contains a value for the specified key.
                     *
                     * @param key Key whose presence in this cache is to be tested.
                     * @return @c true if the cache contains specified key.
                     */
                    bool ContainsKey(const WritableKey& key);

                    /**
                     * Update cache partitions info.
                     */
                    void UpdatePartitions();

                private:
                    /**
                     * Get end points for the key.
                     * Always using Rendezvous Affinity Function algorithm for now.
                     *
                     * @param key Key.
                     */
                    const std::vector<net::EndPoint>& GetEndPointsForKey(const WritableKey& key) const;

                    /**
                     * Synchronously send request message and receive response.
                     *
                     * @param key Key.
                     * @param req Request message.
                     * @param rsp Response message.
                     * @throw IgniteError on error.
                     */
                    template<typename ReqT, typename RspT>
                    void SyncCacheKeyMessage(const WritableKey& key, const ReqT& req, RspT& rsp);

                    /** Data router. */
                    SP_DataRouter router;

                    /** Cache name. */
                    std::string name;

                    /** Cache ID. */
                    int32_t id;

                    /** Binary flag. */
                    bool binary;

                    /** Partitions assignment. */
                    std::vector< std::vector<net::EndPoint> > assignment;

                    /** Rendezvous Affinity Function mask. */
                    int32_t mask;
                };

                typedef common::concurrent::SharedPointer<CacheClientImpl> SP_CacheClientImpl;
            }
        }
    }
}
#endif // _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_IMPL
