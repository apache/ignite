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

#ifndef _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY
#define _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY

#include <ignite/common/concurrent.h>

#include <ignite/thin/cache/query/query_fields_cursor.h>
#include <ignite/thin/cache/query/query_sql_fields.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class Writable;
            
            /* Forward declaration. */
            class WritableKey;

            /* Forward declaration. */
            class Readable;

            namespace cache
            {
                /**
                 * Ignite client class proxy.
                 */
                class IGNITE_IMPORT_EXPORT CacheClientProxy
                {
                public:
                    /**
                     * Default constructor.
                     */
                    CacheClientProxy()
                    {
                        // No-op.
                    }

                    /**
                     * Constructor.
                     */
                    CacheClientProxy(const common::concurrent::SharedPointer<void>& impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~CacheClientProxy()
                    {
                        // No-op.
                    }

                    /**
                     * Put value to cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Put(const WritableKey& key, const Writable& value);

                    /**
                     * Stores given key-value pairs in cache.
                     * If write-through is enabled, the stored values will be persisted to store.
                     *
                     * @param pairs Writable key-value pair sequence.
                     */
                    void PutAll(const Writable& pairs);

                    /**
                     * Get value from cache.
                     *
                     * @param key Key.
                     * @param value Value.
                     */
                    void Get(const WritableKey& key, Readable& value);

                    /**
                     * Retrieves values mapped to the specified keys from cache.
                     * If some value is not present in cache, then it will be looked up from swap storage. If
                     * it's not present in swap, or if swap is disabled, and if read-through is allowed, value
                     * will be loaded from persistent store.
                     *
                     * @param keys Writable key sequence.
                     * @param pairs Readable key-value pair sequence.
                     */
                    void GetAll(const Writable& keys, Readable& pairs);

                    /**
                     * Stores given key-value pair in cache only if there is a previous mapping for it.
                     * If cache previously contained value for the given key, then this value is returned.
                     * In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node,
                     * which in its turn may load the value from the swap storage, and consecutively, if it's not
                     * in swap, rom the underlying persistent storage.
                     * If write-through is enabled, the stored value will be persisted to store.
                     *
                     * @param key Key to store in cache.
                     * @param value Value to be associated with the given key.
                     * @return True if the value was replaced.
                     */
                    bool Replace(const WritableKey& key, const Writable& value);

                    /**
                     * Check if the cache contains a value for the specified key.
                     *
                     * @param key Key whose presence in this cache is to be tested.
                     * @return @c true if the cache contains specified key.
                     */
                    bool ContainsKey(const WritableKey& key);

                    /**
                     * Check if cache contains mapping for these keys.
                     *
                     * @param keys Keys.
                     * @return True if cache contains mapping for all these keys.
                     */
                    bool ContainsKeys(const Writable& keys);

                    /**
                     * Gets the number of all entries cached across all nodes.
                     * @note This operation is distributed and will query all participating nodes for their cache sizes.
                     *
                     * @param peekModes Peek modes mask.
                     * @return Cache size across all nodes.
                     */
                    int64_t GetSize(int32_t peekModes);

                    /**
                     * Peeks at in-memory cached value using default optional peek mode. This method will not load value
                     * from any persistent store or from a remote node.
                     *
                     * Use for testing purposes only.
                     *
                     * @param key Key whose presence in this cache is to be tested.
                     * @param value Value.
                     */
                    void LocalPeek(const WritableKey& key, Readable& value);

                    /**
                     * Removes given key mapping from cache. If cache previously contained value for the given key,
                     * then this value is returned. In case of PARTITIONED or REPLICATED caches, the value will be
                     * loaded from the primary node, which in its turn may load the value from the disk-based swap
                     * storage, and consecutively, if it's not in swap, from the underlying persistent storage.
                     * If the returned value is not needed, method removex() should always be used instead of this
                     * one to avoid the overhead associated with returning of the previous value.
                     * If write-through is enabled, the value will be removed from store.
                     *
                     * @param key Key whose mapping is to be removed from cache.
                     * @return False if there was no matching key.
                     */
                    bool Remove(const WritableKey& key);

                    /**
                     * Removes given key mapping from cache if one exists and value is equal to the passed in value.
                     * If write-through is enabled, the value will be removed from store.
                     *
                     * @param key Key whose mapping is to be removed from cache.
                     * @param val Value to match against currently cached value.
                     * @return True if entry was removed, false otherwise.
                     */
                    bool Remove(const WritableKey& key, const Writable& val);

                    /**
                     * Removes given key mappings from cache.
                     * If write-through is enabled, the value will be removed from store.
                     *
                     * @param keys Keys whose mappings are to be removed from cache.
                     */
                    void RemoveAll(const Writable& keys);

                    /**
                     * Removes all mappings from cache.
                     * If write-through is enabled, the value will be removed from store.
                     */
                    void RemoveAll();

                    /**
                     * Clear entry from the cache and swap storage, without notifying listeners or CacheWriters.
                     * Entry is cleared only if it is not currently locked, and is not participating in a transaction.
                     *
                     * @param key Key to clear.
                     */
                    void Clear(const WritableKey& key);

                    /**
                     * Clear cache.
                     */
                    void Clear();

                    /**
                     * Clear entries from the cache and swap storage, without notifying listeners or CacheWriters.
                     * Entry is cleared only if it is not currently locked, and is not participating in a transaction.
                     *
                     * @param keys Keys to clear.
                     */
                    void ClearAll(const Writable& keys);

                    /**
                     * Stores given key-value pair in cache only if the previous value is equal to the old value passed
                     * as argument.
                     *
                     * @param key Key to store in cache.
                     * @param oldVal Old value to match.
                     * @param newVal Value to be associated with the given key.
                     * @return True if replace happened, false otherwise.
                     */
                    bool Replace(const WritableKey& key, const Writable& oldVal, const Writable& newVal);

                    /**
                     * Associates the specified value with the specified key in this cache, returning an existing value
                     * if one existed.
                     *
                     * @param key Key with which the specified value is to be associated.
                     * @param valIn Value to be associated with the specified key.
                     * @param valOut The value associated with the key at the start of the operation or null if none
                     *     was associated.
                     */
                    void GetAndPut(const WritableKey& key, const Writable& valIn, Readable& valOut);

                    /**
                     * Atomically removes the entry for a key only if currently mapped to some value.
                     *
                     * @param key Key with which the specified value is to be associated.
                     * @param valOut The value associated with the key at the start of the operation or null if none
                     *     was associated.
                     */
                    void GetAndRemove(const WritableKey& key, Readable& valOut);

                    /**
                     * Atomically replaces the value for a given key if and only if there is a value currently mapped by
                     * the key.
                     *
                     * @param key Key with which the specified value is to be associated.
                     * @param valIn Value to be associated with the specified key.
                     * @param valOut The value associated with the key at the start of the operation or null if none was
                     *     associated.
                     */
                    void GetAndReplace(const WritableKey& key, const Writable& valIn, Readable& valOut);

                    /**
                     * Atomically associates the specified key with the given value if it is not already associated with
                     * a value.
                     *
                     * @param key Key with which the specified value is to be associated.
                     * @param val Value to be associated with the specified key.
                     * @return True if a value was set.
                     */
                    bool PutIfAbsent(const WritableKey& key, const Writable& val);

                    /**
                     * Stores given key-value pair in cache only if cache had no previous mapping for it.
                     *
                     * If cache previously contained value for the given key, then this value is returned.
                     *
                     * In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node,
                     * which in  its turn may load the value from the swap storage, and consecutively, if it's not in
                     * swap, from the underlying persistent storage.
                     *
                     *  If the returned value is not needed, method putxIfAbsent() should be used instead of this one to
                     * avoid the overhead associated with returning of the previous value.
                     *
                     * If write-through is enabled, the stored value will be persisted to store.
                     *
                     * @param key Key to store in cache.
                     * @param valIn Value to be associated with the given key.
                     * @param valOut Previously contained value regardless of whether put happened or not (null if there
                     *     was no previous value).
                     */
                    void GetAndPutIfAbsent(const WritableKey& key, const Writable& valIn, Readable& valOut);

                    /**
                     * Perform SQL fields query.
                     *
                     * @param qry Query.
                     * @return Query cursor.
                     */
                    ignite::thin::cache::query::QueryFieldsCursor Query(
                            const ignite::thin::cache::query::SqlFieldsQuery& qry);

                    /**
                     * Get from CacheClient.
                     * Use for testing purposes only.
                     */
                    template<typename CacheClientT>
                    static CacheClientProxy& GetFromCacheClient(CacheClientT& instance)
                    {
                        return instance.proxy;
                    }

                private:
                    /** Implementation. */
                    common::concurrent::SharedPointer<void> impl;
                };
            }
        }
    }
}
#endif // _IGNITE_IMPL_THIN_CACHE_CACHE_CLIENT_PROXY
