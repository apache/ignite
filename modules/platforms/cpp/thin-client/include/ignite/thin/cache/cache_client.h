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
 * Declares ignite::thin::cache::CacheClient.
 */

#ifndef _IGNITE_THIN_CACHE_CACHE_CLIENT
#define _IGNITE_THIN_CACHE_CACHE_CLIENT

#include <ignite/common/concurrent.h>

#include <ignite/thin/cache/query/query_fields_cursor.h>
#include <ignite/thin/cache/query/query_sql_fields.h>

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/writable_key.h>

#include <ignite/impl/thin/readable.h>
#include <ignite/impl/thin/cache/cache_client_proxy.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            /**
             * Cache client class template.
             *
             * Main entry point for all Data Grid APIs.
             *
             * Both key and value types should be default-constructable, copy-constructable and assignable. Also
             * BinaryType class  template should be specialized for both types, if they are not one of the basic types.
             *
             * This class is implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object will be released automatically
             * once all the instances are destructed.
             *
             * @tparam K Cache key type.
             * @tparam V Cache value type.
             */
            template<typename K, typename V>
            class CacheClient
            {
                friend class impl::thin::cache::CacheClientProxy;

            public:
                /** Key type. */
                typedef K KeyType;

                /** Value type. */
                typedef V ValueType;

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                CacheClient(common::concurrent::SharedPointer<void> impl) :
                    proxy(impl)
                {
                    // No-op.
                }

                /**
                 * Default constructor.
                 */
                CacheClient()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~CacheClient()
                {
                    // No-op.
                }

                /**
                 * Associate the specified value with the specified key in the cache.
                 *
                 * @param key Key.
                 * @param value Value.
                 */
                void Put(const KeyType& key, const ValueType& value)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValue(value);

                    proxy.Put(wrKey, wrValue);
                }

                /**
                 * Stores given key-value pairs in cache.
                 * If write-through is enabled, the stored values will be persisted to store.
                 *
                 * @param begin Iterator pointing to the beginning of the key-value pair sequence.
                 * @param end Iterator pointing to the end of the key-value pair sequence.
                 */
                template<typename InIter>
                void PutAll(InIter begin, InIter end)
                {
                    impl::thin::WritableMapImpl<K, V, InIter> wrSeq(begin, end);

                    proxy.PutAll(wrSeq);
                }

                /**
                 * Stores given key-value pairs in cache.
                 * If write-through is enabled, the stored values will be persisted to store.
                 *
                 * @param vals Key-value pairs to store in cache.
                 */
                template<typename Map>
                void PutAll(const Map& vals)
                {
                    PutAll(vals.begin(), vals.end());
                }

                /**
                 * Get value from the cache.
                 *
                 * @param key Key.
                 * @param value Value.
                 */
                void Get(const KeyType& key, ValueType& value)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::ReadableImpl<ValueType> rdValue(value);

                    proxy.Get(wrKey, rdValue);
                }

                /**
                 * Get value from cache.
                 *
                 * @param key Key.
                 * @return Value.
                 */
                ValueType Get(const KeyType& key)
                {
                    ValueType value;

                    Get(key, value);

                    return value;
                }

                /**
                 * Retrieves values mapped to the specified keys from cache.
                 * If some value is not present in cache, then it will be looked up from swap storage. If it's not
                 * present in swap, or if swap is disabled, and if read-through is allowed, value will be loaded from
                 * persistent store.
                 *
                 * @param begin Iterator pointing to the beginning of the key sequence.
                 * @param end Iterator pointing to the end of the key sequence.
                 * @param dst Output iterator. Should dereference to std::pair or CacheEntry.
                 */
                template<typename InIter, typename OutIter>
                void GetAll(InIter begin, InIter end, OutIter dst)
                {
                    impl::thin::WritableSetImpl<K, InIter> wrSeq(begin, end);
                    impl::thin::ReadableMapImpl<K, V, OutIter> rdSeq(dst);

                    proxy.GetAll(wrSeq, rdSeq);
                }

                /**
                 * Retrieves values mapped to the specified keys from cache.
                 * If some value is not present in cache, then it will be looked up from swap storage. If it's not
                 * present in swap, or if swap is disabled, and if read-through is allowed, value will be loaded from
                 * persistent store.
                 *
                 * @param keys Keys.
                 * @param res Map of key-value pairs.
                 */
                template<typename Set, typename Map>
                void GetAll(const Set& keys, Map& res)
                {
                    return GetAll(keys.begin(), keys.end(), std::inserter(res, res.end()));
                }

                /**
                 * Stores given key-value pair in cache only if there is a previous mapping for it.
                 * If cache previously contained value for the given key, then this value is returned.
                 * In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node, which in
                 * its turn may load the value from the swap storage, and consecutively, if it's not in swap, rom the
                 * underlying persistent storage.
                 * If write-through is enabled, the stored value will be persisted to store.
                 *
                 * @param key Key to store in cache.
                 * @param value Value to be associated with the given key.
                 * @return True if the value was replaced.
                 */
                bool Replace(const K& key, const V& value)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValue(value);

                    return proxy.Replace(wrKey, wrValue);
                }

                /**
                 * Stores given key-value pair in cache only if the previous value is equal to the old value passed
                 * as argument.
                 *
                 * @param key Key to store in cache.
                 * @param oldVal Old value to match.
                 * @param newVal Value to be associated with the given key.
                 * @return True if replace happened, false otherwise.
                 */
                bool Replace(const KeyType& key, const ValueType& oldVal, const ValueType& newVal)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrOldVal(oldVal);
                    impl::thin::WritableImpl<ValueType> wrNewVal(newVal);

                    return proxy.Replace(wrKey, wrOldVal, wrNewVal);
                }

                /**
                 * Check if the cache contains a value for the specified key.
                 *
                 * @param key Key whose presence in this cache is to be tested.
                 * @return @c true if the cache contains specified key.
                 */
                bool ContainsKey(const KeyType& key)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);

                    return proxy.ContainsKey(wrKey);
                }

                /**
                 * Check if cache contains mapping for these keys.
                 *
                 * @param keys Keys.
                 * @return True if cache contains mapping for all these keys.
                 */
                template<typename Set>
                bool ContainsKeys(const Set& keys)
                {
                    return ContainsKeys(keys.begin(), keys.end());
                }

                /**
                 * Check if cache contains mapping for these keys.
                 *
                 * @param begin Iterator pointing to the beginning of the key sequence.
                 * @param end Iterator pointing to the end of the key sequence.
                 * @return True if cache contains mapping for all these keys.
                 */
                template<typename InIter>
                bool ContainsKeys(InIter begin, InIter end)
                {
                    impl::thin::WritableSetImpl<K, InIter> wrSeq(begin, end);

                    return proxy.ContainsKeys(wrSeq);
                }

                /**
                 * Gets the number of all entries cached across all nodes.
                 * @note This operation is distributed and will query all participating nodes for their cache sizes.
                 *
                 * @see CachePeekMode for details.
                 *
                 * @param peekModes Peek modes mask.
                 * @return Cache size across all nodes.
                 */
                int64_t GetSize(int32_t peekModes)
                {
                    return proxy.GetSize(peekModes);
                }

                /**
                 * Removes given key mapping from cache. If cache previously contained value for the given key, then
                 * this value is returned. In case of PARTITIONED or REPLICATED caches, the value will be loaded from
                 * the primary node, which in its turn may load the value from the disk-based swap storage, and
                 * consecutively, if it's not in swap, from the underlying persistent storage.
                 * If the returned value is not needed, method removex() should always be used instead of this one
                 * to avoid the overhead associated with returning of the previous value.
                 * If write-through is enabled, the value will be removed from store.
                 *
                 * @param key Key whose mapping is to be removed from cache.
                 * @return False if there was no matching key.
                 */
                bool Remove(const KeyType& key)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);

                    return proxy.Remove(wrKey);
                }

                /**
                 * Removes given key mapping from cache if one exists and value is equal to the passed in value.
                 * If write-through is enabled, the value will be removed from store.
                 *
                 * @param key Key whose mapping is to be removed from cache.
                 * @param val Value to match against currently cached value.
                 * @return True if entry was removed, false otherwise.
                 */
                bool Remove(const KeyType& key, const ValueType& val)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrVal(val);

                    return proxy.Remove(wrKey, wrVal);
                }

                /**
                 * Removes given key mappings from cache.
                 * If write-through is enabled, the value will be removed from store.
                 *
                 * @param keys Keys whose mappings are to be removed from cache.
                 */
                template<typename Set>
                void RemoveAll(const Set& keys)
                {
                    RemoveAll(keys.begin(), keys.end());
                }

                /**
                 * Removes given key mappings from cache.
                 * If write-through is enabled, the value will be removed from store.
                 *
                 * @param begin Iterator pointing to the beginning of the key sequence.
                 * @param end Iterator pointing to the end of the key sequence.
                 */
                template<typename InIter>
                void RemoveAll(InIter begin, InIter end)
                {
                    impl::thin::WritableSetImpl<K, InIter> wrSeq(begin, end);

                    proxy.RemoveAll(wrSeq);
                }

                /**
                 * Removes all mappings from cache.
                 * If write-through is enabled, the value will be removed from store.
                 * This method is transactional and will enlist the entry into ongoing transaction if there is one.
                 */
                void RemoveAll()
                {
                    proxy.RemoveAll();
                }

                /**
                 * Clear entry from the cache and swap storage, without notifying listeners or CacheWriters.
                 * Entry is cleared only if it is not currently locked, and is not participating in a transaction.
                 *
                 * @param key Key to clear.
                 */
                void Clear(const KeyType& key)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);

                    proxy.Clear(wrKey);
                }

                /**
                 * Clear cache.
                 */
                void Clear()
                {
                    proxy.Clear();
                }

                /**
                 * Clear entries from the cache and swap storage, without notifying listeners or CacheWriters.
                 * Entry is cleared only if it is not currently locked, and is not participating in a transaction.
                 *
                 * @param keys Keys to clear.
                 */
                template<typename Set>
                void ClearAll(const Set& keys)
                {
                    ClearAll(keys.begin(), keys.end());
                }

                /**
                 * Clear entries from the cache and swap storage, without notifying listeners or CacheWriters.
                 * Entry is cleared only if it is not currently locked, and is not participating in a transaction.
                 *
                 * @param begin Iterator pointing to the beginning of the key sequence.
                 * @param end Iterator pointing to the end of the key sequence.
                 */
                template<typename InIter>
                void ClearAll(InIter begin, InIter end)
                {
                    impl::thin::WritableSetImpl<K, InIter> wrSeq(begin, end);

                    proxy.ClearAll(wrSeq);
                }

                /**
                 * Associates the specified value with the specified key in this cache, returning an existing value if
                 * one existed.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param valIn Value to be associated with the specified key.
                 * @param valOut The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                void GetAndPut(const KeyType& key, const ValueType& valIn, ValueType& valOut)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValIn(valIn);
                    impl::thin::ReadableImpl<ValueType> rdValOut(valOut);

                    proxy.GetAndPut(wrKey, wrValIn, rdValOut);
                }

                /**
                 * Associates the specified value with the specified key in this cache, returning an existing value if
                 * one existed.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param valIn Value to be associated with the specified key.
                 * @return The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                ValueType GetAndPut(const KeyType& key, const ValueType& valIn)
                {
                    ValueType valOut;

                    GetAndPut(key, valIn, valOut);

                    return valOut;
                }

                /**
                 * Atomically removes the entry for a key only if currently mapped to some value.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param valOut The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                void GetAndRemove(const KeyType& key, ValueType& valOut)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::ReadableImpl<ValueType> rdValOut(valOut);

                    proxy.GetAndRemove(wrKey, rdValOut);
                }

                /**
                 * Atomically removes the entry for a key only if currently mapped to some value.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @return The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                ValueType GetAndRemove(const KeyType& key)
                {
                    ValueType valOut;

                    GetAndRemove(key, valOut);

                    return valOut;
                }

                /**
                 * Atomically replaces the value for a given key if and only if there is a value currently mapped by
                 * the key.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param valIn Value to be associated with the specified key.
                 * @param valOut The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                void GetAndReplace(const KeyType& key, const ValueType& valIn, ValueType& valOut)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValIn(valIn);
                    impl::thin::ReadableImpl<ValueType> rdValOut(valOut);

                    proxy.GetAndReplace(wrKey, wrValIn, rdValOut);
                }

                /**
                 * Atomically replaces the value for a given key if and only if there is a value currently mapped by
                 * the key.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param valIn Value to be associated with the specified key.
                 * @return The value associated with the key at the start of the operation or null if none was
                 *     associated.
                 */
                ValueType GetAndReplace(const KeyType& key, const ValueType& valIn)
                {
                    ValueType valOut;

                    GetAndReplace(key, valIn, valOut);

                    return valOut;
                }

                /**
                 * Atomically associates the specified key with the given value if it is not already associated with
                 * a value.
                 *
                 * @param key Key with which the specified value is to be associated.
                 * @param val Value to be associated with the specified key.
                 * @return True if a value was set.
                 */
                bool PutIfAbsent(const KeyType& key, const ValueType& val)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValIn(val);

                    return proxy.PutIfAbsent(wrKey, wrValIn);
                }

                /**
                 * Stores given key-value pair in cache only if cache had no previous mapping for it.
                 *
                 * If cache previously contained value for the given key, then this value is returned.
                 *
                 * In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node, which in
                 * its turn may load the value from the swap storage, and consecutively, if it's not in swap, from
                 * the underlying persistent storage.
                 *
                 * If the returned value is not needed, method putxIfAbsent() should be used instead of this one to
                 * avoid the overhead associated with returning of the previous value.
                 *
                 * If write-through is enabled, the stored value will be persisted to store.
                 *
                 * @param key Key to store in cache.
                 * @param valIn Value to be associated with the given key.
                 * @param valOut Previously contained value regardless of whether put happened or not (null if there was
                 *     no previous value).
                 */
                void GetAndPutIfAbsent(const KeyType& key, const ValueType& valIn, ValueType& valOut)
                {
                    impl::thin::WritableKeyImpl<KeyType> wrKey(key);
                    impl::thin::WritableImpl<ValueType> wrValIn(valIn);
                    impl::thin::ReadableImpl<ValueType> rdValOut(valOut);

                    proxy.GetAndPutIfAbsent(wrKey, wrValIn, rdValOut);
                }

                /**
                 * Stores given key-value pair in cache only if cache had no previous mapping for it.
                 *
                 * If cache previously contained value for the given key, then this value is returned.
                 *
                 * In case of PARTITIONED or REPLICATED caches, the value will be loaded from the primary node, which in
                 * its turn may load the value from the swap storage, and consecutively, if it's not in swap, from the
                 * underlying persistent storage.
                 *
                 * If the returned value is not needed, method putxIfAbsent() should be used instead of this one to
                 * avoid the overhead associated with returning of the previous value.
                 *
                 * If write-through is enabled, the stored value will be persisted to store.
                 *
                 * @param key Key to store in cache.
                 * @param valIn Value to be associated with the given key.
                 * @return Previously contained value regardless of whether put happened or not (null if there was no
                 *     previous value).
                 */
                ValueType GetAndPutIfAbsent(const KeyType& key, const ValueType& valIn)
                {
                    ValueType valOut;

                    GetAndPutIfAbsent(key, valIn, valOut);

                    return valOut;
                }

                /**
                 * Perform SQL fields query.
                 *
                 * @param qry Query.
                 * @return Query fields cursor.
                 */
                query::QueryFieldsCursor Query(const query::SqlFieldsQuery& qry)
                {
                    return proxy.Query(qry);
                }

                /**
                 * Refresh affinity mapping.
                 *
                 * @deprecated Does nothing since Apache Ignite 2.8. Affinity mapping is refreshed automatically now.
                 *
                 * Retrieves affinity mapping information from remote server. This information uses to send data
                 * requests to the most appropriate nodes. This can lessen latency and improve overall performance.
                 *
                 * It is recommended to refresh affinity mapping after every topology change, i.e. when a node enters or
                 * leaves cluster.
                 */
                void RefreshAffinityMapping()
                {
                    // No-op.
                }

            private:
                /** Implementation. */
                impl::thin::cache::CacheClientProxy proxy;
            };
        }
    }
}

#endif // _IGNITE_THIN_CACHE_CACHE_CLIENT
