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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.AspNet.Tests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Apache.Ignite.AspNet.Impl;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ExpiryCacheHolder{TK,TV}"/>.
    /// </summary>
    public class ExpiryCacheHolderTest
    {
        /// <summary>
        /// Tests the expiry policy.
        /// </summary>
        [Test]
        public void TestExpiryPolicy()
        {
            var cache = new CacheEx();

            Assert.IsNull(cache.ExpiryPolicy);

            var holder = new ExpiryCacheHolder<int, int>(cache);

            // Check same cache for same expiry.
            var cache1 = (CacheEx) holder.GetCacheWithExpiry(15);

            CheckExpiry(TimeSpan.FromSeconds(15), cache1);
            Assert.AreNotSame(cache, cache1);
            Assert.AreSame(cache1, holder.GetCacheWithExpiry(15));

            // Check rounding.
            var cache2 = (CacheEx) holder.GetCacheWithExpiry(DateTime.UtcNow.AddSeconds(15.1));
            Assert.AreSame(cache1, cache2);

            // Check no expiration.
            var cache3 = (CacheEx) holder.GetCacheWithExpiry(DateTime.MaxValue);
            Assert.AreSame(cache, cache3);
        }

        /// <summary>
        /// Checks the expiry.
        /// </summary>
        private static void CheckExpiry(TimeSpan timeSpan, CacheEx cache)
        {
            Assert.AreEqual(timeSpan, cache.ExpiryPolicy.GetExpiryForCreate());
            Assert.IsNull(cache.ExpiryPolicy.GetExpiryForUpdate());
            Assert.IsNull(cache.ExpiryPolicy.GetExpiryForAccess());
        }

        /// <summary>
        /// Test cache implementation.
        /// </summary>
        private class CacheEx : ICache<int, int>
        {
            public IExpiryPolicy ExpiryPolicy { get; set; }

            public IEnumerator<ICacheEntry<int, int>> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public string Name { get; private set; }

            public IIgnite Ignite { get; private set; }

            public CacheConfiguration GetConfiguration()
            {
                throw new NotImplementedException();
            }

            public bool IsEmpty()
            {
                throw new NotImplementedException();
            }

            public bool IsKeepBinary { get; private set; }
            
            public bool IsAllowAtomicOpsInTx { get; private set; }

            public ICache<int, int> WithSkipStore()
            {
                throw new NotImplementedException();
            }

            public ICache<int, int> WithExpiryPolicy(IExpiryPolicy plc)
            {
                return new CacheEx {ExpiryPolicy = plc};
            }

            public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
            {
                throw new NotImplementedException();
            }

            public ICache<int, int> WithAllowAtomicOpsInTx()
            {
                throw new NotImplementedException();
            }

            public void LoadCache(ICacheEntryFilter<int, int> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public Task LoadCacheAsync(ICacheEntryFilter<int, int> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public void LocalLoadCache(ICacheEntryFilter<int, int> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public Task LocalLoadCacheAsync(ICacheEntryFilter<int, int> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public void LoadAll(IEnumerable<int> keys, bool replaceExistingValues)
            {
                throw new NotImplementedException();
            }

            public Task LoadAllAsync(IEnumerable<int> keys, bool replaceExistingValues)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKey(int key)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ContainsKeyAsync(int key)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKeys(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ContainsKeysAsync(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public int LocalPeek(int key, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public bool TryLocalPeek(int key, out int value, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public int this[int key]
            {
                get { throw new NotImplementedException(); }
                set { throw new NotImplementedException(); }
            }

            public int Get(int key)
            {
                throw new NotImplementedException();
            }

            public Task<int> GetAsync(int key)
            {
                throw new NotImplementedException();
            }

            public bool TryGet(int key, out int value)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<int>> TryGetAsync(int key)
            {
                throw new NotImplementedException();
            }

            public ICollection<ICacheEntry<int, int>> GetAll(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public Task<ICollection<ICacheEntry<int, int>>> GetAllAsync(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public void Put(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<int> GetAndPut(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<int>> GetAndPutAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<int> GetAndReplace(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<int>> GetAndReplaceAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<int> GetAndRemove(int key)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<int>> GetAndRemoveAsync(int key)
            {
                throw new NotImplementedException();
            }

            public bool PutIfAbsent(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> PutIfAbsentAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<int> GetAndPutIfAbsent(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<int>> GetAndPutIfAbsentAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public bool Replace(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ReplaceAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public bool Replace(int key, int oldVal, int newVal)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ReplaceAsync(int key, int oldVal, int newVal)
            {
                throw new NotImplementedException();
            }

            public void PutAll(IEnumerable<KeyValuePair<int, int>> vals)
            {
                throw new NotImplementedException();
            }

            public Task PutAllAsync(IEnumerable<KeyValuePair<int, int>> vals)
            {
                throw new NotImplementedException();
            }

            public void LocalEvict(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public Task ClearAsync()
            {
                throw new NotImplementedException();
            }

            public void Clear(int key)
            {
                throw new NotImplementedException();
            }

            public Task ClearAsync(int key)
            {
                throw new NotImplementedException();
            }

            public void ClearAll(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public Task ClearAllAsync(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public void LocalClear(int key)
            {
                throw new NotImplementedException();
            }

            public void LocalClearAll(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public bool Remove(int key)
            {
                throw new NotImplementedException();
            }

            public Task<bool> RemoveAsync(int key)
            {
                throw new NotImplementedException();
            }

            public bool Remove(int key, int val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> RemoveAsync(int key, int val)
            {
                throw new NotImplementedException();
            }

            public void RemoveAll(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public Task RemoveAllAsync(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public void RemoveAll()
            {
                throw new NotImplementedException();
            }

            public Task RemoveAllAsync()
            {
                throw new NotImplementedException();
            }

            public int GetLocalSize(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public int GetSize(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public Task<int> GetSizeAsync(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public long GetSizeLong(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public long GetSizeLong(int partition, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public Task<long> GetSizeLongAsync(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public Task<long> GetSizeLongAsync(int partition, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public long GetLocalSizeLong(params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public long GetLocalSizeLong(int partition, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public IQueryCursor<ICacheEntry<int, int>> Query(QueryBase qry)
            {
                throw new NotImplementedException();
            }

            public IFieldsQueryCursor Query(SqlFieldsQuery qry)
            {
                throw new NotImplementedException();
            }

            public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
            {
                throw new NotImplementedException();
            }

            public IContinuousQueryHandle QueryContinuous(ContinuousQuery<int, int> qry)
            {
                throw new NotImplementedException();
            }

            public IContinuousQueryHandle<ICacheEntry<int, int>> QueryContinuous(ContinuousQuery<int, int> qry, QueryBase initialQry)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<ICacheEntry<int, int>> GetLocalEntries(params CachePeekMode[] peekModes)
            {
                throw new NotImplementedException();
            }

            public TRes Invoke<TArg, TRes>(int key, ICacheEntryProcessor<int, int, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public Task<TRes> InvokeAsync<TArg, TRes>(int key, ICacheEntryProcessor<int, int, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public ICollection<ICacheEntryProcessorResult<int, TRes>> InvokeAll<TArg, TRes>(IEnumerable<int> keys, ICacheEntryProcessor<int, int, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public Task<ICollection<ICacheEntryProcessorResult<int, TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<int> keys, ICacheEntryProcessor<int, int, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public ICacheLock Lock(int key)
            {
                throw new NotImplementedException();
            }

            public ICacheLock LockAll(IEnumerable<int> keys)
            {
                throw new NotImplementedException();
            }

            public bool IsLocalLocked(int key, bool byCurrentThread)
            {
                throw new NotImplementedException();
            }

            public ICacheMetrics GetMetrics()
            {
                throw new NotImplementedException();
            }

            public ICacheMetrics GetMetrics(IClusterGroup clusterGroup)
            {
                throw new NotImplementedException();
            }

            public ICacheMetrics GetLocalMetrics()
            {
                throw new NotImplementedException();
            }

            public void EnableStatistics(bool enabled)
            {
                throw new NotImplementedException();
            }

            public Task Rebalance()
            {
                throw new NotImplementedException();
            }

            public ICache<int, int> WithNoRetries()
            {
                throw new NotImplementedException();
            }

            public ICache<int, int> WithPartitionRecover()
            {
                throw new NotImplementedException();
            }

            public ICollection<int> GetLostPartitions()
            {
                throw new NotImplementedException();
            }

            public IQueryMetrics GetQueryMetrics()
            {
                throw new NotImplementedException();
            }

            public void ResetQueryMetrics()
            {
                throw new NotImplementedException();
            }

            public void PreloadPartition(int partition)
            {
                throw new NotImplementedException();
            }

            public Task PreloadPartitionAsync(int partition)
            {
                throw new NotImplementedException();
            }

            public bool LocalPreloadPartition(int partition)
            {
                throw new NotImplementedException();
            }
        }
    }
}
