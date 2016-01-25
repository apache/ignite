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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests SQL generation without starting the grid.
    /// </summary>
    public class CacheLinqProviderTest
    {
        [Test]
        public void TestWhere()
        {
            var cache = new CacheStub<int, QueryPerson>();

            var res = cache.ToQueryable().Where(x => x.Value.Age > 20 && x.Value.Name.Contains("john")).ToList();

            Assert.IsNotNull(res);

            Assert.AreEqual("((Age > ?) and (Name like '%' + ? + '%'))", cache.LastQuery);
            Assert.AreEqual(new object[] {20, "john"}, cache.LastQueryArgs);
        }

        private class CacheStub<TKey, TValue> : ICache<TKey, TValue>
        {
            public string LastQuery { get; private set; }
            public object[] LastQueryArgs { get; private set; }

            public IEnumerator<ICacheEntry<TKey, TValue>> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public string Name
            {
                get { throw new NotImplementedException(); }
            }

            public IIgnite Ignite
            {
                get { throw new NotImplementedException(); }
            }

            public bool IsEmpty()
            {
                throw new NotImplementedException();
            }

            public bool IsKeepBinary
            {
                get { throw new NotImplementedException(); }
            }

            public ICache<TKey, TValue> WithSkipStore()
            {
                throw new NotImplementedException();
            }

            public ICache<TKey, TValue> WithExpiryPolicy(IExpiryPolicy plc)
            {
                throw new NotImplementedException();
            }

            public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
            {
                throw new NotImplementedException();
            }

            public void LoadCache(ICacheEntryFilter<TKey, TValue> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public Task LoadCacheAsync(ICacheEntryFilter<TKey, TValue> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public void LocalLoadCache(ICacheEntryFilter<TKey, TValue> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public Task LocalLoadCacheAsync(ICacheEntryFilter<TKey, TValue> p, params object[] args)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKey(TKey key)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ContainsKeyAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public bool ContainsKeys(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ContainsKeysAsync(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public TValue LocalPeek(TKey key, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public bool TryLocalPeek(TKey key, out TValue value, params CachePeekMode[] modes)
            {
                throw new NotImplementedException();
            }

            public TValue this[TKey key]
            {
                get { throw new NotImplementedException(); }
                set { throw new NotImplementedException(); }
            }

            public TValue Get(TKey key)
            {
                throw new NotImplementedException();
            }

            public Task<TValue> GetAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public bool TryGet(TKey key, out TValue value)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<TValue>> TryGetAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public IDictionary<TKey, TValue> GetAll(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public Task<IDictionary<TKey, TValue>> GetAllAsync(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public void Put(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task PutAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<TValue> GetAndPut(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<TValue>> GetAndPutAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<TValue> GetAndReplace(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<TValue>> GetAndReplaceAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<TValue> GetAndRemove(TKey key)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<TValue>> GetAndRemoveAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public bool PutIfAbsent(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> PutIfAbsentAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public CacheResult<TValue> GetAndPutIfAbsent(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<CacheResult<TValue>> GetAndPutIfAbsentAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public bool Replace(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ReplaceAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public bool Replace(TKey key, TValue oldVal, TValue newVal)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ReplaceAsync(TKey key, TValue oldVal, TValue newVal)
            {
                throw new NotImplementedException();
            }

            public void PutAll(IDictionary<TKey, TValue> vals)
            {
                throw new NotImplementedException();
            }

            public Task PutAllAsync(IDictionary<TKey, TValue> vals)
            {
                throw new NotImplementedException();
            }

            public void LocalEvict(IEnumerable<TKey> keys)
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

            public void Clear(TKey key)
            {
                throw new NotImplementedException();
            }

            public Task ClearAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public void ClearAll(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public Task ClearAllAsync(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public void LocalClear(TKey key)
            {
                throw new NotImplementedException();
            }

            public void LocalClearAll(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public bool Remove(TKey key)
            {
                throw new NotImplementedException();
            }

            public Task<bool> RemoveAsync(TKey key)
            {
                throw new NotImplementedException();
            }

            public bool Remove(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public Task<bool> RemoveAsync(TKey key, TValue val)
            {
                throw new NotImplementedException();
            }

            public void RemoveAll(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public Task RemoveAllAsync(IEnumerable<TKey> keys)
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

            public void LocalPromote(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public IQueryCursor<ICacheEntry<TKey, TValue>> Query(QueryBase qry)
            {
                var sqlQuery = (SqlQuery) qry;

                LastQuery = sqlQuery.Sql;
                LastQueryArgs = sqlQuery.Arguments;

                return new CursorStub<ICacheEntry<TKey, TValue>>();
            }

            public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
            {
                LastQuery = qry.Sql;
                LastQueryArgs = qry.Arguments;

                return new CursorStub<IList>();
            }

            public IContinuousQueryHandle QueryContinuous(ContinuousQuery<TKey, TValue> qry)
            {
                throw new NotImplementedException();
            }

            public IContinuousQueryHandle<ICacheEntry<TKey, TValue>> QueryContinuous(ContinuousQuery<TKey, TValue> qry, QueryBase initialQry)
            {
                throw new NotImplementedException();
            }

            public IEnumerable<ICacheEntry<TKey, TValue>> GetLocalEntries(params CachePeekMode[] peekModes)
            {
                throw new NotImplementedException();
            }

            public TRes Invoke<TArg, TRes>(TKey key, ICacheEntryProcessor<TKey, TValue, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public Task<TRes> InvokeAsync<TArg, TRes>(TKey key, ICacheEntryProcessor<TKey, TValue, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public IDictionary<TKey, ICacheEntryProcessorResult<TRes>> InvokeAll<TArg, TRes>(IEnumerable<TKey> keys, ICacheEntryProcessor<TKey, TValue, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public Task<IDictionary<TKey, ICacheEntryProcessorResult<TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TKey> keys, ICacheEntryProcessor<TKey, TValue, TArg, TRes> processor, TArg arg)
            {
                throw new NotImplementedException();
            }

            public ICacheLock Lock(TKey key)
            {
                throw new NotImplementedException();
            }

            public ICacheLock LockAll(IEnumerable<TKey> keys)
            {
                throw new NotImplementedException();
            }

            public bool IsLocalLocked(TKey key, bool byCurrentThread)
            {
                throw new NotImplementedException();
            }

            public ICacheMetrics GetMetrics()
            {
                throw new NotImplementedException();
            }

            public Task Rebalance()
            {
                throw new NotImplementedException();
            }

            public ICache<TKey, TValue> WithNoRetries()
            {
                throw new NotImplementedException();
            }
        }

        private class CursorStub<T> : List<T>, IQueryCursor<T>
        {
            public IList<T> GetAll()
            {
                return this;
            }

            public void Dispose()
            {
                // No-op.
            }
        }
    }
}