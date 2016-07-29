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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native cache wrapper.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheImpl<TK, TV> : PlatformTarget, ICache<TK, TV>, ICacheInternal
    {
        /** Duration: unchanged. */
        private const long DurUnchanged = -2;

        /** Duration: eternal. */
        private const long DurEternal = -1;

        /** Duration: zero. */
        private const long DurZero = 0;

        /** Ignite instance. */
        private readonly Ignite _ignite;
        
        /** Flag: skip store. */
        private readonly bool _flagSkipStore;

        /** Flag: keep binary. */
        private readonly bool _flagKeepBinary;

        /** Flag: async mode.*/
        private readonly bool _flagAsync;

        /** Flag: no-retries.*/
        private readonly bool _flagNoRetries;

        /** Async instance. */
        private readonly Lazy<CacheImpl<TK, TV>> _asyncInstance;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="flagSkipStore">Skip store flag.</param>
        /// <param name="flagKeepBinary">Keep binary flag.</param>
        /// <param name="flagAsync">Async mode flag.</param>
        /// <param name="flagNoRetries">No-retries mode flag.</param>
        public CacheImpl(Ignite grid, IUnmanagedTarget target, Marshaller marsh,
            bool flagSkipStore, bool flagKeepBinary, bool flagAsync, bool flagNoRetries) : base(target, marsh)
        {
            _ignite = grid;
            _flagSkipStore = flagSkipStore;
            _flagKeepBinary = flagKeepBinary;
            _flagAsync = flagAsync;
            _flagNoRetries = flagNoRetries;

            _asyncInstance = new Lazy<CacheImpl<TK, TV>>(() => new CacheImpl<TK, TV>(this));
        }

        /// <summary>
        /// Initializes a new async instance.
        /// </summary>
        /// <param name="cache">The cache.</param>
        private CacheImpl(CacheImpl<TK, TV> cache) : base(UU.CacheWithAsync(cache.Target), cache.Marshaller)
        {
            _ignite = cache._ignite;
            _flagSkipStore = cache._flagSkipStore;
            _flagKeepBinary = cache._flagKeepBinary;
            _flagAsync = true;
            _flagNoRetries = cache._flagNoRetries;
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite; }
        }

        /** <inheritDoc /> */
        private bool IsAsync
        {
            get { return _flagAsync; }
        }

        /// <summary>
        /// Gets and resets task for previous asynchronous operation.
        /// </summary>
        /// <param name="lastAsyncOp">The last async op id.</param>
        /// <returns>
        /// Task for previous asynchronous operation.
        /// </returns>
        private Task GetTask(CacheOp lastAsyncOp)
        {
            return GetTask<object>(lastAsyncOp);
        }

        /// <summary>
        /// Gets and resets task for previous asynchronous operation.
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="lastAsyncOp">The last async op id.</param>
        /// <param name="converter">The converter.</param>
        /// <returns>
        /// Task for previous asynchronous operation.
        /// </returns>
        private Task<TResult> GetTask<TResult>(CacheOp lastAsyncOp, Func<BinaryReader, TResult> converter = null)
        {
            Debug.Assert(_flagAsync);

            return GetFuture((futId, futTypeId) => UU.TargetListenFutureForOperation(Target, futId, futTypeId, 
                (int) lastAsyncOp), _flagKeepBinary, converter).Task;
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return DoInOp<string>((int)CacheOp.GetName); }
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetConfiguration()
        {
            return DoInOp((int) CacheOp.GetConfig, stream => new CacheConfiguration(Marshaller.StartUnmarshal(stream)));
        }

        /** <inheritDoc /> */
        public bool IsEmpty()
        {
            return GetSize() == 0;
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithSkipStore()
        {
            if (_flagSkipStore)
                return this;

            return new CacheImpl<TK, TV>(_ignite, UU.CacheWithSkipStore(Target), Marshaller, 
                true, _flagKeepBinary, _flagAsync, true);
        }

        /// <summary>
        /// Skip store flag getter.
        /// </summary>
        internal bool IsSkipStore { get { return _flagSkipStore; } }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepBinary<TK1, TV1>()
        {
            if (_flagKeepBinary)
            {
                var result = this as ICache<TK1, TV1>;

                if (result == null)
                    throw new InvalidOperationException(
                        "Can't change type of binary cache. WithKeepBinary has been called on an instance of " +
                        "binary cache with incompatible generic arguments.");

                return result;
            }

            return new CacheImpl<TK1, TV1>(_ignite, UU.CacheWithKeepBinary(Target), Marshaller, 
                _flagSkipStore, true, _flagAsync, _flagNoRetries);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            IgniteArgumentCheck.NotNull(plc, "plc");

            long create = ConvertDuration(plc.GetExpiryForCreate());
            long update = ConvertDuration(plc.GetExpiryForUpdate());
            long access = ConvertDuration(plc.GetExpiryForAccess());

            IUnmanagedTarget cache0 = UU.CacheWithExpiryPolicy(Target, create, update, access);

            return new CacheImpl<TK, TV>(_ignite, cache0, Marshaller, _flagSkipStore, _flagKeepBinary, _flagAsync, _flagNoRetries);
        }

        /// <summary>
        /// Convert TimeSpan to duration recognizable by Java.
        /// </summary>
        /// <param name="dur">.Net duration.</param>
        /// <returns>Java duration in milliseconds.</returns>
        private static long ConvertDuration(TimeSpan? dur)
        {
            if (dur.HasValue)
            {
                if (dur.Value == TimeSpan.MaxValue)
                    return DurEternal;

                long dur0 = (long)dur.Value.TotalMilliseconds;

                return dur0 > 0 ? dur0 : DurZero;
            }
            
            return DurUnchanged;
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { return _flagKeepBinary; }
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LoadCache);
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            AsyncInstance.LoadCache(p, args);

            return AsyncInstance.GetTask(CacheOp.LoadCache);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LocLoadCache);
        }

        /** <inheritDoc /> */
        public Task LocalLoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            AsyncInstance.LocalLoadCache(p, args);

            return AsyncInstance.GetTask(CacheOp.LocLoadCache);
        }

        /// <summary>
        /// Loads the cache.
        /// </summary>
        private void LoadCache0(ICacheEntryFilter<TK, TV> p, object[] args, int opId)
        {
            DoOutOp(opId, writer =>
            {
                if (p != null)
                {
                    var p0 = new CacheEntryFilterHolder(p, (k, v) => p.Invoke(new CacheEntry<TK, TV>((TK) k, (TV) v)),
                        Marshaller, IsKeepBinary);

                    writer.WriteObject(p0);
                }
                else
                    writer.WriteObject<CacheEntryFilterHolder>(null);

                writer.WriteArray(args);
            });
        }

        /** <inheritDoc /> */
        public void LoadAll(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            LoadAllAsync(keys, replaceExistingValues).Wait();
        }

        /** <inheritDoc /> */
        public Task LoadAllAsync(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            return GetFuture<object>((futId, futTyp) => DoOutOp((int) CacheOp.LoadAll, writer =>
            {
                writer.WriteLong(futId);
                writer.WriteBoolean(replaceExistingValues);
                WriteEnumerable(writer, keys);
            })).Task;
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp((int)CacheOp.ContainsKey, key) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            AsyncInstance.ContainsKey(key);

            return AsyncInstance.GetTask<bool>(CacheOp.ContainsKey);
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOp((int)CacheOp.ContainsKeys, writer => WriteEnumerable(writer, keys)) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            AsyncInstance.ContainsKeys(keys);

            return AsyncInstance.GetTask<bool>(CacheOp.ContainsKeys);
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            TV res;

            if (TryLocalPeek(key, out res))
                return res;

            throw GetKeyNotFoundException();
        }

        /** <inheritDoc /> */
        public bool TryLocalPeek(TK key, out TV value, params CachePeekMode[] modes)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            var res = DoOutInOpNullable<TV>((int)CacheOp.Peek, writer =>
            {
                writer.Write(key);
                writer.WriteInt(EncodePeekModes(modes));
            });

            value = res.Success ? res.Value : default(TV);

            return res.Success;
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get
            {
                if (IsAsync)
                    throw new InvalidOperationException("Indexer can't be used in async mode.");

                return Get(key);
            }
            set
            {
                if (IsAsync)
                    throw new InvalidOperationException("Indexer can't be used in async mode.");

                Put(key, value);
            }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            var result = DoOutInOpNullable<TK, TV>((int) CacheOp.Get, key);

            if (!IsAsync)
            {
                if (!result.Success)
                    throw GetKeyNotFoundException();

                return result.Value;
            }

            Debug.Assert(!result.Success);

            return default(TV);
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            AsyncInstance.Get(key);

            return AsyncInstance.GetTask(CacheOp.Get, reader =>
            {
                if (reader != null)
                    return reader.ReadObject<TV>();

                throw GetKeyNotFoundException();
            });
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            if (IsAsync)
                throw new InvalidOperationException("TryGet can't be used in async mode.");

            var res = DoOutInOpNullable<TK, TV>((int) CacheOp.Get, key);

            value = res.Value;

            return res.Success;
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            AsyncInstance.Get(key);

            return AsyncInstance.GetTask(CacheOp.Get, GetCacheResult);
        }

        /** <inheritDoc /> */
        public IDictionary<TK, TV> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp((int)CacheOp.GetAll,
                writer => WriteEnumerable(writer, keys),
                input =>
                {
                    var reader = Marshaller.StartUnmarshal(input, _flagKeepBinary);

                    return ReadGetAllDictionary(reader);
                });
        }

        /** <inheritDoc /> */
        public Task<IDictionary<TK, TV>> GetAllAsync(IEnumerable<TK> keys)
        {
            AsyncInstance.GetAll(keys);

            return AsyncInstance.GetTask(CacheOp.GetAll, r => r == null ? null : ReadGetAllDictionary(r));
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            DoOutOp((int)CacheOp.Put, key, val);
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            AsyncInstance.Put(key, val);

            return AsyncInstance.GetTask(CacheOp.Put);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int)CacheOp.GetAndPut, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            AsyncInstance.GetAndPut(key, val);

            return AsyncInstance.GetTask(CacheOp.GetAndPut, GetCacheResult);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int) CacheOp.GetAndReplace, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            AsyncInstance.GetAndReplace(key, val);

            return AsyncInstance.GetTask(CacheOp.GetAndReplace, GetCacheResult);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpNullable<TK, TV>((int)CacheOp.GetAndRemove, key);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            AsyncInstance.GetAndRemove(key);

            return AsyncInstance.GetTask(CacheOp.GetAndRemove, GetCacheResult);
        }

        /** <inheritdoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int) CacheOp.PutIfAbsent, key, val) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            AsyncInstance.PutIfAbsent(key, val);

            return AsyncInstance.GetTask<bool>(CacheOp.PutIfAbsent);
        }

        /** <inheritdoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int)CacheOp.GetAndPutIfAbsent, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            AsyncInstance.GetAndPutIfAbsent(key, val);

            return AsyncInstance.GetTask(CacheOp.GetAndPutIfAbsent, GetCacheResult);
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int) CacheOp.Replace2, key, val) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            AsyncInstance.Replace(key, val);

            return AsyncInstance.GetTask<bool>(CacheOp.Replace2);
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(oldVal, "oldVal");

            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutOp((int)CacheOp.Replace3, key, oldVal, newVal) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            AsyncInstance.Replace(key, oldVal, newVal);

            return AsyncInstance.GetTask<bool>(CacheOp.Replace3);
        }

        /** <inheritdoc /> */
        public void PutAll(IDictionary<TK, TV> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            DoOutOp((int) CacheOp.PutAll, writer => WriteDictionary(writer, vals));
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IDictionary<TK, TV> vals)
        {
            AsyncInstance.PutAll(vals);

            return AsyncInstance.GetTask(CacheOp.PutAll);
        }

        /** <inheritdoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int) CacheOp.LocEvict, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            UU.CacheClear(Target);
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            AsyncInstance.Clear();

            return AsyncInstance.GetTask();
        }

        /** <inheritdoc /> */
        public void Clear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp((int) CacheOp.Clear, key);
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            AsyncInstance.Clear(key);

            return AsyncInstance.GetTask(CacheOp.Clear);
        }

        /** <inheritdoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.ClearAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            AsyncInstance.ClearAll(keys);

            return AsyncInstance.GetTask(CacheOp.ClearAll);
        }

        /** <inheritdoc /> */
        public void LocalClear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp((int) CacheOp.LocalClear, key);
        }

        /** <inheritdoc /> */
        public void LocalClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.LocalClearAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public bool Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp((int) CacheOp.RemoveObj, key) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            AsyncInstance.Remove(key);

            return AsyncInstance.GetTask<bool>(CacheOp.RemoveObj);
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int)CacheOp.RemoveBool, key, val) == True;
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            AsyncInstance.Remove(key, val);

            return AsyncInstance.GetTask<bool>(CacheOp.RemoveBool);
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.RemoveAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            AsyncInstance.RemoveAll(keys);

            return AsyncInstance.GetTask(CacheOp.RemoveAll);
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            UU.CacheRemoveAll(Target);
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            AsyncInstance.RemoveAll();

            return AsyncInstance.GetTask();
        }

        /** <inheritDoc /> */
        public int GetLocalSize(params CachePeekMode[] modes)
        {
            return Size0(true, modes);
        }

        /** <inheritDoc /> */
        public int GetSize(params CachePeekMode[] modes)
        {
            return Size0(false, modes);
        }

        /** <inheritDoc /> */
        public Task<int> GetSizeAsync(params CachePeekMode[] modes)
        {
            AsyncInstance.GetSize(modes);

            return AsyncInstance.GetTask<int>();
        }

        /// <summary>
        /// Internal size routine.
        /// </summary>
        /// <param name="loc">Local flag.</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private int Size0(bool loc, params CachePeekMode[] modes)
        {
            int modes0 = EncodePeekModes(modes);

            return UU.CacheSize(Target, modes0, loc);
        }

        /** <inheritDoc /> */
        public void LocalPromote(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.LocPromote, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(processor, "processor");

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutInOp((int)CacheOp.Invoke, writer =>
            {
                writer.Write(key);
                writer.Write(holder);
            },
            input => GetResultOrThrow<TRes>(Unmarshal<object>(input)));
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            AsyncInstance.Invoke(key, processor, arg);

            return AsyncInstance.GetTask(CacheOp.Invoke, r =>
            {
                if (r == null)
                    return default(TRes);

                var hasError = r.ReadBoolean();

                if (hasError)
                    throw ReadException(r.Stream);

                return r.ReadObject<TRes>();
            });
        }

        /** <inheritdoc /> */
        public IDictionary<TK, ICacheEntryProcessorResult<TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            IgniteArgumentCheck.NotNull(processor, "processor");

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutInOp((int) CacheOp.InvokeAll,
                writer =>
                {
                    WriteEnumerable(writer, keys);
                    writer.Write(holder);
                },
                input => ReadInvokeAllResults<TRes>(input));
        }

        /** <inheritDoc /> */
        public Task<IDictionary<TK, ICacheEntryProcessorResult<TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            AsyncInstance.InvokeAll(keys, processor, arg);

            return AsyncInstance.GetTask(CacheOp.InvokeAll, reader => ReadInvokeAllResults<TRes>(reader.Stream));
        }

        /** <inheritdoc /> */
        public ICacheLock Lock(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp((int)CacheOp.Lock, writer =>
            {
                writer.Write(key);
            }, input => new CacheLock(input.ReadInt(), Target));
        }

        /** <inheritdoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp((int)CacheOp.LockAll, writer =>
            {
                WriteEnumerable(writer, keys);
            }, input => new CacheLock(input.ReadInt(), Target));
        }

        /** <inheritdoc /> */
        public bool IsLocalLocked(TK key, bool byCurrentThread)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp((int)CacheOp.IsLocalLocked, writer =>
            {
                writer.Write(key);
                writer.WriteBoolean(byCurrentThread);
            }) == True;
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            return DoInOp((int)CacheOp.Metrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public Task Rebalance()
        {
            return GetFuture<object>((futId, futTyp) => UU.CacheRebalance(Target, futId)).Task;
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            if (_flagNoRetries)
                return this;

            return new CacheImpl<TK, TV>(_ignite, UU.CacheWithNoRetries(Target), Marshaller,
                _flagSkipStore, _flagKeepBinary, _flagAsync, true);
        }

        /// <summary>
        /// Gets the asynchronous instance.
        /// </summary>
        private CacheImpl<TK, TV> AsyncInstance
        {
            get { return _asyncInstance.Value; }
        }

        #region Queries

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            return QueryFields(qry, ReadFieldsArrayList);
        }

        /// <summary>
        /// Reads the fields array list.
        /// </summary>
        private static IList ReadFieldsArrayList(IBinaryRawReader reader, int count)
        {
            IList res = new ArrayList(count);

            for (var i = 0; i < count; i++)
                res.Add(reader.ReadObject<object>());

            return res;
        }

        /** <inheritDoc /> */
        public IQueryCursor<T> QueryFields<T>(SqlFieldsQuery qry, Func<IBinaryRawReader, int, T> readerFunc)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");
            IgniteArgumentCheck.NotNull(readerFunc, "readerFunc");

            if (string.IsNullOrEmpty(qry.Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            IUnmanagedTarget cursor;

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = Marshaller.StartMarshal(stream);

                writer.WriteBoolean(qry.Local);
                writer.WriteString(qry.Sql);
                writer.WriteInt(qry.PageSize);

                WriteQueryArgs(writer, qry.Arguments);

                writer.WriteBoolean(qry.EnableDistributedJoins);
                writer.WriteBoolean(qry.EnforceJoinOrder);

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(Target, (int) CacheOp.QrySqlFields, stream.SynchronizeOutput());
            }
        
            return new FieldsQueryCursor<T>(cursor, Marshaller, _flagKeepBinary, readerFunc);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            IUnmanagedTarget cursor;

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = Marshaller.StartMarshal(stream);

                qry.Write(writer, IsKeepBinary);

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(Target, (int)qry.OpId, stream.SynchronizeOutput()); 
            }

            return new QueryCursor<TK, TV>(cursor, Marshaller, _flagKeepBinary);
        }
                
        /// <summary>
        /// Write query arguments.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="args">Arguments.</param>
        private static void WriteQueryArgs(BinaryWriter writer, object[] args)
        {
            if (args == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(args.Length);

                foreach (var arg in args)
                {
                    // Write DateTime as TimeStamp always, otherwise it does not make sense
                    // Wrapped DateTime comparison does not work in SQL
                    var dt = arg as DateTime?;  // Works with DateTime also

                    if (dt != null)
                        writer.WriteTimestamp(dt);
                    else
                        writer.WriteObject(arg);
                }
            }
        }

        /** <inheritdoc /> */
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<TK, TV> qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            return QueryContinuousImpl(qry, null);
        }

        /** <inheritdoc /> */
        public IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuous(ContinuousQuery<TK, TV> qry, QueryBase initialQry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");
            IgniteArgumentCheck.NotNull(initialQry, "initialQry");

            return QueryContinuousImpl(qry, initialQry);
        }

        /// <summary>
        /// QueryContinuous implementation.
        /// </summary>
        private IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuousImpl(ContinuousQuery<TK, TV> qry,
            QueryBase initialQry)
        {
            qry.Validate();

            var hnd = new ContinuousQueryHandleImpl<TK, TV>(qry, Marshaller, _flagKeepBinary);

            try
            {
                using (var stream = IgniteManager.Memory.Allocate().GetStream())
                {
                    var writer = Marshaller.StartMarshal(stream);

                    hnd.Start(_ignite, writer, () =>
                    {
                        if (initialQry != null)
                        {
                            writer.WriteInt((int) initialQry.OpId);

                            initialQry.Write(writer, IsKeepBinary);
                        }
                        else
                            writer.WriteInt(-1); // no initial query

                        FinishMarshal(writer);

                        // ReSharper disable once AccessToDisposedClosure
                        return UU.CacheOutOpContinuousQuery(Target, (int) CacheOp.QryContinuous,
                            stream.SynchronizeOutput());
                    }, qry);
                }

                return hnd;
            }
            catch (Exception)
            {
                hnd.Dispose();

                throw;
            }
        }

        #endregion

        #region Enumerable support

        /** <inheritdoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(CachePeekMode[] peekModes)
        {
            return new CacheEnumerable<TK, TV>(this, EncodePeekModes(peekModes));
        }

        /** <inheritdoc /> */
        public IEnumerator<ICacheEntry<TK, TV>> GetEnumerator()
        {
            return new CacheEnumeratorProxy<TK, TV>(this, false, 0);
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Create real cache enumerator.
        /// </summary>
        /// <param name="loc">Local flag.</param>
        /// <param name="peekModes">Peek modes for local enumerator.</param>
        /// <returns>Cache enumerator.</returns>
        internal CacheEnumerator<TK, TV> CreateEnumerator(bool loc, int peekModes)
        {
            if (loc)
                return new CacheEnumerator<TK, TV>(UU.CacheLocalIterator(Target, peekModes), Marshaller, _flagKeepBinary);

            return new CacheEnumerator<TK, TV>(UU.CacheIterator(Target), Marshaller, _flagKeepBinary);
        }

        #endregion

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IBinaryStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, _flagKeepBinary);
        }

        /// <summary>
        /// Encodes the peek modes into a single int value.
        /// </summary>
        private static int EncodePeekModes(CachePeekMode[] modes)
        {
            int modesEncoded = 0;

            if (modes != null)
            {
                foreach (var mode in modes)
                    modesEncoded |= (int) mode;
            }

            return modesEncoded;
        }

        /// <summary>
        /// Unwraps an exception.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="obj">Object.</param>
        /// <returns>Result.</returns>
        private static T GetResultOrThrow<T>(object obj)
        {
            var err = obj as Exception;

            if (err != null)
                throw err as CacheEntryProcessorException ?? new CacheEntryProcessorException(err);

            return obj == null ? default(T) : (T) obj;
        }

        /// <summary>
        /// Reads results of InvokeAll operation.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="inStream">Stream.</param>
        /// <returns>Results of InvokeAll operation.</returns>
        private IDictionary<TK, ICacheEntryProcessorResult<T>> ReadInvokeAllResults<T>(IBinaryStream inStream)
        {
            var count = inStream.ReadInt();

            if (count == -1)
                return null;

            var results = new Dictionary<TK, ICacheEntryProcessorResult<T>>(count);

            for (var i = 0; i < count; i++)
            {
                var key = Unmarshal<TK>(inStream);

                var hasError = inStream.ReadBool();

                results[key] = hasError
                    ? new CacheEntryProcessorResult<T>(ReadException(inStream))
                    : new CacheEntryProcessorResult<T>(Unmarshal<T>(inStream));
            }

            return results;
        }

        /// <summary>
        /// Reads the exception, either in binary wrapper form, or as a pair of strings.
        /// </summary>
        /// <param name="inStream">The stream.</param>
        /// <returns>Exception.</returns>
        private CacheEntryProcessorException ReadException(IBinaryStream inStream)
        {
            var item = Unmarshal<object>(inStream);

            var clsName = item as string;

            if (clsName == null)
                return new CacheEntryProcessorException((Exception) item);

            var msg = Unmarshal<string>(inStream);
            var trace = Unmarshal<string>(inStream);
                
            return new CacheEntryProcessorException(ExceptionUtils.GetException(_ignite, clsName, msg, trace));
        }

        /// <summary>
        /// Read dictionary returned by GET_ALL operation.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Dictionary.</returns>
        private static IDictionary<TK, TV> ReadGetAllDictionary(BinaryReader reader)
        {
            IBinaryStream stream = reader.Stream;

            if (stream.ReadBool())
            {
                int size = stream.ReadInt();

                IDictionary<TK, TV> res = new Dictionary<TK, TV>(size);

                for (int i = 0; i < size; i++)
                {
                    TK key = reader.ReadObject<TK>();
                    TV val = reader.ReadObject<TV>();

                    res[key] = val;
                }

                return res;
            }
            return null;
        }

        /// <summary>
        /// Gets the cache result.
        /// </summary>
        private static CacheResult<TV> GetCacheResult(BinaryReader reader)
        {
            var res = reader == null
                ? new CacheResult<TV>()
                : new CacheResult<TV>(reader.ReadObject<TV>());

            return res;
        }

        /// <summary>
        /// Throws the key not found exception.
        /// </summary>
        private static KeyNotFoundException GetKeyNotFoundException()
        {
            return new KeyNotFoundException("The given key was not present in the cache.");
        }

        /// <summary>
        /// Perform simple out-in operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        private CacheResult<TR> DoOutInOpNullable<T1, TR>(int type, T1 val)
        {
            var res = DoOutInOp<T1, object>(type, val);

            return res == null
                ? new CacheResult<TR>()
                : new CacheResult<TR>((TR)res);
        }

        /// <summary>
        /// Perform out-in operation.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="outAction">Out action.</param>
        /// <returns>Result.</returns>
        private CacheResult<TR> DoOutInOpNullable<TR>(int type, Action<BinaryWriter> outAction)
        {
            var res = DoOutInOp<object>(type, outAction);

            return res == null
                ? new CacheResult<TR>()
                : new CacheResult<TR>((TR)res);
        }

        /// <summary>
        /// Perform simple out-in operation accepting single argument.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="val1">Value.</param>
        /// <param name="val2">Value.</param>
        /// <returns>Result.</returns>
        private CacheResult<TR> DoOutInOpNullable<T1, T2, TR>(int type, T1 val1, T2 val2)
        {
            var res = DoOutInOp<T1, T2, object>(type, val1, val2);

            return res == null
                ? new CacheResult<TR>()
                : new CacheResult<TR>((TR)res);
        }
    }
}
