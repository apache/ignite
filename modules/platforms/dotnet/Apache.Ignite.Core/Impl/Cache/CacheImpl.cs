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
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Cache.Platform;
    using Apache.Ignite.Core.Impl.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Transactions;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Native cache wrapper.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheImpl<TK, TV> : PlatformTargetAdapter, ICache<TK, TV>, ICacheInternal, ICacheLockInternal
    {
        /** Ignite instance. */
        private readonly IIgniteInternal _ignite;

        /** Flag: skip store. */
        private readonly bool _flagSkipStore;

        /** Flag: keep binary. */
        private readonly bool _flagKeepBinary;

        /** Flag: no-retries.*/
        private readonly bool _flagNoRetries;

        /** Flag: partition recover.*/
        private readonly bool _flagPartitionRecover;

        /** Transaction manager. */
        private readonly CacheTransactionManager _txManager;

        /** Pre-allocated delegate. */
        private readonly Func<IBinaryStream, Exception> _readException;

        /** Platform cache. */
        private readonly IPlatformCache _platformCache;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="flagSkipStore">Skip store flag.</param>
        /// <param name="flagKeepBinary">Keep binary flag.</param>
        /// <param name="flagNoRetries">No-retries mode flag.</param>
        /// <param name="flagPartitionRecover">Partition recover mode flag.</param>
        public CacheImpl(IPlatformTargetInternal target,
            bool flagSkipStore, bool flagKeepBinary, bool flagNoRetries, bool flagPartitionRecover
            ) : base(target)
        {
            _ignite = target.Marshaller.Ignite;
            _flagSkipStore = flagSkipStore;
            _flagKeepBinary = flagKeepBinary;
            _flagNoRetries = flagNoRetries;
            _flagPartitionRecover = flagPartitionRecover;

            var configuration = GetConfiguration();
            _txManager = configuration.AtomicityMode == CacheAtomicityMode.Transactional
                ? new CacheTransactionManager(_ignite.GetIgnite().GetTransactions())
                : null;

            _readException = stream => ReadException(Marshaller.StartUnmarshal(stream));

            if (configuration.PlatformCacheConfiguration != null)
            {
                _platformCache = _ignite.PlatformCacheManager.GetOrCreatePlatformCache(configuration);
            }
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite.GetIgnite(); }
        }

        /// <summary>
        /// Returns a value indicating whether this instance has platform cache.
        /// </summary>
        private bool HasPlatformCache
        {
            get { return _platformCache != null && !_platformCache.IsStopped; }
        }

        /// <summary>
        /// Returns a value indicating whether platform caching can be used.
        /// </summary>
        private bool CanUsePlatformCache
        {
            get
            {
                // Platform caching within transaction is not supported for now.
                // Commit/rollback logic requires additional implementation.
                return HasPlatformCache && (_txManager == null || !_txManager.IsInTx());
            }
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task DoOutOpAsync<T1>(CacheOp op, T1 val1)
        {
            return DoOutOpAsync<object, T1>((int) op, val1);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task<TR> DoOutOpAsync<T1, TR>(CacheOp op, T1 val1)
        {
            return DoOutOpAsync<T1, TR>((int) op, val1);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task DoOutOpAsync<T1, T2>(CacheOp op, T1 val1, T2 val2)
        {
            return DoOutOpAsync<T1, T2, object>((int) op, val1, val2);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task<TR> DoOutOpAsync<T1, T2, TR>(CacheOp op, T1 val1, T2 val2)
        {
            return DoOutOpAsync<T1, T2, TR>((int) op, val1, val2);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task DoOutOpAsync(CacheOp op, Action<BinaryWriter> writeAction = null)
        {
            return DoOutOpAsync<object>(op, writeAction);
        }

        /// <summary>
        /// Performs async operation.
        /// </summary>
        private Task<T> DoOutOpAsync<T>(CacheOp op, Action<BinaryWriter> writeAction = null,
            Func<BinaryReader, T> convertFunc = null)
        {
            return DoOutOpAsync((int)op, writeAction, IsKeepBinary, convertFunc);
        }


        /** <inheritDoc /> */
        public string Name
        {
            get { return DoInOp<string>((int)CacheOp.GetName); }
        }

        /** <inheritDoc /> */
        public CacheConfiguration GetConfiguration()
        {
            return DoInOp((int) CacheOp.GetConfig, stream => new CacheConfiguration(
                BinaryUtils.Marshaller.StartUnmarshal(stream)));
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

            var target = DoOutOpObject((int) CacheOp.WithSkipStore);

            return new CacheImpl<TK, TV>(
                target,
                flagSkipStore: true,
                flagKeepBinary: _flagKeepBinary,
                flagNoRetries: _flagNoRetries,
                flagPartitionRecover: _flagPartitionRecover);
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

            var target = DoOutOpObject((int) CacheOp.WithKeepBinary);

            return new CacheImpl<TK1, TV1>(
                target,
                flagSkipStore: _flagSkipStore,
                flagKeepBinary: true,
                flagNoRetries: _flagNoRetries,
                flagPartitionRecover: _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithAllowAtomicOpsInTx()
        {
            var target = DoOutOpObject((int)CacheOp.WithSkipStore);

            return new CacheImpl<TK, TV>(
                target,
                flagSkipStore: true,
                flagKeepBinary: _flagKeepBinary,
                flagNoRetries: _flagNoRetries,
                flagPartitionRecover: _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            IgniteArgumentCheck.NotNull(plc, "plc");

            var cache0 = DoOutOpObject((int)CacheOp.WithExpiryPolicy, w => ExpiryPolicySerializer.WritePolicy(w, plc));

            return new CacheImpl<TK, TV>(
                cache0,
                flagSkipStore: _flagSkipStore,
                flagKeepBinary: _flagKeepBinary,
                flagNoRetries: _flagNoRetries,
                flagPartitionRecover: _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { return _flagKeepBinary; }
        }

        /** <inheritDoc /> */
        public bool IsAllowAtomicOpsInTx
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            DoOutInOpX((int) CacheOp.LoadCache, writer => WriteLoadCacheData(writer, p, args), _readException);
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            return DoOutOpAsync(CacheOp.LoadCacheAsync, writer => WriteLoadCacheData(writer, p, args));
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            DoOutInOpX((int) CacheOp.LocLoadCache, writer => WriteLoadCacheData(writer, p, args), _readException);
        }

        /** <inheritDoc /> */
        public Task LocalLoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            return DoOutOpAsync(CacheOp.LocLoadCacheAsync, writer => WriteLoadCacheData(writer, p, args));
        }

        /// <summary>
        /// Writes the load cache data to the writer.
        /// </summary>
        private void WriteLoadCacheData(BinaryWriter writer, ICacheEntryFilter<TK, TV> p, object[] args)
        {
            if (p != null)
            {
                var p0 = new CacheEntryFilterHolder(p, (k, v) => p.Invoke(new CacheEntry<TK, TV>((TK) k, (TV) v)),
                    Marshaller, IsKeepBinary);

                writer.WriteObjectDetached(p0);
            }
            else
            {
                writer.WriteObjectDetached<CacheEntryFilterHolder>(null);
            }

            if (args != null && args.Length > 0)
            {
                writer.WriteInt(args.Length);

                foreach (var o in args)
                {
                    writer.WriteObjectDetached(o);
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }

        /** <inheritDoc /> */
        public void LoadAll(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            LoadAllAsync(keys, replaceExistingValues).Wait();
        }

        /** <inheritDoc /> */
        public Task LoadAllAsync(IEnumerable<TK> keys, bool replaceExistingValues)
        {
            return DoOutOpAsync(CacheOp.LoadAll, writer =>
            {
                writer.WriteBoolean(replaceExistingValues);
                writer.WriteEnumerable(keys);
            });
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            TV _;
            if (CanUsePlatformCache && _platformCache.TryGetValue(key, out _))
            {
                return true;
            }

            return DoOutOp(CacheOp.ContainsKey, key);
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            TV _;
            if (CanUsePlatformCache && _platformCache.TryGetValue(key, out _))
            {
                return TaskRunner.FromResult(true);
            }

            return DoOutOpAsync<TK, bool>(CacheOp.ContainsKeyAsync, key);
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            if (CanUsePlatformCache)
            {
                var allKeysAreInPlatformCache = true;

                using (var enumerator = keys.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        var key = enumerator.Current;

                        TV _;
                        if (!_platformCache.TryGetValue(key, out _))
                        {
                            allKeysAreInPlatformCache = false;
                            break;
                        }
                    }

                    if (allKeysAreInPlatformCache)
                    {
                        return true;
                    }

                    // ReSharper disable AccessToDisposedClosure (operation is synchronous, not an issue).
                    ICollection<ICacheEntry<TK, TV>> res = null;
                    return DoOutOp(CacheOp.ContainsKeys,
                        writer => WriteKeysOrGetFromPlatformCache(writer, enumerator, ref res, discardResults: true));
                }
            }

            return DoOutOp(CacheOp.ContainsKeys, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            if (CanUsePlatformCache)
            {
                var allKeysAreInPlatformCache = true;

                using (var enumerator = keys.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        var key = enumerator.Current;

                        TV _;
                        if (!_platformCache.TryGetValue(key, out _))
                        {
                            allKeysAreInPlatformCache = false;
                            break;
                        }
                    }

                    if (allKeysAreInPlatformCache)
                    {
                        return TaskRunner.FromResult(true);
                    }

                    // ReSharper disable AccessToDisposedClosure (write is synchronous, not an issue).
                    ICollection<ICacheEntry<TK, TV>> res = null;
                    return DoOutOpAsync<bool>(CacheOp.ContainsKeysAsync,
                        writer => WriteKeysOrGetFromPlatformCache(writer, enumerator, ref res, discardResults: true));
                }
            }

            return DoOutOpAsync<bool>(CacheOp.ContainsKeysAsync, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public TV LocalPeek(TK key, params CachePeekMode[] modes)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            TV res;

            if (TryLocalPeek(key, out res, modes))
                return res;

            throw GetKeyNotFoundException(key);
        }

        /** <inheritDoc /> */
        public bool TryLocalPeek(TK key, out TV value, params CachePeekMode[] modes)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            bool hasPlatformCache;
            var peekModes = IgniteUtils.EncodePeekModes(modes, out hasPlatformCache);

            if (hasPlatformCache)
            {
                if (_platformCache != null && _platformCache.TryGetValue(key, out value))
                {
                    return true;
                }

                if (peekModes == 0)
                {
                    // Only Platform is specified.
                    value = default(TV);
                    return false;
                }
            }

            var res = DoOutInOpX((int) CacheOp.Peek,
                w =>
                {
                    w.WriteObjectDetached(key);
                    w.WriteInt(peekModes);
                },
                (s, r) => r == True ? new CacheResult<TV>(Unmarshal<TV>(s)) : new CacheResult<TV>(),
                _readException);

            value = res.Success ? res.Value : default(TV);

            return res.Success;
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get
            {
                return Get(key);
            }
            set
            {
                Put(key, value);
            }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            TV val;
            if (CanUsePlatformCache && _platformCache.TryGetValue(key, out val))
            {
                return val;
            }

            return DoOutInOpX((int) CacheOp.Get,
                w => w.Write(key),
                (stream, res) =>
                {
                    if (res != True)
                        throw GetKeyNotFoundException(key);

                    return Unmarshal<TV>(stream);
                }, _readException);
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            TV val;
            if (CanUsePlatformCache && _platformCache.TryGetValue(key, out val))
            {
                return TaskRunner.FromResult(val);
            }

            return DoOutOpAsync(CacheOp.GetAsync, w => w.WriteObject(key), reader =>
            {
                if (reader != null)
                    return reader.ReadObject<TV>();

                throw GetKeyNotFoundException(key);
            });
        }

        /** <inheritDoc /> */
        public bool TryGet(TK key, out TV value)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            if (CanUsePlatformCache && _platformCache.TryGetValue(key, out value))
            {
                return true;
            }

            var res = DoOutInOpNullable(CacheOp.Get, key);

            value = res.Value;

            return res.Success;
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.GetAsync, w => w.WriteObject(key), reader => GetCacheResult(reader));
        }

        /** <inheritDoc /> */
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTxIfNeeded();

            if (CanUsePlatformCache)
            {
                // Get what we can from platform cache, and the rest from Java.
                // Enumerator usage is necessary to satisfy performance requirements:
                // * No overhead when all keys are resolved from platform cache.
                // * Do not enumerate keys twice.
                // * Do not allocate a collection for keys.

                // Resulting collection is null by default:
                // When no keys are found in platform cache, there is no extra allocations,
                // because result size will be known.
                ICollection<ICacheEntry<TK, TV>> res = null;
                var allKeysAreInPlatformCache = true;

                using (var enumerator = keys.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        var key = enumerator.Current;

                        TV val;
                        if (_platformCache.TryGetValue(key, out val))
                        {
                            res = res ?? new List<ICacheEntry<TK, TV>>();
                            res.Add(new CacheEntry<TK, TV>(key, val));
                        }
                        else
                        {
                            allKeysAreInPlatformCache = false;
                            break;
                        }
                    }

                    if (allKeysAreInPlatformCache)
                    {
                        return res;
                    }

                    // ReSharper disable AccessToDisposedClosure (operation is synchronous, not an issue).
                    return DoOutInOpX((int) CacheOp.GetAll,
                        w => WriteKeysOrGetFromPlatformCache(w, enumerator, ref res),
                        (s, r) => r == True
                            ? ReadGetAllDictionary(Marshaller.StartUnmarshal(s, _flagKeepBinary), res)
                            : res,
                        _readException);
                    // ReSharper restore AccessToDisposedClosure
                }
            }

            return DoOutInOpX((int) CacheOp.GetAll,
                writer => writer.WriteEnumerable(keys),
                (s, r) => r == True
                    ? ReadGetAllDictionary(Marshaller.StartUnmarshal(s, _flagKeepBinary))
                    : null,
                _readException);
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTxIfNeeded();

            if (CanUsePlatformCache)
            {
                // Get what we can from platform cache, and the rest from Java.
                // Duplicates the logic from GetAll above, but extracting common parts increases complexity too much.
                ICollection<ICacheEntry<TK, TV>> res = null;
                var allKeysAreInPlatformCache = true;

                using (var enumerator = keys.GetEnumerator())
                {
                    while (enumerator.MoveNext())
                    {
                        var key = enumerator.Current;

                        TV val;
                        if (_platformCache.TryGetValue(key, out val))
                        {
                            res = res ?? new List<ICacheEntry<TK, TV>>();
                            res.Add(new CacheEntry<TK, TV>(key, val));
                        }
                        else
                        {
                            allKeysAreInPlatformCache = false;
                            break;
                        }
                    }

                    if (allKeysAreInPlatformCache)
                    {
                        return TaskRunner.FromResult(res);
                    }

                    // ReSharper disable AccessToDisposedClosure (write operation is synchronous, not an issue).
                    return DoOutOpAsync(CacheOp.GetAllAsync,
                        w => WriteKeysOrGetFromPlatformCache(w, enumerator, ref res),
                        r => ReadGetAllDictionary(r, res));
                    // ReSharper restore AccessToDisposedClosure
                }
            }

            return DoOutOpAsync(CacheOp.GetAllAsync,
                w => w.WriteEnumerable(keys),
                r => ReadGetAllDictionary(r));
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            var platformCache = CanUsePlatformCache;

            try
            {
                if (platformCache)
                {
                    // Platform cache optimization on primary nodes:
                    // Update from Java comes in this same thread, so we don't need to pass key/val from Java.
                    // However, we still rely on a callback to maintain the order of updates.
                    _platformCache.SetThreadLocalPair(key, val);
                }

                DoOutOp(CacheOp.PutWithPlatformCache, key, val);
            }
            finally
            {
                if (platformCache)
                {
                    _platformCache.ResetThreadLocalPair();
                }
            }
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.PutAsync, key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutInOpNullable(CacheOp.GetAndPut, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.GetAndPutAsync, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutInOpNullable(CacheOp.GetAndReplace, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.GetAndReplaceAsync, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            return DoOutInOpNullable(CacheOp.GetAndRemove, key);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.GetAndRemoveAsync, w => w.WriteObject(key), r => GetCacheResult(r));
        }

        /** <inheritdoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOp(CacheOp.PutIfAbsent, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.PutIfAbsentAsync, key, val);
        }

        /** <inheritdoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutInOpNullable(CacheOp.GetAndPutIfAbsent, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.GetAndPutIfAbsentAsync, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOp(CacheOp.Replace2, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.Replace2Async, key, val);
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            StartTxIfNeeded();

            return DoOutOp(CacheOp.Replace3, key, oldVal, newVal);
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            StartTxIfNeeded();

            return DoOutOpAsync<bool>(CacheOp.Replace3Async, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(oldVal);
                w.WriteObjectDetached(newVal);
            });
        }

        /** <inheritdoc /> */
        public void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            StartTxIfNeeded();

            DoOutOp(CacheOp.PutAll, writer => writer.WriteDictionary(vals));
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IEnumerable<KeyValuePair<TK, TV>> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.PutAllAsync, writer => writer.WriteDictionary(vals));
        }

        /** <inheritdoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(CacheOp.LocEvict, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            DoOutInOp((int) CacheOp.ClearCache);
        }

        /** <inheritDoc /> */
        public Task ClearAsync()
        {
            return DoOutOpAsync(CacheOp.ClearCacheAsync);
        }

        /** <inheritdoc /> */
        public void Clear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp(CacheOp.Clear, key);
        }

        /** <inheritDoc /> */
        public Task ClearAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAsync(CacheOp.ClearAsync, key);
        }

        /** <inheritdoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(CacheOp.ClearAll, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(CacheOp.ClearAllAsync, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritdoc /> */
        public void LocalClear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp(CacheOp.LocalClear, key);
        }

        /** <inheritdoc /> */
        public void LocalClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(CacheOp.LocalClearAll, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritdoc /> */
        public bool Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            return DoOutOp(CacheOp.RemoveObj, key);
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTxIfNeeded();

            return DoOutOpAsync<TK, bool>(CacheOp.RemoveObjAsync, key);
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOp(CacheOp.RemoveBool, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.RemoveBoolAsync, key, val);
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTxIfNeeded();

            DoOutOp(CacheOp.RemoveAll, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.RemoveAllAsync, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            StartTxIfNeeded();

            DoOutInOp((int) CacheOp.RemoveAll2);
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            StartTxIfNeeded();

            return DoOutOpAsync(CacheOp.RemoveAll2Async);
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
            return SizeAsync0(modes);
        }

        /** <inheritDoc /> */
        public long GetSizeLong(params CachePeekMode[] modes)
        {
            return Size0(false, null, modes);
        }

        /** <inheritDoc /> */
        public long GetSizeLong(int partition, params CachePeekMode[] modes)
        {
            return Size0(false, partition, modes);
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeLongAsync(params CachePeekMode[] modes)
        {
            return SizeAsync0(null, modes);
        }

        /** <inheritDoc /> */
        public Task<long> GetSizeLongAsync(int partition, params CachePeekMode[] modes)
        {
            return SizeAsync0(partition, modes);
        }

        /** <inheritDoc /> */
        public long GetLocalSizeLong(params CachePeekMode[] modes)
        {
            return Size0(true, null, modes);
        }

        /** <inheritDoc /> */
        public long GetLocalSizeLong(int partition, params CachePeekMode[] modes)
        {
            return Size0(true, partition, modes);
        }

        /// <summary>
        /// Internal integer size routine.
        /// </summary>
        /// <param name="loc">Local flag.</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private int Size0(bool loc, params CachePeekMode[] modes)
        {
            int platformCacheSize;
            bool onlyPlatform;
            var modes0 = EncodePeekModes(null, modes, out onlyPlatform, out platformCacheSize);

            if (onlyPlatform)
            {
                return platformCacheSize;
            }

            var op = loc ? CacheOp.SizeLoc : CacheOp.Size;

            return (int) DoOutInOp((int) op, modes0) + platformCacheSize;
        }

        /// <summary>
        /// Internal long size routine.
        /// </summary>
        /// <param name="loc">Local flag.</param>
        /// <param name="part">Partition number</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private long Size0(bool loc, int? part, params CachePeekMode[] modes)
        {
            int platformCacheSize;
            bool onlyPlatform;
            var modes0 = EncodePeekModes(part, modes, out onlyPlatform, out platformCacheSize);

            if (onlyPlatform)
            {
                return platformCacheSize;
            }

            var op = loc ? CacheOp.SizeLongLoc : CacheOp.SizeLong;

            return DoOutOp((int) op, writer =>
            {
                writer.WriteInt(modes0);

                if (part != null)
                {
                    writer.WriteBoolean(true);
                    writer.WriteInt((int) part);
                }
                else
                {
                    writer.WriteBoolean(false);
                }
            }) + platformCacheSize;
        }

        /// <summary>
        /// Internal async integer size routine.
        /// </summary>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private Task<int> SizeAsync0(params CachePeekMode[] modes)
        {
            int platformCacheSize;
            bool onlyPlatform;
            var modes0 = EncodePeekModes(null, modes, out onlyPlatform, out platformCacheSize);

            if (onlyPlatform)
            {
                return TaskRunner.FromResult(platformCacheSize);
            }

            return DoOutOpAsync<int>(CacheOp.SizeAsync, w => w.WriteInt(modes0))
                .ContWith(t => t.Result + platformCacheSize, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Internal async long size routine.
        /// </summary>
        /// <param name="part">Partition number</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private Task<long> SizeAsync0(int? part, params CachePeekMode[] modes)
        {
            int platformCacheSize;
            bool onlyPlatform;
            var modes0 = EncodePeekModes(part, modes, out onlyPlatform, out platformCacheSize);

            if (onlyPlatform)
            {
                return TaskRunner.FromResult((long) platformCacheSize);
            }

            return DoOutOpAsync<long>(CacheOp.SizeLongAsync, writer =>
            {
                writer.WriteInt(modes0);

                if (part != null)
                {
                    writer.WriteBoolean(true);
                    writer.WriteInt((int) part);
                }
                else
                {
                    writer.WriteBoolean(false);
                }
            }).ContWith(t => t.Result + platformCacheSize, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Encodes peek modes, includes native platform check.
        /// </summary>
        private int EncodePeekModes(int? part, CachePeekMode[] modes, out bool onlyPlatform, out int size)
        {
            size = 0;
            onlyPlatform = false;

            bool hasPlatformCache;
            var modes0 = IgniteUtils.EncodePeekModes(modes, out hasPlatformCache);

            if (hasPlatformCache)
            {
                if (_platformCache != null)
                {
                    size += _platformCache.GetSize(part);
                }

                if (modes0 == 0)
                {
                    onlyPlatform = true;
                }
            }

            return modes0;
        }

        /** <inheritdoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTxIfNeeded();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            var ptr = AllocateIfNoTx(holder);

            try
            {
                return DoOutInOpX((int) CacheOp.Invoke,
                    writer =>
                    {
                        writer.WriteObjectDetached(key);
                        writer.WriteLong(ptr);
                        writer.WriteObjectDetached(holder);
                    },
                    (input, res) => res == True ? Unmarshal<TRes>(input) : default(TRes),
                    _readException);
            }
            finally
            {
                if (ptr != 0)
                    _ignite.HandleRegistry.Release(ptr);
            }
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTxIfNeeded();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            var ptr = AllocateIfNoTx(holder);

            try
            {
                return DoOutOpAsync(CacheOp.InvokeAsync, writer =>
                    {
                        writer.WriteObjectDetached(key);
                        writer.WriteLong(ptr);
                        writer.WriteObjectDetached(holder);
                    },
                    reader =>
                    {
                        if (ptr != 0)
                            _ignite.HandleRegistry.Release(ptr);

                        if (reader == null)
                            return default(TRes);

                        var hasError = reader.ReadBoolean();

                        if (hasError)
                            throw ReadException(reader);

                        return reader.ReadObject<TRes>();
                    });
            }
            catch (Exception)
            {
                if (ptr != 0)
                    _ignite.HandleRegistry.Release(ptr);

                throw;
            }
        }

        /** <inheritdoc /> */
        public ICollection<ICacheEntryProcessorResult<TK, TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTxIfNeeded();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            var ptr = AllocateIfNoTx(holder);

            try
            {
                return DoOutInOpX((int) CacheOp.InvokeAll,
                    writer =>
                    {
                        writer.WriteEnumerable(keys);
                        writer.WriteLong(ptr);
                        writer.Write(holder);
                    },
                    (input, res) => res == True
                        ? ReadInvokeAllResults<TRes>(Marshaller.StartUnmarshal(input, IsKeepBinary))
                        : null, _readException);
            }
            finally
            {
                if (ptr != 0)
                    _ignite.HandleRegistry.Release(ptr);
            }
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntryProcessorResult<TK, TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTxIfNeeded();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            var ptr = AllocateIfNoTx(holder);

            try
            {
                return DoOutOpAsync(CacheOp.InvokeAllAsync,
                    writer =>
                    {
                        writer.WriteEnumerable(keys);
                        writer.WriteLong(ptr);
                        writer.Write(holder);
                    },
                    reader =>
                    {
                        if (ptr != 0)
                            _ignite.HandleRegistry.Release(ptr);

                        return ReadInvokeAllResults<TRes>(reader);
                    });
            }
            catch (Exception)
            {
                if (ptr != 0)
                    _ignite.HandleRegistry.Release(ptr);

                throw;
            }
        }

        /** <inheritDoc /> */
        public T DoOutInOpExtension<T>(int extensionId, int opCode, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, T> readFunc)
        {
            return DoOutInOpX((int) CacheOp.Extension, writer =>
                {
                    writer.WriteInt(extensionId);
                    writer.WriteInt(opCode);
                    writeAction(writer);
                },
                (input, res) => res == True
                    ? readFunc(Marshaller.StartUnmarshal(input))
                    : default(T), _readException);
        }

        /** <inheritdoc /> */
        public ICacheLock Lock(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpX((int) CacheOp.Lock, w => w.Write(key),
                (stream, res) => new CacheLock(stream.ReadInt(), this), _readException);
        }

        /** <inheritdoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpX((int) CacheOp.LockAll, w => w.WriteEnumerable(keys),
                (stream, res) => new CacheLock(stream.ReadInt(), this), _readException);
        }

        /** <inheritdoc /> */
        public bool IsLocalLocked(TK key, bool byCurrentThread)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp(CacheOp.IsLocalLocked, writer =>
            {
                writer.Write(key);
                writer.WriteBoolean(byCurrentThread);
            });
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            return DoInOp((int) CacheOp.GlobalMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics(IClusterGroup clusterGroup)
        {
            IgniteArgumentCheck.NotNull(clusterGroup, "clusterGroup");

            var prj = clusterGroup as ClusterGroupImpl;

            if (prj == null)
                throw new ArgumentException("Unexpected IClusterGroup implementation: " + clusterGroup.GetType());

            return prj.GetCacheMetrics(Name);
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetLocalMetrics()
        {
            return DoInOp((int) CacheOp.LocalMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public void EnableStatistics(bool enabled)
        {
            DoOutInOp((int) CacheOp.EnableStatistics, enabled ? True : False);
        }

        /** <inheritDoc /> */
        public void ClearStatistics()
        {
            DoOutInOp((int)CacheOp.ClearStatistics);
        }

        /** <inheritDoc /> */
        public Task Rebalance()
        {
            return DoOutOpAsync(CacheOp.Rebalance);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            if (_flagNoRetries)
                return this;

            var target = DoOutOpObject((int) CacheOp.WithNoRetries);

            return new CacheImpl<TK, TV>(
                target,
                flagSkipStore: _flagSkipStore,
                flagKeepBinary: _flagKeepBinary,
                flagNoRetries: true,
                flagPartitionRecover: _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithPartitionRecover()
        {
            if (_flagPartitionRecover)
                return this;

            var target = DoOutOpObject((int) CacheOp.WithPartitionRecover);

            return new CacheImpl<TK, TV>(
                target,
                flagSkipStore: _flagSkipStore,
                flagKeepBinary: _flagKeepBinary,
                flagNoRetries: _flagNoRetries,
                flagPartitionRecover: true);
        }

        /** <inheritDoc /> */
        public ICollection<int> GetLostPartitions()
        {
            return DoInOp((int) CacheOp.GetLostPartitions, s =>
            {
                var cnt = s.ReadInt();

                var res = new List<int>(cnt);

                if (cnt > 0)
                {
                    for (var i = 0; i < cnt; i++)
                    {
                        res.Add(s.ReadInt());
                    }
                }

                return res;
            });
        }

        #region Queries

        /** <inheritDoc /> */
        public IFieldsQueryCursor Query(SqlFieldsQuery qry)
        {
            var cursor = QueryFieldsInternal(qry);

            return new FieldsQueryCursor(cursor, _flagKeepBinary);
        }

        /** <inheritDoc /> */
        [Obsolete("Use Query(SqlFieldsQuery qry) instead.")]
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            return Query(qry, (reader, count) => (IList) FieldsQueryCursor.ReadFieldsArrayList(reader, count));
        }

        /** <inheritDoc /> */
        public IQueryCursor<T> Query<T>(SqlFieldsQuery qry, Func<IBinaryRawReader, int, T> readerFunc)
        {
            var cursor = QueryFieldsInternal(qry);

            return new FieldsQueryCursor<T>(cursor, _flagKeepBinary, readerFunc);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            if (HasPlatformCache)
            {
                // NOTE: Users can pass a ScanQuery that has different generic arguments.
                // We do not support this scenario for platform cache scan optimization.
                var scan = qry as ScanQuery<TK, TV>;

                // Local scan with Partition can be satisfied directly from platform cache on server nodes.
                if (scan != null && scan.Local && scan.Partition != null)
                {
                    return ScanPlatformCache(scan);
                }
            }

            var cursor = DoOutOpObject((int) qry.OpId, writer => qry.Write(writer, IsKeepBinary));

            return new QueryCursor<TK, TV>(cursor, _flagKeepBinary);
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

        /** <inheritdoc /> */
        public IContinuousQueryHandleFields QueryContinuous(ContinuousQuery<TK, TV> qry, SqlFieldsQuery initialQry)
        {
            qry.Validate();

            return new ContinuousQueryHandleImpl<TK, TV>(qry, Marshaller, _flagKeepBinary,
                writeAction => DoOutOpObject((int) CacheOp.QryContinuous, writeAction), initialQry);
        }

        /// <summary>
        /// QueryContinuous implementation.
        /// </summary>
        private IContinuousQueryHandle<ICacheEntry<TK, TV>> QueryContinuousImpl(ContinuousQuery<TK, TV> qry,
            QueryBase initialQry)
        {
            qry.Validate();

            return new ContinuousQueryHandleImpl<TK, TV>(qry, Marshaller, _flagKeepBinary,
                writeAction => DoOutOpObject((int) CacheOp.QryContinuous, writeAction), initialQry);
        }

        #endregion

        #region Enumerable support

        /** <inheritdoc /> */
        public IEnumerable<ICacheEntry<TK, TV>> GetLocalEntries(CachePeekMode[] peekModes)
        {
            bool hasPlatformCacheMode;
            var encodedPeekModes = IgniteUtils.EncodePeekModes(peekModes, out hasPlatformCacheMode);
            var onlyPlatformCacheMode = hasPlatformCacheMode && encodedPeekModes == 0;

            if (HasPlatformCache && hasPlatformCacheMode)
            {
                if (onlyPlatformCacheMode)
                {
                    // Only platform cache.
                    return _platformCache.GetEntries<TK, TV>();
                }

                return _platformCache.GetEntries<TK, TV>().Concat(new CacheEnumerable<TK, TV>(this, encodedPeekModes));
            }

            if (!HasPlatformCache && onlyPlatformCacheMode)
            {
                return Enumerable.Empty<ICacheEntry<TK, TV>>();
            }

            return new CacheEnumerable<TK, TV>(this, encodedPeekModes);
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
            {
                var target = DoOutOpObject((int) CacheOp.LocIterator, (IBinaryStream s) => s.WriteInt(peekModes));

                return new CacheEnumerator<TK, TV>(target, _flagKeepBinary);
            }

            return new CacheEnumerator<TK, TV>(DoOutOpObject((int) CacheOp.Iterator), _flagKeepBinary);
        }

        #endregion

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IBinaryStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, _flagKeepBinary);
        }

        /// <summary>
        /// Reads results of InvokeAll operation.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="reader">Stream.</param>
        /// <returns>Results of InvokeAll operation.</returns>
        private ICollection<ICacheEntryProcessorResult<TK, T>> ReadInvokeAllResults<T>(BinaryReader reader)
        {
            var count = reader.ReadInt();

            if (count == -1)
                return null;

            var results = new List<ICacheEntryProcessorResult<TK, T>>(count);

            for (var i = 0; i < count; i++)
            {
                var key = reader.ReadObject<TK>();

                var hasError = reader.ReadBoolean();

                results.Add(hasError
                    ? new CacheEntryProcessorResult<TK, T>(key, ReadException(reader))
                    : new CacheEntryProcessorResult<TK, T>(key, reader.ReadObject<T>()));
            }

            return results;
        }

        /// <summary>
        /// Reads the exception, either in binary wrapper form, or as a pair of strings.
        /// </summary>
        /// <param name="reader">The stream.</param>
        /// <returns>Exception.</returns>
        private Exception ReadException(BinaryReader reader)
        {
            var item = reader.ReadObject<object>();

            var clsName = item as string;

            if (clsName == null)
                return new CacheEntryProcessorException((Exception) item);

            var msg = reader.ReadObject<string>();
            var trace = reader.ReadObject<string>();
            var inner = reader.ReadBoolean() ? reader.ReadObject<Exception>() : null;

            return ExceptionUtils.GetException(_ignite, clsName, msg, trace, reader, inner);
        }

        /// <summary>
        /// Read dictionary returned by GET_ALL operation.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="res">Resulting collection.</param>
        /// <returns>Dictionary.</returns>
        private static ICollection<ICacheEntry<TK, TV>> ReadGetAllDictionary(BinaryReader reader,
            ICollection<ICacheEntry<TK, TV>> res = null)
        {
            if (reader == null)
                return null;

            IBinaryStream stream = reader.Stream;

            if (stream.ReadBool())
            {
                int size = stream.ReadInt();

                res = res ?? new List<ICacheEntry<TK, TV>>(size);

                for (int i = 0; i < size; i++)
                {
                    TK key = reader.ReadObject<TK>();
                    TV val = reader.ReadObject<TV>();

                    res.Add(new CacheEntry<TK, TV>(key, val));
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
        private static KeyNotFoundException GetKeyNotFoundException(TK key)
        {
            return new KeyNotFoundException("The given key was not present in the cache: " + key);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1>(CacheOp op, T1 x)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.Write(x);
            }, _readException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1, T2>(CacheOp op, T1 x, T2 y)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.WriteObjectDetached(x);
                w.WriteObjectDetached(y);
            }, _readException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1, T2, T3>(CacheOp op, T1 x, T2 y, T3 z)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.WriteObjectDetached(x);
                w.WriteObjectDetached(y);
                w.WriteObjectDetached(z);
            }, _readException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp(CacheOp op, Action<BinaryWriter> write)
        {
            return DoOutInOpX((int) op, write, _readException);
        }

        /// <summary>
        /// Does the out-in op.
        /// </summary>
        private CacheResult<TV> DoOutInOpNullable(CacheOp cacheOp, TK x)
        {
            return DoOutInOpX((int)cacheOp,
                w => w.Write(x),
                (stream, res) => res == True ? new CacheResult<TV>(Unmarshal<TV>(stream)) : new CacheResult<TV>(),
                _readException);
        }

        /// <summary>
        /// Does the out-in op.
        /// </summary>
        private CacheResult<TV> DoOutInOpNullable<T1, T2>(CacheOp cacheOp, T1 x, T2 y)
        {
            return DoOutInOpX((int)cacheOp,
                w =>
                {
                    w.WriteObjectDetached(x);
                    w.WriteObjectDetached(y);
                },
                (stream, res) => res == True ? new CacheResult<TV>(Unmarshal<TV>(stream)) : new CacheResult<TV>(),
                _readException);
        }

        /** <inheritdoc /> */
        public void Enter(long id)
        {
            DoOutInOp((int) CacheOp.EnterLock, id);
        }

        /** <inheritdoc /> */
        public bool TryEnter(long id, TimeSpan timeout)
        {
            return DoOutOp((int) CacheOp.TryEnterLock, (IBinaryStream s) =>
                   {
                       s.WriteLong(id);
                       s.WriteLong((long) timeout.TotalMilliseconds);
                   }) == True;
        }

        /** <inheritdoc /> */
        public void Exit(long id)
        {
            DoOutInOp((int) CacheOp.ExitLock, id);
        }

        /** <inheritdoc /> */
        public void Close(long id)
        {
            DoOutInOp((int) CacheOp.CloseLock, id);
        }

        /** <inheritdoc /> */
        public IQueryMetrics GetQueryMetrics()
        {
            return DoInOp((int)CacheOp.QueryMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new QueryMetricsImpl(reader);
            });
        }

        /** <inheritdoc /> */
        public void ResetQueryMetrics()
        {
            DoOutInOp((int)CacheOp.ResetQueryMetrics);
        }

        /** <inheritdoc /> */
        public void PreloadPartition(int partition)
        {
            DoOutInOp((int)CacheOp.PreloadPartition, partition);
        }

        /** <inheritdoc /> */
        public Task PreloadPartitionAsync(int partition)
        {
            return DoOutOpAsync(CacheOp.PreloadPartitionAsync, w => w.WriteInt(partition));
        }

        /** <inheritdoc /> */
        public bool LocalPreloadPartition(int partition)
        {
            return DoOutOp(CacheOp.LocalPreloadPartition, w => w.WriteInt(partition));
        }

        /// <summary>
        /// Starts a transaction when applicable.
        /// </summary>
        private void StartTxIfNeeded()
        {
            if (_txManager != null)
                _txManager.StartTx();
        }

        /// <summary>
        /// Allocates a handle only when there is no active transaction.
        /// </summary>
        /// <returns>Handle or 0 when there is an active transaction.</returns>
        private long AllocateIfNoTx(object obj)
        {
            // With transactions, actual cache operation execution is delayed, so we don't control handle lifetime.
            if (_txManager != null && _txManager.IsInTx())
                return 0;

            return _ignite.HandleRegistry.Allocate(obj);
        }

        /// <summary>
        /// Enumerates provided keys, looking for platform cache values.
        /// Keys that are not in platform cache are written to the writer.
        /// </summary>
        private void WriteKeysOrGetFromPlatformCache(BinaryWriter writer, IEnumerator<TK> enumerator,
            ref ICollection<ICacheEntry<TK, TV>> res, bool discardResults = false)
        {
            var count = 1;
            var pos = writer.Stream.Position;
            writer.WriteInt(count); // Reserve count.

            writer.WriteObjectDetached(enumerator.Current);

            while (enumerator.MoveNext())
            {
                TV val;
                if (_platformCache.TryGetValue(enumerator.Current, out val))
                {
                    if (!discardResults)
                    {
                        res = res ?? new List<ICacheEntry<TK, TV>>();
                        res.Add(new CacheEntry<TK, TV>(enumerator.Current, val));
                    }
                }
                else
                {
                    writer.WriteObjectDetached(enumerator.Current);
                    count++;
                }
            }

            var endPos = writer.Stream.Position;
            writer.Stream.Seek(pos, SeekOrigin.Begin);
            writer.WriteInt(count);
            writer.Stream.Seek(endPos, SeekOrigin.Begin);
        }

        /// <summary>
        /// Reserves specified partition.
        /// </summary>
        private void ReservePartition(int part)
        {
            var reserved = Target.InLongOutLong((int) CacheOp.ReservePartition, part) == True;

            if (!reserved)
            {
                // Java exception for Scan Query in this case is 'No queryable nodes for partition N',
                // which is a bit confusing.
                throw new InvalidOperationException(
                    string.Format("Failed to reserve partition {0}, it does not belong to the local node.", part));
            }
        }

        /// <summary>
        /// Releases specified partition.
        /// </summary>
        private void ReleasePartition(int part)
        {
            var released = Target.InLongOutLong((int) CacheOp.ReleasePartition, part) == True;

            if (!released)
            {
                throw new InvalidOperationException("Failed to release partition: " + part);
            }
        }

        /// <summary>
        /// Performs Scan query over platform cache.
        /// </summary>
        private IQueryCursor<ICacheEntry<TK, TV>> ScanPlatformCache(ScanQuery<TK, TV> qry)
        {
            var filter = qry.Filter;

            if (filter != null)
            {
                ResourceProcessor.Inject(filter, Marshaller.Ignite);
            }

            var part = qry.Partition;
            Action dispose = null;

            if (part != null)
            {
                ReservePartition((int) part);

                dispose = () => ReleasePartition((int) part);
            }

            return new PlatformCacheQueryCursor<TK, TV>(_platformCache, filter, part, dispose);
        }

        /// <summary>
        /// Executes fields query.
        /// </summary>
        private IPlatformTargetInternal QueryFieldsInternal(SqlFieldsQuery qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            if (string.IsNullOrEmpty(qry.Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            return DoOutOpObject((int) CacheOp.QrySqlFields, writer => qry.Write(writer));
        }
    }
}
