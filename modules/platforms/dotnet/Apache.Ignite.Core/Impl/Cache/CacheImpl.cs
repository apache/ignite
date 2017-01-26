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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Transactions;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Native cache wrapper.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheImpl<TK, TV> : PlatformTarget, ICache<TK, TV>, ICacheInternal, ICacheLockInternal
    {
        /** Ignite instance. */
        private readonly Ignite _ignite;
        
        /** Flag: skip store. */
        private readonly bool _flagSkipStore;

        /** Flag: keep binary. */
        private readonly bool _flagKeepBinary;

        /** Flag: no-retries.*/
        private readonly bool _flagNoRetries;

        /** Transaction manager. */
        private readonly CacheTransactionManager _txManager;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="flagSkipStore">Skip store flag.</param>
        /// <param name="flagKeepBinary">Keep binary flag.</param>
        /// <param name="flagNoRetries">No-retries mode flag.</param>
        public CacheImpl(Ignite grid, IUnmanagedTarget target, Marshaller marsh,
            bool flagSkipStore, bool flagKeepBinary, bool flagNoRetries) : base(target, marsh)
        {
            Debug.Assert(grid != null);

            _ignite = grid;
            _flagSkipStore = flagSkipStore;
            _flagKeepBinary = flagKeepBinary;
            _flagNoRetries = flagNoRetries;

            _txManager = GetConfiguration().AtomicityMode == CacheAtomicityMode.Transactional
                ? new CacheTransactionManager(grid.GetTransactions())
                : null;
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite; }
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

            return new CacheImpl<TK, TV>(_ignite, DoOutOpObject((int) CacheOp.WithSkipStore), Marshaller,
                true, _flagKeepBinary, true);
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

            return new CacheImpl<TK1, TV1>(_ignite, DoOutOpObject((int) CacheOp.WithKeepBinary), Marshaller,
                _flagSkipStore, true, _flagNoRetries);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            IgniteArgumentCheck.NotNull(plc, "plc");

            var cache0 = DoOutOpObject((int)CacheOp.WithExpiryPolicy, w => ExpiryPolicySerializer.WritePolicy(w, plc));

            return new CacheImpl<TK, TV>(_ignite, cache0, Marshaller, _flagSkipStore, _flagKeepBinary, _flagNoRetries);
        }

        /** <inheritDoc /> */
        public bool IsKeepBinary
        {
            get { return _flagKeepBinary; }
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            DoOutInOpX((int) CacheOp.LoadCache, writer => WriteLoadCacheData(writer, p, args), ReadException);
        }

        /** <inheritDoc /> */
        public Task LoadCacheAsync(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            return DoOutOpAsync(CacheOp.LoadCacheAsync, writer => WriteLoadCacheData(writer, p, args));
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            DoOutInOpX((int) CacheOp.LocLoadCache, writer => WriteLoadCacheData(writer, p, args), ReadException);
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

                writer.WriteObject(p0);
            }
            else
                writer.WriteObject<CacheEntryFilterHolder>(null);

            if (args != null && args.Length > 0)
            {
                writer.WriteInt(args.Length);

                foreach (var o in args)
                    writer.WriteObject(o);
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
                WriteEnumerable(writer, keys);
            });
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp(CacheOp.ContainsKey, key);
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeyAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAsync<TK, bool>(CacheOp.ContainsKeyAsync, key);
        }

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOp(CacheOp.ContainsKeys, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync<bool>(CacheOp.ContainsKeysAsync, writer => WriteEnumerable(writer, keys));
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

            var res = DoOutInOpX((int)CacheOp.Peek,
                w =>
                {
                    w.Write(key);
                    w.WriteInt(EncodePeekModes(modes));
                },
                (s, r) => r == True ? new CacheResult<TV>(Unmarshal<TV>(s)) : new CacheResult<TV>(),
                ReadException);

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

            return DoOutInOpX((int) CacheOp.Get,
                w => w.Write(key),
                (stream, res) =>
                {
                    if (res != True)
                        throw GetKeyNotFoundException();

                    return Unmarshal<TV>(stream);
                }, ReadException);
        }

        /** <inheritDoc /> */
        public Task<TV> GetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAsync(CacheOp.GetAsync, w => w.WriteObject(key), reader =>
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

            var res = DoOutInOpNullable(CacheOp.Get, key);

            value = res.Value;

            return res.Success;
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> TryGetAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOpAsync(CacheOp.GetAsync, w => w.WriteObject(key), reader => GetCacheResult(reader));
        }

        /** <inheritDoc /> */
        public IDictionary<TK, TV> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpX((int) CacheOp.GetAll,
                writer => WriteEnumerable(writer, keys),
                (s, r) => r == True ? ReadGetAllDictionary(Marshaller.StartUnmarshal(s, _flagKeepBinary)) : null,
                ReadException);
        }

        /** <inheritDoc /> */
        public Task<IDictionary<TK, TV>> GetAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(CacheOp.GetAllAsync, w => WriteEnumerable(w, keys), r => ReadGetAllDictionary(r));
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            DoOutOp(CacheOp.Put, key, val);
        }

        /** <inheritDoc /> */
        public Task PutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync(CacheOp.PutAsync, key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutInOpNullable(CacheOp.GetAndPut, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync(CacheOp.GetAndPutAsync, w =>
            {
                w.WriteObject(key);
                w.WriteObject(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutInOpNullable(CacheOp.GetAndReplace, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync(CacheOp.GetAndReplaceAsync, w =>
            {
                w.WriteObject(key);
                w.WriteObject(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTx();

            return DoOutInOpNullable(CacheOp.GetAndRemove, key);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndRemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTx();

            return DoOutOpAsync(CacheOp.GetAndRemoveAsync, w => w.WriteObject(key), r => GetCacheResult(r));
        }

        /** <inheritdoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOp(CacheOp.PutIfAbsent, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> PutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.PutIfAbsentAsync, key, val);
        }

        /** <inheritdoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutInOpNullable(CacheOp.GetAndPutIfAbsent, key, val);
        }

        /** <inheritDoc /> */
        public Task<CacheResult<TV>> GetAndPutIfAbsentAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync(CacheOp.GetAndPutIfAbsentAsync, w =>
            {
                w.WriteObject(key);
                w.WriteObject(val);
            }, r => GetCacheResult(r));
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOp(CacheOp.Replace2, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.Replace2Async, key, val);
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            StartTx();

            return DoOutOp(CacheOp.Replace3, key, oldVal, newVal);
        }

        /** <inheritDoc /> */
        public Task<bool> ReplaceAsync(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(oldVal, "oldVal");
            IgniteArgumentCheck.NotNull(newVal, "newVal");

            StartTx();

            return DoOutOpAsync<bool>(CacheOp.Replace3Async, w =>
            {
                w.WriteObject(key);
                w.WriteObject(oldVal);
                w.WriteObject(newVal);
            });
        }

        /** <inheritdoc /> */
        public void PutAll(IDictionary<TK, TV> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            StartTx();

            DoOutOp(CacheOp.PutAll, writer => WriteDictionary(writer, vals));
        }

        /** <inheritDoc /> */
        public Task PutAllAsync(IDictionary<TK, TV> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            StartTx();

            return DoOutOpAsync(CacheOp.PutAllAsync, writer => WriteDictionary(writer, vals));
        }

        /** <inheritdoc /> */
        public void LocalEvict(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(CacheOp.LocEvict, writer => WriteEnumerable(writer, keys));
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

            DoOutOp(CacheOp.ClearAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public Task ClearAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(CacheOp.ClearAllAsync, writer => WriteEnumerable(writer, keys));
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

            DoOutOp(CacheOp.LocalClearAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public bool Remove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTx();

            return DoOutOp(CacheOp.RemoveObj, key);
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            StartTx();

            return DoOutOpAsync<TK, bool>(CacheOp.RemoveObjAsync, key);
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOp(CacheOp.RemoveBool, key, val);
        }

        /** <inheritDoc /> */
        public Task<bool> RemoveAsync(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTx();

            return DoOutOpAsync<TK, TV, bool>(CacheOp.RemoveBoolAsync, key, val);
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTx();

            DoOutOp(CacheOp.RemoveAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            StartTx();

            return DoOutOpAsync(CacheOp.RemoveAllAsync, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            StartTx();

            DoOutInOp((int) CacheOp.RemoveAll2);
        }

        /** <inheritDoc /> */
        public Task RemoveAllAsync()
        {
            StartTx();

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
            var modes0 = EncodePeekModes(modes);

            return DoOutOpAsync<int>(CacheOp.SizeAsync, w => w.WriteInt(modes0));
        }

        /// <summary>
        /// Internal size routine.
        /// </summary>
        /// <param name="loc">Local flag.</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private int Size0(bool loc, params CachePeekMode[] modes)
        {
            var modes0 = EncodePeekModes(modes);

            var op = loc ? CacheOp.SizeLoc : CacheOp.Size;

            return (int) DoOutInOp((int) op, modes0);
        }

        /** <inheritDoc /> */
        public void LocalPromote(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp(CacheOp.LocPromote, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public TRes Invoke<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTx();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutInOpX((int) CacheOp.Invoke,
                writer =>
                {
                    writer.Write(key);
                    writer.Write(holder);
                },
                (input, res) => res == True ? Unmarshal<TRes>(input) : default(TRes),
                ReadException);
        }

        /** <inheritDoc /> */
        public Task<TRes> InvokeAsync<TArg, TRes>(TK key, ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTx();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutOpAsync(CacheOp.InvokeAsync, writer =>
                {
                    writer.Write(key);
                    writer.Write(holder);
                },
                r =>
                {
                    if (r == null)
                        return default(TRes);

                    var hasError = r.ReadBoolean();

                    if (hasError)
                        throw ReadException(r);

                    return r.ReadObject<TRes>();
                });
        }

        /** <inheritdoc /> */
        public IDictionary<TK, ICacheEntryProcessorResult<TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTx();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutInOpX((int) CacheOp.InvokeAll,
                writer =>
                {
                    WriteEnumerable(writer, keys);
                    writer.Write(holder);
                },
                (input, res) => res == True ? ReadInvokeAllResults<TRes>(Marshaller.StartUnmarshal(input, IsKeepBinary)): null, ReadException);
        }

        /** <inheritDoc /> */
        public Task<IDictionary<TK, ICacheEntryProcessorResult<TRes>>> InvokeAllAsync<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");
            IgniteArgumentCheck.NotNull(processor, "processor");

            StartTx();

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutOpAsync(CacheOp.InvokeAllAsync,
                writer =>
                {
                    WriteEnumerable(writer, keys);
                    writer.Write(holder);
                },
                input => ReadInvokeAllResults<TRes>(input));

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
                    : default(T), ReadException);
        }

        /** <inheritdoc /> */
        public ICacheLock Lock(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpX((int) CacheOp.Lock, w => w.Write(key),
                (stream, res) => new CacheLock(stream.ReadInt(), this), ReadException);
        }

        /** <inheritdoc /> */
        public ICacheLock LockAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpX((int) CacheOp.LockAll, w => WriteEnumerable(w, keys),
                (stream, res) => new CacheLock(stream.ReadInt(), this), ReadException);
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
        public Task Rebalance()
        {
            return DoOutOpAsync(CacheOp.Rebalance);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            if (_flagNoRetries)
                return this;

            return new CacheImpl<TK, TV>(_ignite, DoOutOpObject((int) CacheOp.WithNoRetries), Marshaller,
                _flagSkipStore, _flagKeepBinary, true);
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

            var cursor = DoOutOpObject((int) CacheOp.QrySqlFields, writer =>
            {
                writer.WriteBoolean(qry.Local);
                writer.WriteString(qry.Sql);
                writer.WriteInt(qry.PageSize);

                QueryBase.WriteQueryArgs(writer, qry.Arguments);

                writer.WriteBoolean(qry.EnableDistributedJoins);
                writer.WriteBoolean(qry.EnforceJoinOrder);
            });
        
            return new FieldsQueryCursor<T>(cursor, Marshaller, _flagKeepBinary, readerFunc);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            var cursor = DoOutOpObject((int) qry.OpId, writer => qry.Write(writer, IsKeepBinary));

            return new QueryCursor<TK, TV>(cursor, Marshaller, _flagKeepBinary);
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

            return new ContinuousQueryHandleImpl<TK, TV>(qry, Marshaller, _flagKeepBinary,
                writeAction => DoOutOpObject((int) CacheOp.QryContinuous, writeAction), initialQry);
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
            {
                var target = DoOutOpObject((int) CacheOp.LocIterator, w => w.WriteInt(peekModes));

                return new CacheEnumerator<TK, TV>(target, Marshaller, _flagKeepBinary);
            }

            return new CacheEnumerator<TK, TV>(DoOutOpObject((int) CacheOp.Iterator), Marshaller, _flagKeepBinary);
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
        /// Reads results of InvokeAll operation.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="reader">Stream.</param>
        /// <returns>Results of InvokeAll operation.</returns>
        private IDictionary<TK, ICacheEntryProcessorResult<T>> ReadInvokeAllResults<T>(BinaryReader reader)
        {
            var count = reader.ReadInt();

            if (count == -1)
                return null;

            var results = new Dictionary<TK, ICacheEntryProcessorResult<T>>(count);

            for (var i = 0; i < count; i++)
            {
                var key = reader.ReadObject<TK>();

                var hasError = reader.ReadBoolean();

                results[key] = hasError
                    ? new CacheEntryProcessorResult<T>(ReadException(reader))
                    : new CacheEntryProcessorResult<T>(reader.ReadObject<T>());
            }

            return results;
        }

        /// <summary>
        /// Reads the exception.
        /// </summary>
        private Exception ReadException(IBinaryStream stream)
        {
            return ReadException(Marshaller.StartUnmarshal(stream));
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
        /// <returns>Dictionary.</returns>
        private static IDictionary<TK, TV> ReadGetAllDictionary(BinaryReader reader)
        {
            if (reader == null)
                return null;

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
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1>(CacheOp op, T1 x)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.Write(x);
            }, ReadException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1, T2>(CacheOp op, T1 x, T2 y)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.Write(x);
                w.Write(y);
            }, ReadException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp<T1, T2, T3>(CacheOp op, T1 x, T2 y, T3 z)
        {
            return DoOutInOpX((int) op, w =>
            {
                w.Write(x);
                w.Write(y);
                w.Write(z);
            }, ReadException);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private bool DoOutOp(CacheOp op, Action<BinaryWriter> write)
        {
            return DoOutInOpX((int) op, write, ReadException);
        }

        /// <summary>
        /// Does the out-in op.
        /// </summary>
        private CacheResult<TV> DoOutInOpNullable(CacheOp cacheOp, TK x)
        {
            return DoOutInOpX((int)cacheOp,
                w => w.Write(x),
                (stream, res) => res == True ? new CacheResult<TV>(Unmarshal<TV>(stream)) : new CacheResult<TV>(),
                ReadException);
        }

        /// <summary>
        /// Does the out-in op.
        /// </summary>
        private CacheResult<TV> DoOutInOpNullable<T1, T2>(CacheOp cacheOp, T1 x, T2 y)
        {
            return DoOutInOpX((int)cacheOp,
                w =>
                {
                    w.Write(x);
                    w.Write(y);
                },
                (stream, res) => res == True ? new CacheResult<TV>(Unmarshal<TV>(stream)) : new CacheResult<TV>(),
                ReadException);
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

        /// <summary>
        /// Starts a transaction when applicable.
        /// </summary>
        private void StartTx()
        {
            if (_txManager != null)
                _txManager.StartTx();
        }
    }
}
