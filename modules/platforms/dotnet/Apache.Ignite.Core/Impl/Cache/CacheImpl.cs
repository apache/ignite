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
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Transactions;

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

            _txManager = GetConfiguration().AtomicityMode == CacheAtomicityMode.Transactional
                ? new CacheTransactionManager(_ignite.GetIgnite().GetTransactions())
                : null;

            _readException = stream => ReadException(Marshaller.StartUnmarshal(stream));
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite.GetIgnite(); }
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
                BinaryUtils.Marshaller.StartUnmarshal(stream), ClientSocket.CurrentProtocolVersion));
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

            return new CacheImpl<TK, TV>(DoOutOpObject((int) CacheOp.WithSkipStore),
                true, _flagKeepBinary, true, _flagPartitionRecover);
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

            return new CacheImpl<TK1, TV1>(DoOutOpObject((int) CacheOp.WithKeepBinary),
                _flagSkipStore, true, _flagNoRetries, _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithAllowAtomicOpsInTx()
        {
            return this;
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithExpiryPolicy(IExpiryPolicy plc)
        {
            IgniteArgumentCheck.NotNull(plc, "plc");

            var cache0 = DoOutOpObject((int)CacheOp.WithExpiryPolicy, w => ExpiryPolicySerializer.WritePolicy(w, plc));

            return new CacheImpl<TK, TV>(cache0, _flagSkipStore, _flagKeepBinary, _flagNoRetries, _flagPartitionRecover);
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

            return DoOutOp(CacheOp.ContainsKeys, writer => writer.WriteEnumerable(keys));
        }

        /** <inheritDoc /> */
        public Task<bool> ContainsKeysAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync<bool>(CacheOp.ContainsKeysAsync, writer => writer.WriteEnumerable(keys));
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
                    w.WriteObjectDetached(key);
                    w.WriteInt(IgniteUtils.EncodePeekModes(modes));
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

            return DoOutInOpX((int) CacheOp.Get,
                w => w.Write(key),
                (stream, res) =>
                {
                    if (res != True)
                        throw GetKeyNotFoundException();

                    return Unmarshal<TV>(stream);
                }, _readException);
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
        public ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOpX((int) CacheOp.GetAll,
                writer => writer.WriteEnumerable(keys),
                (s, r) => r == True ? ReadGetAllDictionary(Marshaller.StartUnmarshal(s, _flagKeepBinary)) : null,
                _readException);
        }

        /** <inheritDoc /> */
        public Task<ICollection<ICacheEntry<TK, TV>>> GetAllAsync(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOpAsync(CacheOp.GetAllAsync, w => w.WriteEnumerable(keys), r => ReadGetAllDictionary(r));
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            StartTxIfNeeded();

            DoOutOp(CacheOp.Put, key, val);
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
            var modes0 = IgniteUtils.EncodePeekModes(modes);

            var op = loc ? CacheOp.SizeLoc : CacheOp.Size;

            return (int) DoOutInOp((int) op, modes0); 
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
            var modes0 = IgniteUtils.EncodePeekModes(modes);

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
            });  
        }
        
        /// <summary>
        /// Internal async integer size routine.
        /// </summary>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private Task<int> SizeAsync0(params CachePeekMode[] modes)
        {
            var modes0 = IgniteUtils.EncodePeekModes(modes);

            return DoOutOpAsync<int>(CacheOp.SizeAsync, w => w.WriteInt(modes0));
        }
        
        /// <summary>
        /// Internal async long size routine.
        /// </summary>
        /// <param name="part">Partition number</param>
        /// <param name="modes">peek modes</param>
        /// <returns>Size.</returns>
        private Task<long> SizeAsync0(int? part, params CachePeekMode[] modes)
        {
            var modes0 = IgniteUtils.EncodePeekModes(modes);

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
            });
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
        public Task Rebalance()
        {
            return DoOutOpAsync(CacheOp.Rebalance);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            if (_flagNoRetries)
                return this;

            return new CacheImpl<TK, TV>(DoOutOpObject((int) CacheOp.WithNoRetries),
                _flagSkipStore, _flagKeepBinary, true, _flagPartitionRecover);
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithPartitionRecover()
        {
            if (_flagPartitionRecover)
                return this;

            return new CacheImpl<TK, TV>(DoOutOpObject((int) CacheOp.WithPartitionRecover),
                _flagSkipStore, _flagKeepBinary, _flagNoRetries, true);
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

            return new FieldsQueryCursor(cursor, _flagKeepBinary, 
                (reader, count) => ReadFieldsArrayList(reader, count));
        }

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            return Query(qry, (reader, count) => (IList) ReadFieldsArrayList(reader, count));
        }

        /// <summary>
        /// Reads the fields array list.
        /// </summary>
        private static List<object> ReadFieldsArrayList(IBinaryRawReader reader, int count)
        {
            var res = new List<object>(count);

            for (var i = 0; i < count; i++)
                res.Add(reader.ReadObject<object>());

            return res;
        }

        /** <inheritDoc /> */
        public IQueryCursor<T> Query<T>(SqlFieldsQuery qry, Func<IBinaryRawReader, int, T> readerFunc)
        {
            var cursor = QueryFieldsInternal(qry);

            return new FieldsQueryCursor<T>(cursor, _flagKeepBinary, readerFunc);
        }

        private IPlatformTargetInternal QueryFieldsInternal(SqlFieldsQuery qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            if (string.IsNullOrEmpty(qry.Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            return DoOutOpObject((int) CacheOp.QrySqlFields, writer =>
            {
                writer.WriteBoolean(qry.Local);
                writer.WriteString(qry.Sql);
                writer.WriteInt(qry.PageSize);

                QueryBase.WriteQueryArgs(writer, qry.Arguments);

                writer.WriteBoolean(qry.EnableDistributedJoins);
                writer.WriteBoolean(qry.EnforceJoinOrder);
                writer.WriteBoolean(qry.Lazy); // Lazy flag.
                writer.WriteInt((int) qry.Timeout.TotalMilliseconds);
#pragma warning disable 618
                writer.WriteBoolean(qry.ReplicatedOnly);
#pragma warning restore 618
                writer.WriteBoolean(qry.Colocated);
                writer.WriteString(qry.Schema); // Schema
            });
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

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
            return new CacheEnumerable<TK, TV>(this, IgniteUtils.EncodePeekModes(peekModes));
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
        /// <returns>Dictionary.</returns>
        private static ICollection<ICacheEntry<TK, TV>> ReadGetAllDictionary(BinaryReader reader)
        {
            if (reader == null)
                return null;

            IBinaryStream stream = reader.Stream;

            if (stream.ReadBool())
            {
                int size = stream.ReadInt();

                var res = new List<ICacheEntry<TK, TV>>(size);

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
    }
}
