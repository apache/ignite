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
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cache.Query;
    using Apache.Ignite.Core.Impl.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native cache wrapper.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheImpl<TK, TV> : PlatformTarget, ICache<TK, TV>
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

        /** Flag: keep portable. */
        private readonly bool _flagKeepPortable;

        /** Flag: async mode.*/
        private readonly bool _flagAsync;

        /** Flag: no-retries.*/
        private readonly bool _flagNoRetries;

        /** 
         * Result converter for async InvokeAll operation. 
         * In future result processing there is only one TResult generic argument, 
         * and we can't get the type of ICacheEntryProcessorResult at compile time from it.
         * This field caches converter for the last InvokeAll operation to avoid using reflection.
         */
        private readonly ThreadLocal<object> _invokeAllConverter = new ThreadLocal<object>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="flagSkipStore">Skip store flag.</param>
        /// <param name="flagKeepPortable">Keep portable flag.</param>
        /// <param name="flagAsync">Async mode flag.</param>
        /// <param name="flagNoRetries">No-retries mode flag.</param>
        public CacheImpl(Ignite grid, IUnmanagedTarget target, PortableMarshaller marsh,
            bool flagSkipStore, bool flagKeepPortable, bool flagAsync, bool flagNoRetries) : base(target, marsh)
        {
            _ignite = grid;
            _flagSkipStore = flagSkipStore;
            _flagKeepPortable = flagKeepPortable;
            _flagAsync = flagAsync;
            _flagNoRetries = flagNoRetries;
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get
            {
                return _ignite;
            }
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return _flagAsync; }
        }

        /** <inheritDoc /> */
        public IFuture GetFuture()
        {
            throw new NotSupportedException("GetFuture() should be called through CacheProxyImpl");
        }

        /** <inheritDoc /> */
        public IFuture<TResult> GetFuture<TResult>()
        {
            throw new NotSupportedException("GetFuture() should be called through CacheProxyImpl");
        }

        /// <summary>
        /// Gets and resets future for previous asynchronous operation.
        /// </summary>
        /// <param name="lastAsyncOp">The last async op id.</param>
        /// <returns>
        /// Future for previous asynchronous operation.
        /// </returns>
        /// <exception cref="System.InvalidOperationException">Asynchronous mode is disabled</exception>
        internal IFuture<TResult> GetFuture<TResult>(CacheOp lastAsyncOp) 
        {
            if (!_flagAsync)
                throw IgniteUtils.GetAsyncModeDisabledException();

            var converter = GetFutureResultConverter<TResult>(lastAsyncOp);

            _invokeAllConverter.Value = null;

            return GetFuture((futId, futTypeId) => UU.TargetListenFutureForOperation(Target, futId, futTypeId, 
                (int) lastAsyncOp), _flagKeepPortable, converter);
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return DoInOp<string>((int)CacheOp.GetName); }
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
                true, _flagKeepPortable, _flagAsync, true);
        }

        /// <summary>
        /// Skip store flag getter.
        /// </summary>
        internal bool IsSkipStore { get { return _flagSkipStore; } }

        /** <inheritDoc /> */
        public ICache<TK1, TV1> WithKeepPortable<TK1, TV1>()
        {
            if (_flagKeepPortable)
            {
                var result = this as ICache<TK1, TV1>;

                if (result == null)
                    throw new InvalidOperationException(
                        "Can't change type of portable cache. WithKeepPortable has been called on an instance of " +
                        "portable cache with incompatible generic arguments.");

                return result;
            }

            return new CacheImpl<TK1, TV1>(_ignite, UU.CacheWithKeepPortable(Target), Marshaller, 
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

            return new CacheImpl<TK, TV>(_ignite, cache0, Marshaller, _flagSkipStore, _flagKeepPortable, _flagAsync, _flagNoRetries);
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
        public ICache<TK, TV> WithAsync()
        {
            return _flagAsync ? this : new CacheImpl<TK, TV>(_ignite, UU.CacheWithAsync(Target), Marshaller,
                _flagSkipStore, _flagKeepPortable, true, _flagNoRetries);
        }

        /** <inheritDoc /> */
        public bool IsKeepPortable
        {
            get { return _flagKeepPortable; }
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LoadCache);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<TK, TV> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LocLoadCache);
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
                    var p0 = new CacheEntryFilterHolder(p, (k, v) => p.Invoke(new CacheEntry<TK, TV>((TK)k, (TV)v)),
                        Marshaller, IsKeepPortable);
                    writer.WriteObject(p0);
                    writer.WriteLong(p0.Handle);
                }
                else
                    writer.WriteObject<CacheEntryFilterHolder>(null);

                writer.WriteArray(args);
            });
        }

        /** <inheritDoc /> */
        public bool ContainsKey(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutOp((int)CacheOp.ContainsKey, key) == True;
        }        

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutOp((int)CacheOp.ContainsKeys, writer => WriteEnumerable(writer, keys)) == True;
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
        public IDictionary<TK, TV> GetAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            return DoOutInOp((int)CacheOp.GetAll,
                writer => WriteEnumerable(writer, keys),
                input =>
                {
                    var reader = Marshaller.StartUnmarshal(input, _flagKeepPortable);

                    return ReadGetAllDictionary(reader);
                });
        }

        /** <inheritdoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            DoOutOp((int)CacheOp.Put, key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndPut(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int)CacheOp.GetAndPut, key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndReplace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int)CacheOp.GetAndReplace, key, val);
        }

        /** <inheritDoc /> */
        public CacheResult<TV> GetAndRemove(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOpNullable<TK, TV>((int)CacheOp.GetAndRemove, key);
        }

        /** <inheritdoc /> */
        public bool PutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int) CacheOp.PutIfAbsent, key, val) == True;
        }

        /** <inheritdoc /> */
        public CacheResult<TV> GetAndPutIfAbsent(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutInOpNullable<TK, TV, TV>((int)CacheOp.GetAndPutIfAbsent, key, val);
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int)CacheOp.Replace2, key, val) == True;
        }

        /** <inheritdoc /> */
        public bool Replace(TK key, TV oldVal, TV newVal)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(oldVal, "oldVal");

            IgniteArgumentCheck.NotNull(newVal, "newVal");

            return DoOutOp((int)CacheOp.Replace3, key, oldVal, newVal) == True;
        }

        /** <inheritdoc /> */
        public void PutAll(IDictionary<TK, TV> vals)
        {
            IgniteArgumentCheck.NotNull(vals, "vals");

            DoOutOp((int) CacheOp.PutAll, writer => WriteDictionary(writer, vals));
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

        /** <inheritdoc /> */
        public void Clear(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            DoOutOp((int)CacheOp.Clear, key);
        }

        /** <inheritdoc /> */
        public void ClearAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.ClearAll, writer => WriteEnumerable(writer, keys));
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

            return DoOutOp((int)CacheOp.RemoveObj, key) == True;
        }

        /** <inheritDoc /> */
        public bool Remove(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            IgniteArgumentCheck.NotNull(val, "val");

            return DoOutOp((int)CacheOp.RemoveBool, key, val) == True;
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<TK> keys)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.RemoveAll, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            UU.CacheRemoveAll(Target);
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

        /** <inheritdoc /> */
        public IDictionary<TK, ICacheEntryProcessorResult<TRes>> InvokeAll<TArg, TRes>(IEnumerable<TK> keys,
            ICacheEntryProcessor<TK, TV, TArg, TRes> processor, TArg arg)
        {
            IgniteArgumentCheck.NotNull(keys, "keys");

            IgniteArgumentCheck.NotNull(processor, "processor");

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<TK, TV>)e, (TArg)a), typeof(TK), typeof(TV));

            return DoOutInOp((int)CacheOp.InvokeAll, writer =>
            {
                WriteEnumerable(writer, keys);
                writer.Write(holder);
            },
            input =>
            {
                if (IsAsync)
                    _invokeAllConverter.Value = (Func<PortableReaderImpl, IDictionary<TK, ICacheEntryProcessorResult<TRes>>>)
                        (reader => ReadInvokeAllResults<TRes>(reader.Stream));

                return ReadInvokeAllResults<TRes>(input);
            });
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
                IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public IFuture Rebalance()
        {
            return GetFuture<object>((futId, futTyp) => UU.CacheRebalance(Target, futId));
        }

        /** <inheritDoc /> */
        public ICache<TK, TV> WithNoRetries()
        {
            if (_flagNoRetries)
                return this;

            return new CacheImpl<TK, TV>(_ignite, UU.CacheWithNoRetries(Target), Marshaller,
                _flagSkipStore, _flagKeepPortable, _flagAsync, true);
        }

        /// <summary>
        /// Gets a value indicating whether this instance is in no-retries mode.
        /// </summary>
        internal bool IsNoRetries
        {
            get { return _flagNoRetries; }
        }

        #region Queries

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

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

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(Target, (int) CacheOp.QrySqlFields, stream.SynchronizeOutput());
            }
        
            return new FieldsQueryCursor(cursor, Marshaller, _flagKeepPortable);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(QueryBase qry)
        {
            IgniteArgumentCheck.NotNull(qry, "qry");

            IUnmanagedTarget cursor;

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = Marshaller.StartMarshal(stream);

                qry.Write(writer, IsKeepPortable);

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(Target, (int)qry.OpId, stream.SynchronizeOutput()); 
            }

            return new QueryCursor<TK, TV>(cursor, Marshaller, _flagKeepPortable);
        }
                
        /// <summary>
        /// Write query arguments.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="args">Arguments.</param>
        private static void WriteQueryArgs(PortableWriterImpl writer, object[] args)
        {
            if (args == null)
                writer.WriteInt(0);
            else
            {
                writer.WriteInt(args.Length);
        
                foreach (var arg in args)
                    writer.WriteObject(arg);
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

            var hnd = new ContinuousQueryHandleImpl<TK, TV>(qry, Marshaller, _flagKeepPortable);

            using (var stream = IgniteManager.Memory.Allocate().GetStream())
            {
                var writer = Marshaller.StartMarshal(stream);

                hnd.Start(_ignite, writer, () =>
                {
                    if (initialQry != null)
                    {
                        writer.WriteInt((int) initialQry.OpId);

                        initialQry.Write(writer, IsKeepPortable);
                    }
                    else
                        writer.WriteInt(-1); // no initial query

                    FinishMarshal(writer);

                    // ReSharper disable once AccessToDisposedClosure
                    return UU.CacheOutOpContinuousQuery(Target, (int)CacheOp.QryContinuous, stream.SynchronizeOutput());
                }, qry);
            }

            return hnd;
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
                return new CacheEnumerator<TK, TV>(UU.CacheLocalIterator(Target, peekModes), Marshaller, _flagKeepPortable);

            return new CacheEnumerator<TK, TV>(UU.CacheIterator(Target), Marshaller, _flagKeepPortable);
        }

        #endregion

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IPortableStream stream)
        {
            return Marshaller.Unmarshal<T>(stream, _flagKeepPortable);
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
        /// Unwraps an exception from PortableResultHolder, if any. Otherwise does the cast.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="obj">Object.</param>
        /// <returns>Result.</returns>
        private static T GetResultOrThrow<T>(object obj)
        {
            var holder = obj as PortableResultWrapper;

            if (holder != null)
            {
                var err = holder.Result as Exception;

                if (err != null)
                    throw err as CacheEntryProcessorException ?? new CacheEntryProcessorException(err);
            }

            return obj == null ? default(T) : (T) obj;
        }

        /// <summary>
        /// Reads results of InvokeAll operation.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="inStream">Stream.</param>
        /// <returns>Results of InvokeAll operation.</returns>
        private IDictionary<TK, ICacheEntryProcessorResult<T>> ReadInvokeAllResults<T>(IPortableStream inStream)
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
        /// Reads the exception, either in portable wrapper form, or as a pair of strings.
        /// </summary>
        /// <param name="inStream">The stream.</param>
        /// <returns>Exception.</returns>
        private CacheEntryProcessorException ReadException(IPortableStream inStream)
        {
            var item = Unmarshal<object>(inStream);

            var clsName = item as string;

            if (clsName == null)
                return new CacheEntryProcessorException((Exception) ((PortableResultWrapper) item).Result);

            var msg = Unmarshal<string>(inStream);
                
            return new CacheEntryProcessorException(ExceptionUtils.GetException(clsName, msg));
        }

        /// <summary>
        /// Read dictionary returned by GET_ALL operation.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Dictionary.</returns>
        private static IDictionary<TK, TV> ReadGetAllDictionary(PortableReaderImpl reader)
        {
            IPortableStream stream = reader.Stream;

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
        /// Gets the future result converter based on the last operation id.
        /// </summary>
        /// <typeparam name="TResult">The type of the future result.</typeparam>
        /// <param name="lastAsyncOpId">The last op id.</param>
        /// <returns>Future result converter.</returns>
        private Func<PortableReaderImpl, TResult> GetFutureResultConverter<TResult>(CacheOp lastAsyncOpId)
        {
            switch (lastAsyncOpId)
            {
                case CacheOp.Get:
                    return reader =>
                    {
                        if (reader != null)
                            return reader.ReadObject<TResult>();

                        throw GetKeyNotFoundException();
                    };

                case CacheOp.GetAll:
                    return reader => reader == null ? default(TResult) : (TResult) ReadGetAllDictionary(reader);

                case CacheOp.Invoke:
                    return reader =>
                    {
                        if (reader == null)
                            return default(TResult);

                        var hasError = reader.ReadBoolean();

                        if (hasError)
                            throw ReadException(reader.Stream);

                        return reader.ReadObject<TResult>();
                    };

                case CacheOp.InvokeAll:
                    return _invokeAllConverter.Value as Func<PortableReaderImpl, TResult>;

                case CacheOp.GetAndPut:
                case CacheOp.GetAndPutIfAbsent:
                case CacheOp.GetAndRemove:
                case CacheOp.GetAndReplace:
                    return reader =>
                    {
                        var res = reader == null
                            ? new CacheResult<TV>()
                            : new CacheResult<TV>(reader.ReadObject<TV>());

                        return TypeCaster<TResult>.Cast(res);
                    };
            }

            return null;
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
        private CacheResult<TR> DoOutInOpNullable<TR>(int type, Action<PortableWriterImpl> outAction)
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
