﻿﻿/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
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
    using U = GridUtils;
    using AC = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native cache wrapper.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheImpl<K, V> : GridTarget, ICache<K, V>
    {
        /** Duration: unchanged. */
        private const long DUR_UNCHANGED = -2;

        /** Duration: eternal. */
        private const long DUR_ETERNAL = -1;

        /** Duration: zero. */
        private const long DUR_ZERO = 0;

        /** Grid instance. */
        private readonly GridImpl grid;
        
        /** Flag: skip store. */
        private readonly bool flagSkipStore;

        /** Flag: keep portable. */
        private readonly bool flagKeepPortable;

        /** Flag: async mode.*/
        private readonly bool flagAsync;

        /** Flag: no-retries.*/
        private readonly bool flagNoRetries;

        /** 
         * Result converter for async InvokeAll operation. 
         * In future result processing there is only one TResult generic argument, 
         * and we can't get the type of ICacheEntryProcessorResult at compile time from it.
         * This field caches converter for the last InvokeAll operation to avoid using reflection.
         */
        private readonly ThreadLocal<object> invokeAllConverter = new ThreadLocal<object>();

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
        public CacheImpl(GridImpl grid, IUnmanagedTarget target, PortableMarshaller marsh,
            bool flagSkipStore, bool flagKeepPortable, bool flagAsync, bool flagNoRetries) : base(target, marsh)
        {
            this.grid = grid;
            this.flagSkipStore = flagSkipStore;
            this.flagKeepPortable = flagKeepPortable;
            this.flagAsync = flagAsync;
            this.flagNoRetries = flagNoRetries;
        }

        /** <inheritDoc /> */
        public IIgnite Grid
        {
            get
            {
                return grid;
            }
        }

        /** <inheritDoc /> */
        public bool IsAsync
        {
            get { return flagAsync; }
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
        /// <param name="lastAsyncOpId">The last async op id.</param>
        /// <returns>
        /// Future for previous asynchronous operation.
        /// </returns>
        /// <exception cref="System.InvalidOperationException">Asynchronous mode is disabled</exception>
        internal IFuture<TResult> GetFuture<TResult>(int lastAsyncOpId)
        {
            if (!flagAsync)
                throw U.GetAsyncModeDisabledException();

            var converter = GetFutureResultConverter<TResult>(lastAsyncOpId);

            invokeAllConverter.Value = null;

            return GetFuture((futId, futTypeId) => UU.TargetListenFutureForOperation(target, futId, futTypeId, lastAsyncOpId), 
                flagKeepPortable, converter);
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return DoInOp<string>((int)CacheOp.GET_NAME); }
        }

        /** <inheritDoc /> */
        public bool IsEmpty
        {
            get { return Size() == 0; }
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithSkipStore()
        {
            if (flagSkipStore)
                return this;

            return new CacheImpl<K, V>(grid, UU.CacheWithSkipStore(target), Marshaller, 
                true, flagKeepPortable, flagAsync, true);
        }

        /// <summary>
        /// Skip store flag getter.
        /// </summary>
        internal bool IsSkipStore { get { return flagSkipStore; } }

        /** <inheritDoc /> */
        public ICache<K1, V1> WithKeepPortable<K1, V1>()
        {
            if (flagKeepPortable)
            {
                var result = this as ICache<K1, V1>;

                if (result == null)
                    throw new InvalidOperationException(
                        "Can't change type of portable cache. WithKeepPortable has been called on an instance of " +
                        "portable cache with incompatible generic arguments.");

                return result;
            }

            return new CacheImpl<K1, V1>(grid, UU.CacheWithKeepPortable(target), marsh, 
                flagSkipStore, true, flagAsync, flagNoRetries);
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithExpiryPolicy(IExpiryPolicy plc)
        {
            AC.NotNull(plc, "plc");

            long create = ConvertDuration(plc.GetExpiryForCreate());
            long update = ConvertDuration(plc.GetExpiryForUpdate());
            long access = ConvertDuration(plc.GetExpiryForAccess());

            IUnmanagedTarget cache0 = UU.CacheWithExpiryPolicy(target, create, update, access);

            return new CacheImpl<K, V>(grid, cache0, Marshaller, flagSkipStore, flagKeepPortable, flagAsync, flagNoRetries);
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
                    return DUR_ETERNAL;

                long dur0 = (long)dur.Value.TotalMilliseconds;

                return dur0 > 0 ? dur0 : DUR_ZERO;
            }
            
            return DUR_UNCHANGED;
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithAsync()
        {
            return flagAsync ? this : new CacheImpl<K, V>(grid, UU.CacheWithAsync(target), Marshaller,
                flagSkipStore, flagKeepPortable, true, flagNoRetries);
        }

        /** <inheritDoc /> */
        public bool KeepPortable
        {
            get { return flagKeepPortable; }
        }

        /** <inheritDoc /> */
        public void LoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LOAD_CACHE);
        }

        /** <inheritDoc /> */
        public void LocalLoadCache(ICacheEntryFilter<K, V> p, params object[] args)
        {
            LoadCache0(p, args, (int)CacheOp.LOC_LOAD_CACHE);
        }

        /// <summary>
        /// Loads the cache.
        /// </summary>
        private void LoadCache0(ICacheEntryFilter<K, V> p, object[] args, int opId)
        {
            DoOutOp(opId, writer =>
            {
                if (p != null)
                {
                    var p0 = new CacheEntryFilterHolder(p, (k, v) => p.Invoke(new CacheEntry<K, V>((K)k, (V)v)),
                        Marshaller, KeepPortable);
                    writer.WriteObject(p0);
                    writer.WriteLong(p0.Handle);
                }
                else
                    writer.WriteObject<CacheEntryFilterHolder>(null);

                writer.WriteObjectArray(args);
            });
        }

        /** <inheritDoc /> */
        public bool ContainsKey(K key)
        {
            AC.NotNull(key, "key");

            return DoOutOp((int)CacheOp.CONTAINS_KEY, key) == TRUE;
        }        

        /** <inheritDoc /> */
        public bool ContainsKeys(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            return DoOutOp((int)CacheOp.CONTAINS_KEYS, writer => WriteEnumerable(writer, keys)) == TRUE;
        }        

        /** <inheritDoc /> */
        public V LocalPeek(K key, params CachePeekMode[] modes)
        {
            AC.NotNull(key, "key");

            return DoOutInOp<V>((int)CacheOp.PEEK, writer =>
            {
                writer.Write(key);
                writer.WriteInt(EncodePeekModes(modes));
            });
        }

        /** <inheritDoc /> */
        public V Get(K key)
        {
            AC.NotNull(key, "key");

            return DoOutInOp<K, V>((int)CacheOp.GET, key);
        }

        /** <inheritDoc /> */
        public IDictionary<K, V> GetAll(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            return DoOutInOp((int)CacheOp.GET_ALL,
                writer => WriteEnumerable(writer, keys),
                input =>
                {
                    var reader = marsh.StartUnmarshal(input, flagKeepPortable);

                    return ReadGetAllDictionary(reader);
                });
        }

        /** <inheritdoc /> */
        public void Put(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            DoOutOp((int)CacheOp.PUT, key, val);
        }

        /** <inheritDoc /> */
        public V GetAndPut(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutInOp<K, V, V>((int)CacheOp.GET_AND_PUT, key, val);
        }

        /** <inheritDoc /> */
        public V GetAndReplace(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutInOp<K, V, V>((int)CacheOp.GET_AND_REPLACE, key, val);
        }

        /** <inheritDoc /> */
        public V GetAndRemove(K key)
        {
            AC.NotNull(key, "key");

            return DoOutInOp<K, V>((int)CacheOp.GET_AND_REMOVE, key);
        }

        /** <inheritdoc /> */
        public bool PutIfAbsent(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutOp((int) CacheOp.PUT_IF_ABSENT, key, val) == TRUE;
        }

        /** <inheritdoc /> */
        public V GetAndPutIfAbsent(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutInOp<K, V, V>((int)CacheOp.GET_AND_PUT_IF_ABSENT, key, val);
        }

        /** <inheritdoc /> */
        public bool Replace(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutOp((int)CacheOp.REPLACE_2, key, val) == TRUE;
        }

        /** <inheritdoc /> */
        public bool Replace(K key, V oldVal, V newVal)
        {
            AC.NotNull(key, "key");

            AC.NotNull(oldVal, "oldVal");

            AC.NotNull(newVal, "newVal");

            return DoOutOp((int)CacheOp.REPLACE_3, key, oldVal, newVal) == TRUE;
        }

        /** <inheritdoc /> */
        public void PutAll(IDictionary<K, V> vals)
        {
            AC.NotNull(vals, "vals");

            DoOutOp((int) CacheOp.PUT_ALL, writer => WriteDictionary(writer, vals));
        }
        
        /** <inheritdoc /> */
        public void LocalEvict(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            DoOutOp((int) CacheOp.LOC_EVICT, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            UU.CacheClear(target);
        }

        /** <inheritdoc /> */
        public void Clear(K key)
        {
            AC.NotNull(key, "key");

            DoOutOp((int)CacheOp.CLEAR, key);
        }

        /** <inheritdoc /> */
        public void ClearAll(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.CLEAR_ALL, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public void LocalClear(K key)
        {
            AC.NotNull(key, "key");

            DoOutOp((int) CacheOp.LOCAL_CLEAR, key);
        }

        /** <inheritdoc /> */
        public void LocalClearAll(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.LOCAL_CLEAR_ALL, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public bool Remove(K key)
        {
            AC.NotNull(key, "key");

            return DoOutOp((int)CacheOp.REMOVE_OBJ, key) == TRUE;
        }

        /** <inheritDoc /> */
        public bool Remove(K key, V val)
        {
            AC.NotNull(key, "key");

            AC.NotNull(val, "val");

            return DoOutOp((int)CacheOp.REMOVE_BOOL, key, val) == TRUE;
        }

        /** <inheritDoc /> */
        public void RemoveAll(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.REMOVE_ALL, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritDoc /> */
        public void RemoveAll()
        {
            UU.CacheRemoveAll(target);
        }

        /** <inheritDoc /> */
        public int LocalSize(params CachePeekMode[] modes)
        {
            return Size0(true, modes);
        }

        /** <inheritDoc /> */
        public int Size(params CachePeekMode[] modes)
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

            return UU.CacheSize(target, modes0, loc);
        }

        /** <inheritDoc /> */
        public void LocalPromote(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            DoOutOp((int)CacheOp.LOC_PROMOTE, writer => WriteEnumerable(writer, keys));
        }

        /** <inheritdoc /> */
        public R Invoke<R, A>(K key, ICacheEntryProcessor<K, V, A, R> processor, A arg)
        {
            AC.NotNull(key, "key");

            AC.NotNull(processor, "processor");

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<K, V>)e, (A)a), typeof(K), typeof(V));

            return DoOutInOp((int)CacheOp.INVOKE, writer =>
            {
                writer.Write(key);
                writer.Write(holder);
            },
            input => GetResultOrThrow<R>(Unmarshal<object>(input)));
        }

        /** <inheritdoc /> */
        public IDictionary<K, ICacheEntryProcessorResult<R>> InvokeAll<R, A>(IEnumerable<K> keys,
            ICacheEntryProcessor<K, V, A, R> processor, A arg)
        {
            AC.NotNull(keys, "keys");

            AC.NotNull(processor, "processor");

            var holder = new CacheEntryProcessorHolder(processor, arg,
                (e, a) => processor.Process((IMutableCacheEntry<K, V>)e, (A)a), typeof(K), typeof(V));

            return DoOutInOp((int)CacheOp.INVOKE_ALL, writer =>
            {
                WriteEnumerable(writer, keys);
                writer.Write(holder);
            },
            input =>
            {
                if (IsAsync)
                    invokeAllConverter.Value = (Func<PortableReaderImpl, IDictionary<K, ICacheEntryProcessorResult<R>>>)
                        (reader => ReadInvokeAllResults<R>(reader.Stream));

                return ReadInvokeAllResults<R>(input);
            });
        }

        /** <inheritdoc /> */
        public ICacheLock Lock(K key)
        {
            AC.NotNull(key, "key");

            return DoOutInOp((int)CacheOp.LOCK, writer =>
            {
                writer.Write(key);
            }, input => new CacheLock(input.ReadInt(), target));
        }

        /** <inheritdoc /> */
        public ICacheLock LockAll(IEnumerable<K> keys)
        {
            AC.NotNull(keys, "keys");

            return DoOutInOp((int)CacheOp.LOCK_ALL, writer =>
            {
                WriteEnumerable(writer, keys);
            }, input => new CacheLock(input.ReadInt(), target));
        }

        /** <inheritdoc /> */
        public bool IsLocalLocked(K key, bool byCurrentThread)
        {
            AC.NotNull(key, "key");

            return DoOutOp((int)CacheOp.IS_LOCAL_LOCKED, writer =>
            {
                writer.Write(key);
                writer.WriteBoolean(byCurrentThread);
            }) == TRUE;
        }

        /** <inheritDoc /> */
        public ICacheMetrics GetMetrics()
        {
            return DoInOp((int)CacheOp.METRICS, stream =>
            {
                IPortableRawReader reader = marsh.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public IFuture Rebalance()
        {
            return GetFuture<object>((futId, futTyp) => UU.CacheRebalance(target, futId));
        }

        /** <inheritDoc /> */
        public ICache<K, V> WithNoRetries()
        {
            if (flagNoRetries)
                return this;

            return new CacheImpl<K, V>(grid, UU.CacheWithNoRetries(target), Marshaller,
                flagSkipStore, flagKeepPortable, flagAsync, true);
        }

        /// <summary>
        /// Gets a value indicating whether this instance is in no-retries mode.
        /// </summary>
        internal bool IsNoRetries
        {
            get { return flagNoRetries; }
        }

        #region Queries

        /** <inheritDoc /> */
        public IQueryCursor<IList> QueryFields(SqlFieldsQuery qry)
        {
            AC.NotNull(qry, "qry");

            if (string.IsNullOrEmpty(qry.Sql))
                throw new ArgumentException("Sql cannot be null or empty");

            IUnmanagedTarget cursor;

            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                var writer = marsh.StartMarshal(stream);

                writer.WriteBoolean(qry.Local);
                writer.WriteString(qry.Sql);
                writer.WriteInt(qry.PageSize);

                WriteQueryArgs(writer, qry.Arguments);

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(target, (int) CacheOp.QRY_SQL_FIELDS, stream.SynchronizeOutput());
            }
        
            return new FieldsQueryCursor(cursor, marsh, flagKeepPortable);
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<K, V>> Query(QueryBase qry)
        {
            AC.NotNull(qry, "qry");

            IUnmanagedTarget cursor;

            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                var writer = marsh.StartMarshal(stream);

                qry.Write(writer, KeepPortable);

                FinishMarshal(writer);

                cursor = UU.CacheOutOpQueryCursor(target, (int)qry.OpId, stream.SynchronizeOutput()); 
            }

            return new QueryCursor<K, V>(cursor, marsh, flagKeepPortable);
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
        public IContinuousQueryHandle QueryContinuous(ContinuousQuery<K, V> qry)
        {
            AC.NotNull(qry, "qry");

            return QueryContinuousImpl(qry, null);
        }

        /** <inheritdoc /> */
        public IContinuousQueryHandle<ICacheEntry<K, V>> QueryContinuous(ContinuousQuery<K, V> qry, QueryBase initialQry)
        {
            AC.NotNull(qry, "qry");
            AC.NotNull(initialQry, "initialQry");

            return QueryContinuousImpl(qry, initialQry);
        }

        /// <summary>
        /// QueryContinuous implementation.
        /// </summary>
        private IContinuousQueryHandle<ICacheEntry<K, V>> QueryContinuousImpl(ContinuousQuery<K, V> qry, 
            QueryBase initialQry)
        {
            qry.Validate();

            var hnd = new ContinuousQueryHandleImpl<K, V>(qry, marsh, flagKeepPortable);

            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                var writer = marsh.StartMarshal(stream);

                hnd.Start(grid, writer, () =>
                {
                    if (initialQry != null)
                    {
                        writer.WriteInt((int) initialQry.OpId);

                        initialQry.Write(writer, KeepPortable);
                    }
                    else
                        writer.WriteInt(-1); // no initial query

                    FinishMarshal(writer);

                    // ReSharper disable once AccessToDisposedClosure
                    return UU.CacheOutOpContinuousQuery(target, (int)CacheOp.QRY_CONTINUOUS, stream.SynchronizeOutput());
                }, qry);
            }

            return hnd;
        }

        #endregion

        #region Enumerable support

        /** <inheritdoc /> */
        public IEnumerable<ICacheEntry<K, V>> GetLocalEntries(CachePeekMode[] peekModes)
        {
            return new CacheEnumerable<K, V>(this, EncodePeekModes(peekModes));
        }

        /** <inheritdoc /> */
        public IEnumerator<ICacheEntry<K, V>> GetEnumerator()
        {
            return new CacheEnumeratorProxy<K, V>(this, false, 0);
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
        internal CacheEnumerator<K, V> CreateEnumerator(bool loc, int peekModes)
        {
            if (loc)
                return new CacheEnumerator<K, V>(UU.CacheLocalIterator(target, peekModes), marsh, flagKeepPortable);

            return new CacheEnumerator<K, V>(UU.CacheIterator(target), marsh, flagKeepPortable);
        }

        #endregion

        /** <inheritDoc /> */
        protected override T Unmarshal<T>(IPortableStream stream)
        {
            return marsh.Unmarshal<T>(stream, flagKeepPortable);
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
        private IDictionary<K, ICacheEntryProcessorResult<T>> ReadInvokeAllResults<T>(IPortableStream inStream)
        {
            var count = inStream.ReadInt();

            if (count == -1)
                return null;

            var results = new Dictionary<K, ICacheEntryProcessorResult<T>>(count);

            for (var i = 0; i < count; i++)
            {
                var key = Unmarshal<K>(inStream);

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
        private static IDictionary<K, V> ReadGetAllDictionary(PortableReaderImpl reader)
        {
            IPortableStream stream = reader.Stream;

            if (stream.ReadBool())
            {
                int size = stream.ReadInt();

                IDictionary<K, V> res = new Dictionary<K, V>(size);

                for (int i = 0; i < size; i++)
                {
                    K key = reader.ReadObject<K>();
                    V val = reader.ReadObject<V>();

                    res[key] = val;
                }

                return res;
            }
            else
                return null;
        }

        /// <summary>
        /// Gets the future result converter based on the last operation id.
        /// </summary>
        /// <typeparam name="TResult">The type of the future result.</typeparam>
        /// <param name="lastAsyncOpId">The last op id.</param>
        /// <returns>Future result converter.</returns>
        private Func<PortableReaderImpl, TResult> GetFutureResultConverter<TResult>(int lastAsyncOpId)
        {
            if (lastAsyncOpId == (int) CacheOp.GET_ALL)
                return reader => (TResult)ReadGetAllDictionary(reader);
            
            if (lastAsyncOpId == (int)CacheOp.INVOKE)
                return reader => { throw ReadException(reader.Stream); };

            if (lastAsyncOpId == (int) CacheOp.INVOKE_ALL)
                return invokeAllConverter.Value as Func<PortableReaderImpl, TResult>;

            return null;
        }
    }
}
