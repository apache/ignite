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

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;
    using CQU = ContinuousQueryUtils;

    /// <summary>
    /// Continuous query handle interface.
    /// </summary>
    internal interface IContinuousQueryHandleImpl : IDisposable
    {
        /// <summary>
        /// Process callback.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        void Apply(IBinaryStream stream);
    }

    /// <summary>
    /// Continuous query handle.
    /// </summary>
    internal class ContinuousQueryHandleImpl<TK, TV> : IContinuousQueryHandleImpl, IContinuousQueryFilter,
        IContinuousQueryHandle<ICacheEntry<TK, TV>>, IContinuousQueryHandleFields
    {
        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Real listener. */
        private readonly ICacheEntryEventListener<TK, TV> _lsnr;

        /** Real filter. */
        private readonly ICacheEntryEventFilter<TK, TV> _filter;

        /** GC handle. */
        private readonly long _hnd;

        /** Native query. */
        private readonly IPlatformTargetInternal _nativeQry;

        /** Indicates whether initial query is a Fields query. */
        private readonly bool _initialQueryIsFields;

        /** Initial query cursor. */
        private volatile IPlatformTargetInternal _nativeInitialQueryCursor;

        /** Disposed flag. */
        private bool _disposed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="qry">Query.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="createTargetCb">The initialization callback.</param>
        /// <param name="initialQry">The initial query.</param>
        public ContinuousQueryHandleImpl(ContinuousQuery<TK, TV> qry, Marshaller marsh, bool keepBinary,
            Func<Action<BinaryWriter>, IPlatformTargetInternal> createTargetCb, IQueryBaseInternal initialQry)
        {
            _marsh = marsh;
            _keepBinary = keepBinary;

            _lsnr = qry.Listener;
            _filter = qry.Filter;

            // 1. Inject resources.
            ResourceProcessor.Inject(_lsnr, _marsh.Ignite);
            ResourceProcessor.Inject(_filter, _marsh.Ignite);

            try
            {
                // 2. Allocate handle.
                _hnd = _marsh.Ignite.HandleRegistry.Allocate(this);

                // 3. Call Java.
                _nativeQry = createTargetCb(writer =>
                {
                    writer.WriteLong(_hnd);
                    writer.WriteBoolean(qry.Local);
                    writer.WriteBoolean(_filter != null);

                    var javaFilter = _filter as PlatformJavaObjectFactoryProxy;

                    if (javaFilter != null)
                    {
                        writer.WriteObject(javaFilter.GetRawProxy());
                    }
                    else
                    {
                        var filterHolder = _filter == null || qry.Local
                            ? null
                            : new ContinuousQueryFilterHolder(_filter, _keepBinary);

                        writer.WriteObject(filterHolder);
                    }

                    writer.WriteInt(qry.BufferSize);
                    writer.WriteLong((long)qry.TimeInterval.TotalMilliseconds);
                    writer.WriteBoolean(qry.AutoUnsubscribe);

                    if (initialQry != null)
                    {
                        writer.WriteInt((int) initialQry.OpId);

                        initialQry.Write(writer, _keepBinary);
                    }
                    else
                        writer.WriteInt(-1); // no initial query
                });

                // 4. Initial query.
                _nativeInitialQueryCursor = _nativeQry.OutObjectInternal(0);
                _initialQueryIsFields = initialQry is SqlFieldsQuery;
            }
            catch (Exception)
            {
                if (_hnd > 0)
                    _marsh.Ignite.HandleRegistry.Release(_hnd);

                if (_nativeQry != null)
                    _nativeQry.Dispose();

                if (_nativeInitialQueryCursor != null)
                    _nativeInitialQueryCursor.Dispose();

                throw;
            }
        }

        /** <inheritdoc /> */
        public void Apply(IBinaryStream stream)
        {
            ICacheEntryEvent<TK, TV>[] evts = CQU.ReadEvents<TK, TV>(stream, _marsh, _keepBinary);

            _lsnr.OnEvent(evts);
        }

        /** <inheritdoc /> */
        public bool Evaluate(IBinaryStream stream)
        {
            Debug.Assert(_filter != null, "Evaluate should not be called if filter is not set.");

            ICacheEntryEvent<TK, TV> evt = CQU.ReadEvent<TK, TV>(stream, _marsh, _keepBinary);

            return _filter.Evaluate(evt);
        }

        /** <inheritdoc /> */
        public void Inject(Ignite grid)
        {
            throw new NotSupportedException("Should not be called.");
        }

        /** <inheritdoc /> */
        public long Allocate()
        {
            throw new NotSupportedException("Should not be called.");
        }

        /** <inheritdoc /> */
        public void Release()
        {
            _marsh.Ignite.HandleRegistry.Release(_hnd);
        }

        /** <inheritdoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> GetInitialQueryCursor()
        {
            if (_initialQueryIsFields)
            {
                throw new IgniteException("Initial query is SqlFieldsQuery");
            }

            return new QueryCursor<TK, TV>(GetInitialQueryCursorInternal(), _keepBinary);
        }

        /** <inheritdoc /> */
        IFieldsQueryCursor IContinuousQueryHandleFields.GetInitialQueryCursor()
        {
            if (!_initialQueryIsFields)
            {
                throw new IgniteException("Initial query is not SqlFieldsQuery");
            }

            return new FieldsQueryCursor(GetInitialQueryCursorInternal(), _keepBinary);
        }

        private IPlatformTargetInternal GetInitialQueryCursorInternal()
        {
            lock (this)
            {
                if (_disposed)
                    throw new ObjectDisposedException("Continuous query handle has been disposed.");

                var cur = _nativeInitialQueryCursor;

                if (cur == null)
                    throw new InvalidOperationException("GetInitialQueryCursor() can be called only once.");

                _nativeInitialQueryCursor = null;

                return cur;
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            lock (this)
            {
                if (_disposed || _nativeQry == null)
                    return;

                try
                {
                    _nativeQry.InLongOutLong(0, 0);
                }
                finally
                {
                    _nativeQry.Dispose();

                    _disposed = true;
                }
            }
        }
    }
}
