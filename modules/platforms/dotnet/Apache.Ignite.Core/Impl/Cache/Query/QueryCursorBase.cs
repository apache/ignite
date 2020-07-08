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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Abstract query cursor implementation.
    /// </summary>
    internal abstract class QueryCursorBase<T> : IQueryCursor<T>, IEnumerator<T>
    {
        /** Position before head. */
        private const int BatchPosBeforeHead = -1;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Read func. */
        private readonly Func<BinaryReader, T> _readFunc;
        
        /** Lock object. */
        private readonly object _syncRoot = new object();

        /** Whether "GetAll" was called. */
        private bool _getAllCalled;

        /** Whether "GetEnumerator" was called. */
        private bool _iterCalled;

        /** Batch with entries. */
        private T[] _batch;

        /** Current position in batch. */
        private int _batchPos = BatchPosBeforeHead;

        /** Disposed flag. */
        private volatile bool _disposed;

        /** Whether next batch is available. */
        private bool _hasNext = true;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="readFunc">The read function.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        protected QueryCursorBase(Marshaller marsh, bool keepBinary, Func<BinaryReader, T> readFunc, 
            IBinaryStream initialBatchStream = null)
        {
            Debug.Assert(marsh != null);

            _keepBinary = keepBinary;
            _readFunc = readFunc;
            _marsh = marsh;

            if (initialBatchStream != null)
            {
                _batch = ConvertGetBatch(initialBatchStream);
            }
        }

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            if (_getAllCalled)
                throw new InvalidOperationException("Failed to get all entries because GetAll() " +
                                                    "method has already been called.");

            if (_iterCalled)
                throw new InvalidOperationException("Failed to get all entries because GetEnumerator() " +
                                                    "method has already been called.");

            lock (_syncRoot)
            {
                ThrowIfDisposed();

                var res = GetAllInternal();

                _getAllCalled = true;
                _hasNext = false;

                return res;
            }
        }

        #region Public IEnumerable methods

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            if (_getAllCalled)
            {
                throw new InvalidOperationException("Failed to get enumerator entries because " +
                                                    "GetAll() method has already been called.");
            }

            if (_iterCalled)
            {
                throw new InvalidOperationException("Failed to get enumerator entries because " +
                                                    "GetEnumerator() method has already been called.");
            }

            ThrowIfDisposed();

            InitIterator();

            _iterCalled = true;

            return this;
        }

        protected abstract void InitIterator();

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Public IEnumerator methods

        /** <inheritdoc /> */
        public T Current
        {
            get
            {
                ThrowIfDisposed();

                lock (_syncRoot)
                {
                    if (_batchPos == BatchPosBeforeHead)
                        throw new InvalidOperationException("MoveNext has not been called.");

                    if (_batch == null)
                        throw new InvalidOperationException("Previous call to MoveNext returned false.");

                    return _batch[_batchPos];
                }
            }
        }

        /** <inheritdoc /> */
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            ThrowIfDisposed();

            lock (_syncRoot)
            {
                if (_batch == null)
                {
                    if (_batchPos == BatchPosBeforeHead)
                        // Standing before head, let's get batch and advance position.
                        RequestBatch();
                }
                else
                {
                    _batchPos++;

                    if (_batch.Length == _batchPos)
                        // Reached batch end => request another.
                        RequestBatch();
                }

                return _batch != null;
            }
        }

        /** <inheritdoc /> */
        public void Reset()
        {
            throw new NotSupportedException("Reset is not supported.");
        }

        #endregion

        /// <summary>
        /// Gets all entries.
        /// </summary>
        protected abstract IList<T> GetAllInternal();

        /// <summary>
        /// Requests next batch.
        /// </summary>
        private void RequestBatch()
        {
            lock (_syncRoot)
            {
                ThrowIfDisposed();
                
                _batch = _hasNext ? GetBatch() : null;

                _batchPos = 0;
            }
        }

        /// <summary>
        /// Gets the next batch.
        /// </summary>
        protected abstract T[] GetBatch();

        /// <summary>
        /// Converter for GET_ALL operation.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        protected IList<T> ConvertGetAll(IBinaryStream stream)
        {
            var reader = _marsh.StartUnmarshal(stream, _keepBinary);

            var size = reader.ReadInt();

            var res = new List<T>(size);

            for (var i = 0; i < size; i++)
                res.Add(_readFunc(reader));

            return res;
        }

        /// <summary>
        /// Converter for GET_BATCH operation.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>Result.</returns>
        protected T[] ConvertGetBatch(IBinaryStream stream)
        {
            var reader = _marsh.StartUnmarshal(stream, _keepBinary);

            var size = reader.ReadInt();

            lock (_syncRoot)
            {
                if (size == 0)
                {
                    _hasNext = false;
                    return null;
                }

                var res = new T[size];

                for (var i = 0; i < size; i++)
                {
                    res[i] = _readFunc(reader);
                }

                _hasNext = stream.ReadBool();

                return res;
            }
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            lock (_syncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                if (_hasNext)
                {
                    Dispose(true);
                }

                GC.SuppressFinalize(this);

                _disposed = true;
            }
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose;  <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            // No-op.
        }

        /// <summary>
        /// Throws <see cref="ObjectDisposedException"/> if this instance has been disposed.
        /// </summary>
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().Name, "Object has been disposed.");
            }
        }
    }
}
