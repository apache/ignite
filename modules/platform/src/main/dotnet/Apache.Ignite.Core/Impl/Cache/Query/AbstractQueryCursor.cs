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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Abstract query cursor implementation.
    /// </summary>
    internal abstract class AbstractQueryCursor<T> : PlatformDisposableTarget, IQueryCursor<T>, IEnumerator<T>
    {
        /** */
        private const int OpGetAll = 1;

        /** */
        private const int OpGetBatch = 2;

        /** Position before head. */
        private const int BatchPosBeforeHead = -1;

        /** Keep portable flag. */
        private readonly bool _keepPortable;

        /** Wherther "GetAll" was called. */
        private bool _getAllCalled;

        /** Whether "GetEnumerator" was called. */
        private bool _iterCalled;

        /** Batch with entries. */
        private T[] _batch;

        /** Current position in batch. */
        private int _batchPos = BatchPosBeforeHead;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        protected AbstractQueryCursor(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable) : 
            base(target, marsh)
        {
            _keepPortable = keepPortable;
        }

        #region Public methods

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            ThrowIfDisposed();

            if (_iterCalled)
                throw new InvalidOperationException("Failed to get all entries because GetEnumerator() " + 
                    "method has already been called.");

            if (_getAllCalled)
                throw new InvalidOperationException("Failed to get all entries because GetAll() " + 
                    "method has already been called.");

            var res = DoInOp<IList<T>>(OpGetAll, ConvertGetAll);

            _getAllCalled = true;

            return res;
        }

        /** <inheritdoc /> */
        protected override void Dispose(bool disposing)
        {
            try
            {
                UU.QueryCursorClose(Target);
            }
            finally 
            {
                base.Dispose(disposing);
            }
        }

        #endregion

        #region Public IEnumerable methods

        /** <inheritdoc /> */
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        public IEnumerator<T> GetEnumerator()
        {
            ThrowIfDisposed();

            if (_iterCalled)
                throw new InvalidOperationException("Failed to get enumerator entries because " + 
                    "GetEnumeartor() method has already been called.");

            if (_getAllCalled)
                throw new InvalidOperationException("Failed to get enumerator entries because " + 
                    "GetAll() method has already been called.");

            UU.QueryCursorIterator(Target);

            _iterCalled = true;

            return this;
        }

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

                if (_batchPos == BatchPosBeforeHead)
                    throw new InvalidOperationException("MoveNext has not been called.");
                
                if (_batch == null)
                    throw new InvalidOperationException("Previous call to MoveNext returned false.");

                return _batch[_batchPos];
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

        /** <inheritdoc /> */
        public void Reset()
        {
            throw new NotSupportedException("Reset is not supported.");
        }

        #endregion

        #region Non-public methods

        /// <summary>
        /// Read entry from the reader.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Entry.</returns>
        protected abstract T Read(PortableReaderImpl reader);

        /** <inheritdoc /> */
        protected override T1 Unmarshal<T1>(IPortableStream stream)
        {
            return Marshaller.Unmarshal<T1>(stream, _keepPortable);
        }

        /// <summary>
        /// Request next batch.
        /// </summary>
        private void RequestBatch()
        {
            _batch = DoInOp<T[]>(OpGetBatch, ConvertGetBatch);

            _batchPos = 0;
        }

        /// <summary>
        /// Converter for GET_ALL operation.
        /// </summary>
        /// <param name="stream">Portable stream.</param>
        /// <returns>Result.</returns>
        private IList<T> ConvertGetAll(IPortableStream stream)
        {
            var reader = Marshaller.StartUnmarshal(stream, _keepPortable);

            var size = reader.ReadInt();

            var res = new List<T>(size);

            for (var i = 0; i < size; i++)
                res.Add(Read(reader));

            return res;
        }

        /// <summary>
        /// Converter for GET_BATCH operation.
        /// </summary>
        /// <param name="stream">Portable stream.</param>
        /// <returns>Result.</returns>
        private T[] ConvertGetBatch(IPortableStream stream)
        {
            var reader = Marshaller.StartUnmarshal(stream, _keepPortable);

            var size = reader.ReadInt();

            if (size == 0)
                return null;

            var res = new T[size];

            for (var i = 0; i < size; i++)
                res[i] = Read(reader);

            return res;
        }

        #endregion

    }
}
