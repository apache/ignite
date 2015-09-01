/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Query
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Cache.Query;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;
    
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Abstract query cursor implementation.
    /// </summary>
    internal abstract class AbstractQueryCursor<T> : GridDisposableTarget, IQueryCursor<T>, IEnumerator<T>
    {
        /** */
        private const int OP_GET_ALL = 1;

        /** */
        private const int OP_GET_BATCH = 2;

        /** Position before head. */
        private const int BATCH_POS_BEFORE_HEAD = -1;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Wherther "GetAll" was called. */
        private bool getAllCalled;

        /** Whether "GetEnumerator" was called. */
        private bool iterCalled;

        /** Batch with entries. */
        private T[] batch;

        /** Current position in batch. */
        private int batchPos = BATCH_POS_BEFORE_HEAD;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        protected AbstractQueryCursor(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable) : 
            base(target, marsh)
        {
            this.keepPortable = keepPortable;
        }

        #region Public methods

        /** <inheritdoc /> */
        public IList<T> GetAll()
        {
            ThrowIfDisposed();

            if (iterCalled)
                throw new InvalidOperationException("Failed to get all entries because GetEnumerator() " + 
                    "method has already been called.");

            if (getAllCalled)
                throw new InvalidOperationException("Failed to get all entries because GetAll() " + 
                    "method has already been called.");

            var res = DoInOp<IList<T>>(OP_GET_ALL, ConvertGetAll);

            getAllCalled = true;

            return res;
        }

        /** <inheritdoc /> */
        protected override void Dispose(bool disposing)
        {
            try
            {
                UU.QueryCursorClose(target);
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

            if (iterCalled)
                throw new InvalidOperationException("Failed to get enumerator entries because " + 
                    "GetEnumeartor() method has already been called.");

            if (getAllCalled)
                throw new InvalidOperationException("Failed to get enumerator entries because " + 
                    "GetAll() method has already been called.");

            UU.QueryCursorIterator(target);

            iterCalled = true;

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

                if (batchPos == BATCH_POS_BEFORE_HEAD)
                    throw new InvalidOperationException("MoveNext has not been called.");
                
                if (batch == null)
                    throw new InvalidOperationException("Previous call to MoveNext returned false.");

                return batch[batchPos];
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

            if (batch == null)
            {
                if (batchPos == BATCH_POS_BEFORE_HEAD)
                    // Standing before head, let's get batch and advance position.
                    RequestBatch();
            }
            else
            {
                batchPos++;

                if (batch.Length == batchPos)
                    // Reached batch end => request another.
                    RequestBatch();
            }

            return batch != null;
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
            return marsh.Unmarshal<T1>(stream, keepPortable);
        }

        /// <summary>
        /// Request next batch.
        /// </summary>
        private void RequestBatch()
        {
            batch = DoInOp<T[]>(OP_GET_BATCH, ConvertGetBatch);

            batchPos = 0;
        }

        /// <summary>
        /// Converter for GET_ALL operation.
        /// </summary>
        /// <param name="stream">Portable stream.</param>
        /// <returns>Result.</returns>
        private IList<T> ConvertGetAll(IPortableStream stream)
        {
            var reader = marsh.StartUnmarshal(stream, keepPortable);

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
            var reader = marsh.StartUnmarshal(stream, keepPortable);

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
