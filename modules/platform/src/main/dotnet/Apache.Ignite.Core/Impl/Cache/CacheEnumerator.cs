/*
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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Real cache enumerator communicating with Java.
    /// </summary>
    internal class CacheEnumerator<K, V> : GridDisposableTarget, IEnumerator<ICacheEntry<K, V>>
    {
        /** Operation: next value. */
        private const int OP_NEXT = 1;

        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Current entry. */
        private CacheEntry<K, V>? cur;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public CacheEnumerator(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable) : 
            base(target, marsh)
        {
            this.keepPortable = keepPortable;
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            ThrowIfDisposed();

            return DoInOp(OP_NEXT, stream =>
            {
                var reader = marsh.StartUnmarshal(stream, keepPortable);

                bool hasNext = reader.ReadBoolean();

                if (hasNext)
                {
                    reader.DetachNext();
                    K key = reader.ReadObject<K>();

                    reader.DetachNext();
                    V val = reader.ReadObject<V>();

                    cur = new CacheEntry<K, V>(key, val);

                    return true;
                }

                cur = null;

                return false;
            });
        }

        /** <inheritdoc /> */
        public ICacheEntry<K, V> Current
        {
            get
            {
                ThrowIfDisposed();

                if (cur == null)
                    throw new InvalidOperationException(
                        "Invalid enumerator state, enumeration is either finished or not started");

                return cur.Value;
            }
        }

        /** <inheritdoc /> */
        object IEnumerator.Current
        {
            get { return Current; }
        }

        /** <inheritdoc /> */
        public void Reset()
        {
            throw new NotSupportedException("Specified method is not supported.");
        }

        /** <inheritdoc /> */
        protected override T Unmarshal<T>(IPortableStream stream)
        {
            throw new InvalidOperationException("Should not be called.");
        }
    }
}
