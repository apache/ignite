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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Real cache enumerator communicating with Java.
    /// </summary>
    internal class CacheEnumerator<TK, TV> : PlatformDisposableTarget, IEnumerator<ICacheEntry<TK, TV>>
    {
        /** Operation: next value. */
        private const int OpNext = 1;

        /** Keep portable flag. */
        private readonly bool _keepPortable;

        /** Current entry. */
        private CacheEntry<TK, TV>? _cur;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public CacheEnumerator(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable) : 
            base(target, marsh)
        {
            _keepPortable = keepPortable;
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            ThrowIfDisposed();

            return DoInOp(OpNext, stream =>
            {
                var reader = Marshaller.StartUnmarshal(stream, _keepPortable);

                bool hasNext = reader.ReadBoolean();

                if (hasNext)
                {
                    reader.DetachNext();
                    TK key = reader.ReadObject<TK>();

                    reader.DetachNext();
                    TV val = reader.ReadObject<TV>();

                    _cur = new CacheEntry<TK, TV>(key, val);

                    return true;
                }

                _cur = null;

                return false;
            });
        }

        /** <inheritdoc /> */
        public ICacheEntry<TK, TV> Current
        {
            get
            {
                ThrowIfDisposed();

                if (_cur == null)
                    throw new InvalidOperationException(
                        "Invalid enumerator state, enumeration is either finished or not started");

                return _cur.Value;
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
