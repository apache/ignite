/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Real cache enumerator communicating with Java.
    /// </summary>
    internal class CacheEnumerator<TK, TV> : PlatformDisposableTargetAdapter, IEnumerator<ICacheEntry<TK, TV>>
    {
        /** Operation: next value. */
        private const int OpNext = 1;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Current entry. */
        private CacheEntry<TK, TV>? _cur;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public CacheEnumerator(IPlatformTargetInternal target, bool keepBinary) : base(target)
        {
            _keepBinary = keepBinary;
        }

        /** <inheritdoc /> */
        public bool MoveNext()
        {
            ThrowIfDisposed();

            return DoInOp(OpNext, stream =>
            {
                var reader = Marshaller.StartUnmarshal(stream, _keepBinary);

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
        protected override T Unmarshal<T>(IBinaryStream stream)
        {
            throw new InvalidOperationException("Should not be called.");
        }
    }
}
