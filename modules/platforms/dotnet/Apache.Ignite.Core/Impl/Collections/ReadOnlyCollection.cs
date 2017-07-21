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

namespace Apache.Ignite.Core.Impl.Collections
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Read-only wrapper over ICollection{T}.
    /// </summary>
    internal struct ReadOnlyCollection<T> : ICollection<T>
    {
        /** Wrapped collection. */
        private readonly ICollection<T> _col;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyCollection{T}"/> class.
        /// </summary>
        public ReadOnlyCollection(ICollection<T> col)
        {
            _col = col;
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            return _col.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _col).GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Add(T item)
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public bool Contains(T item)
        {
            return _col.Contains(item);
        }

        /** <inheritdoc /> */
        public void CopyTo(T[] array, int arrayIndex)
        {
            _col.CopyTo(array, arrayIndex);
        }

        /** <inheritdoc /> */
        public bool Remove(T item)
        {
            throw GetReadOnlyException();
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return _col.Count; }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly
        {
            get { return true; }
        }

        /// <summary>
        /// Gets the readonly exception.
        /// </summary>
        private static Exception GetReadOnlyException()
        {
            return new NotSupportedException("Collection is read-only.");
        }
    }
}