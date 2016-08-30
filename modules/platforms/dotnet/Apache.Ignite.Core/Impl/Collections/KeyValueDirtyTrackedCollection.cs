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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Binarizable key-value collection with dirty item tracking.
    /// </summary>
    public class KeyValueDirtyTrackedCollection : ICollection, IBinaryWriteAware  // TODO: Generic?
    {
        // TODO: Keep deserialized while not needed.
        private readonly Dictionary<object, Entry> _dict = new Dictionary<object, Entry>();

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValueDirtyTrackedCollection"/> class.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        internal KeyValueDirtyTrackedCollection(IBinaryRawReader binaryReader)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through 
        /// the collection.
        /// </returns>
        public IEnumerator GetEnumerator()
        {
            foreach (var entry in _dict)
                entry.Value.IsDirty = true;

            return _dict.Select(x => new DictionaryEntry(x.Key, x.Value.Value)).GetEnumerator();
        }

        /// <summary>
        /// Copies the elements of the <see cref="T:System.Collections.ICollection" /> 
        /// to an <see cref="T:System.Array" />, starting at a particular <see cref="T:System.Array" /> index.
        /// </summary>
        /// <param name="array">The one-dimensional <see cref="T:System.Array" /> that is the destination of 
        /// the elements copied from <see cref="T:System.Collections.ICollection" />. The <see cref="T:System.Array" /> 
        /// must have zero-based indexing.</param>
        /// <param name="index">The zero-based index in <paramref name="array" /> at which copying begins.</param>
        /// <exception cref="System.NotSupportedException"></exception>
        public void CopyTo(Array array, int index)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the number of elements contained in the <see cref="T:System.Collections.ICollection" />.
        /// </summary>
        public int Count
        {
            get { return _dict.Count; }
        }

        /// <summary>
        /// Gets an object that can be used to synchronize access 
        /// to the <see cref="T:System.Collections.ICollection" />.
        /// </summary>
        /// <exception cref="System.NotSupportedException"></exception>
        public object SyncRoot
        {
            get { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Gets a value indicating whether access to the <see cref="T:System.Collections.ICollection" /> 
        /// is synchronized (thread safe).
        /// </summary>
        public bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Gets or sets the value with the specified key.
        /// </summary>
        public object this[object key]
        {
            get
            {
                var entry = _dict[key];

                entry.IsDirty = true;

                return entry.Value;
            }
            set
            {
                Entry entry;

                if (!_dict.TryGetValue(key, out entry))
                    entry = new Entry();

                entry.IsDirty = true;

                entry.Value = key;
            }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            throw new NotImplementedException();
        }

        private class Entry
        {
            public object Value;
            public bool IsDirty;
        }
    }
}
