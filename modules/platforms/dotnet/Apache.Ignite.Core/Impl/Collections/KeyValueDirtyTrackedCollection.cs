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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Binarizable key-value collection with dirty item tracking.
    /// </summary>
    public class KeyValueDirtyTrackedCollection : IBinaryWriteAware  // TODO: Generic?
    {
        // TODO: Keep deserialized while not needed.
        // TODO: Dedicated unit test
        private readonly Dictionary<object, Entry> _dict = new Dictionary<object, Entry>();
        private readonly List<Entry> _list = new List<Entry>();

        private bool _dirtyAll;

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValueDirtyTrackedCollection"/> class.
        /// </summary>
        /// <param name="binaryReader">The binary reader.</param>
        internal KeyValueDirtyTrackedCollection(IBinaryRawReader binaryReader)
        {
            Debug.Assert(binaryReader != null);

            var count = binaryReader.ReadInt();

            for (var i = 0; i < count; i++)
            {
                var key = binaryReader.ReadObject<object>();

                var entry = new Entry
                {
                    Value = binaryReader.ReadObject<object>()
                };

                _dict[key] = entry;

                _list.Add(entry);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValueDirtyTrackedCollection"/> class.
        /// </summary>
        public KeyValueDirtyTrackedCollection()
        {
            // No-op.
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
        /// Gets the number of elements contained in the <see cref="T:System.Collections.ICollection" />.
        /// </summary>
        public int Count
        {
            get { return _dict.Count; }
        }

        /// <summary>
        /// Gets or sets the value with the specified key.
        /// </summary>
        public object this[object key]
        {
            get
            {
                Entry entry;

                if (!_dict.TryGetValue(key, out entry))
                    return null;

                // TODO: Check for immutable type
                entry.IsDirty = true;

                return entry.Value;
            }
            set
            {
                Entry entry;

                if (!_dict.TryGetValue(key, out entry))
                {
                    entry = new Entry();
                    _dict[key] = entry;
                    _list.Add(entry);
                }

                entry.IsDirty = true;

                entry.Value = value;
            }
        }

        /// <summary>
        /// Gets or sets the value at the specified index.
        /// </summary>
        public object this[int index]
        {
            get
            {
                var entry = _list[index];

                // TODO: Check for immutable type
                entry.IsDirty = true;

                return entry.Value;
            }
            set
            {
                var entry = _list[index];

                entry.IsDirty = true;

                entry.Value = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is dirty.
        /// </summary>
        public bool IsDirty
        {
            get { return _dirtyAll || _dict.Values.Any(x => x.IsDirty); }
            set { _dirtyAll = value; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = writer.GetRawWriter();

            raw.WriteInt(_dict.Count);

            foreach (var entry in _dict)
            {
                raw.WriteObject(entry.Key);
                raw.WriteObject(entry.Value.Value);
            }
        }

        private class Entry
        {
            public object Value;
            public bool IsDirty = true;
        }
    }
}
