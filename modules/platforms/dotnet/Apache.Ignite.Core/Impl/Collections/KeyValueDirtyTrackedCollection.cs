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
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Binarizable key-value collection with dirty item tracking.
    /// </summary>
    public class KeyValueDirtyTrackedCollection : IBinaryWriteAware
    {
        // TODO: Keep deserialized while not needed.
        // TODO: Dedicated unit test
        private readonly Dictionary<string, int> _dict = new Dictionary<string, int>();
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
                var key = binaryReader.ReadString();

                var entry = new Entry(key)
                {
                    Value = binaryReader.ReadObject<object>()
                };

                _dict[key] = _list.Count;

                _list.Add(entry);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValueDirtyTrackedCollection"/> class.
        /// </summary>
        public KeyValueDirtyTrackedCollection()
        {
            _dirtyAll = true;
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
            foreach (var entry in _list)
                SetDirtyOnRead(entry);

            return _dict.Select(x => new DictionaryEntry(x.Key, _list[x.Value])).GetEnumerator();
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
        public object this[string key]
        {
            get
            {
                var entry = GetEntry(key);

                if (entry == null)
                    return null;

                SetDirtyOnRead(entry);

                return entry.Value;
            }
            set
            {
                var entry = GetEntry(key);

                if (entry == null)
                {
                    entry = new Entry(key);
                    _dict[key] = _list.Count;
                    _list.Add(entry);
                }

                entry.IsDirty = true;

                entry.Value = value;
            }
        }

        private Entry GetEntry(string key)
        {
            int index;

            return !_dict.TryGetValue(key, out index) ? null : _list[index];
        }

        private int GetIndex(string key)
        {
            int index;

            return !_dict.TryGetValue(key, out index) ? -1 : index;
        }

        /// <summary>
        /// Gets or sets the value at the specified index.
        /// </summary>
        public object this[int index]
        {
            get
            {
                var entry = _list[index];

                SetDirtyOnRead(entry);

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
            get { return _dirtyAll || _list.Any(x => x.IsDirty); }
            set { _dirtyAll = value; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            var wr = (BinaryWriter) writer;

            wr.WriteInt(_dict.Count);

            foreach (var entry in _list)
            {
                wr.WriteString(entry.Key);

                // ReSharper disable once AccessToForEachVariableInClosure
                wr.WithDetach(w => w.WriteObject(entry.Value));
            }
        }

        private static void SetDirtyOnRead(Entry entry)
        {
            var type = entry.Value.GetType();

            if (IsImmutable(type))
                return;

            entry.IsDirty = true;
        }

        private static bool IsImmutable(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;  // Unwrap nullable.

            if (type.IsPrimitive)
                return true;

            if (type == typeof(string) || type == typeof(DateTime) || type == typeof(Guid) || type == typeof(decimal))
                return true;

            return false;
        }

        /// <summary>
        /// Removes the specified key.
        /// </summary>
        public void Remove(string key)
        {
            var index = GetIndex(key);

            if (index < 0)
                return;

            _dict.Remove(key);
            _list.RemoveAt(index);
        }

        /// <summary>
        /// Removes at specified index.
        /// </summary>
        public void RemoveAt(int index)
        {
            var entry = _list[index];

            _list.RemoveAt(index);
            _dict.Remove(entry.Key);
        }

        /// <summary>
        /// Clears this instance.
        /// </summary>
        public void Clear()
        {
            _list.Clear();
            _dict.Clear();

            _dirtyAll = true;
        }

        private class Entry
        {
            public object Value;
            public bool IsDirty;
            public readonly string Key;

            public Entry(string key)
            {
                Key = key;
            }
        }
    }
}
