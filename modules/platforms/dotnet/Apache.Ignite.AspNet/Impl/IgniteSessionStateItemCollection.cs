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

namespace Apache.Ignite.AspNet.Impl
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Web.SessionState;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binarizable key-value collection with dirty item tracking.
    /// </summary>
    internal class IgniteSessionStateItemCollection : ISessionStateItemCollection
    {
        /** */
        private readonly Dictionary<string, int> _dict;

        /** */
        private readonly List<Entry> _list;

        /** Indicates where this is a new collection, not a deserialized old one. */
        private readonly bool _isNew;

        /** Removed keys. Hash set because keys can be removed multiple times. */
        private HashSet<string> _removedKeys;

        /** Indicates that entire collection is dirty and can't be written as a diff. */
        private bool _dirtyAll;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateItemCollection"/> class.
        /// </summary>
        /// <param name="reader">The binary reader.</param>
        internal IgniteSessionStateItemCollection(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            var count = reader.ReadInt();

            _dict = new Dictionary<string, int>(count);
            _list = new List<Entry>(count);

            for (var i = 0; i < count; i++)
            {
                var key = reader.ReadString();

                var valBytes = reader.ReadByteArray();

                if (valBytes != null)
                {
                    var entry = new Entry(key, true, valBytes);

                    _dict[key] = _list.Count;

                    _list.Add(entry);
                }
                else
                    AddRemovedKey(key);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateItemCollection"/> class.
        /// </summary>
        public IgniteSessionStateItemCollection()
        {
            _dict = new Dictionary<string, int>();
            _list = new List<Entry>();
            _isNew = true;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0", 
            Justification = "Validation is present.")]
        public void CopyTo(Array array, int index)
        {
            IgniteArgumentCheck.NotNull(array, "array");
            IgniteArgumentCheck.Ensure(Count + index < array.Length, "array",
                "The number of elements in the source collection is greater than the available space " +
                "from specified index to the end of the array.");

            // This should return only keys.
            foreach (var entry in _list)
                array.SetValue(entry.Key, index++);
        }

        /** <inheritdoc /> */
        public IEnumerator GetEnumerator()
        {
            // This should return only keys.
            return _list.Select(x => x.Key).GetEnumerator();
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return _dict.Count; }
        }

        /** <inheritdoc /> */
        public object SyncRoot
        {
            get { return _list; }
        }

        /** <inheritdoc /> */
        public bool IsSynchronized
        {
            get { return false; }
        }

        /** <inheritdoc /> */
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
                var entry = GetOrCreateDirtyEntry(key);

                entry.Value = value;
            }
        }

        /** <inheritdoc /> */
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

        /** <inheritdoc /> */
        public NameObjectCollectionBase.KeysCollection Keys
        {
            get { return new NameObjectCollection(this).Keys; }
        }


        /** <inheritdoc /> */
        public bool Dirty
        {
            get { return _dirtyAll || _list.Any(x => x.IsDirty); }
            set { _dirtyAll = value; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        public void WriteBinary(IBinaryRawWriter writer, bool changesOnly)
        {
            IgniteArgumentCheck.NotNull(writer, "writer");

            if (_isNew || _dirtyAll || !changesOnly || (_removedKeys == null && _list.All(x => x.IsDirty)))
            {
                // Write in full mode.
                writer.WriteInt(_list.Count);

                foreach (var entry in _list)
                {
                    writer.WriteString(entry.Key);

                    // Write as byte array to enable partial deserialization.
                    writer.WriteByteArray(entry.GetBytes());
                }
            }
            else
            {
                // Write in diff mode.
                var removed = GetRemovedKeys();

                var count = _list.Count(x => x.IsDirty) + (removed == null ? 0 : removed.Count);

                writer.WriteInt(count);  // reserve count

                // Write removed keys as [key + null].
                if (removed != null)
                {
                    foreach (var removedKey in removed)
                    {
                        writer.WriteString(removedKey);
                        writer.WriteByteArray(null);
                    }
                }

                // Write dirty items.
                foreach (var entry in _list)
                {
                    if (!entry.IsDirty)
                        continue;

                    writer.WriteString(entry.Key);

                    // Write as byte array to enable partial deserialization.
                    writer.WriteByteArray(entry.GetBytes());
                }
            }
        }

        /// <summary>
        /// Gets the removed keys.
        /// </summary>
        private ICollection<string> GetRemovedKeys()
        {
            if (_removedKeys == null)
                return null;

            // Filter out existing keys.
            var removed = new HashSet<string>(_removedKeys);

            foreach (var entry in _list)
                removed.Remove(entry.Key);

            return removed;
        }

        /// <summary>
        /// Removes the specified key.
        /// </summary>
        public void Remove(string key)
        {
            var index = GetIndex(key);

            if (index < 0)
                return;

            var entry = _list[index];
            Debug.Assert(key == entry.Key);

            _list.RemoveAt(index);
            _dict.Remove(key);

            // Update all indexes.
            for (var i = 0; i < _list.Count; i++)
                _dict[_list[i].Key] = i;

            if (entry.IsInitial)
                AddRemovedKey(key);
        }

        /// <summary>
        /// Removes at specified index.
        /// </summary>
        public void RemoveAt(int index)
        {
            var entry = _list[index];

            _list.RemoveAt(index);
            _dict.Remove(entry.Key);

            if (entry.IsInitial)
                AddRemovedKey(entry.Key);
        }

        /// <summary>
        /// Clears this instance.
        /// </summary>
        public void Clear()
        {
            foreach (var entry in _list)
            {
                if (entry.IsInitial)
                    AddRemovedKey(entry.Key);
            }

            _list.Clear();
            _dict.Clear();

            _dirtyAll = true;
        }

        /// <summary>
        /// Applies the changes.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        public void ApplyChanges(IgniteSessionStateItemCollection changes)
        {
            var removed = changes._removedKeys;

            if (removed != null)
            {
                foreach (var key in removed)
                    Remove(key);
            }
            else
            {
                // Not a diff: replace all.
                Clear();
            }

            foreach (var changedEntry in changes._list)
            {
                var entry = GetOrCreateDirtyEntry(changedEntry.Key);

                // Copy without deserialization.
                changedEntry.CopyTo(entry);
            }
        }

        /// <summary>
        /// Adds the removed key.
        /// </summary>
        private void AddRemovedKey(string key)
        {
            Debug.Assert(!_isNew);

            if (_removedKeys == null)
                _removedKeys = new HashSet<string>();

            _removedKeys.Add(key);
        }

        /// <summary>
        /// Gets or creates an entry.
        /// </summary>
        private Entry GetOrCreateDirtyEntry(string key)
        {
            var entry = GetEntry(key);

            if (entry == null)
            {
                entry = new Entry(key, false, null);

                _dict[key] = _list.Count;
                _list.Add(entry);
            }

            entry.IsDirty = true;

            return entry;
        }

        /// <summary>
        /// Gets the entry.
        /// </summary>
        private Entry GetEntry(string key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            int index;

            return !_dict.TryGetValue(key, out index) ? null : _list[index];
        }

        /// <summary>
        /// Gets the index.
        /// </summary>
        private int GetIndex(string key)
        {
            int index;

            return !_dict.TryGetValue(key, out index) ? -1 : index;
        }

        /// <summary>
        /// Sets the dirty on read.
        /// </summary>
        private static void SetDirtyOnRead(Entry entry)
        {
            var type = entry.Value.GetType();

            if (IsImmutable(type))
                return;

            entry.IsDirty = true;
        }

        /// <summary>
        /// Determines whether the specified type is immutable.
        /// </summary>
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
        /// Inner entry.
        /// </summary>
        private class Entry
        {
            /** */
            public readonly bool IsInitial;

            /** */
            public readonly string Key;

            /** */
            public bool IsDirty;

            /** */
            private object _value;

            /** */
            private bool _isDeserialized;

            /// <summary>
            /// Initializes a new instance of the <see cref="Entry"/> class.
            /// </summary>
            public Entry(string key, bool isInitial, object value)
            {
                Debug.Assert(key != null);

                Key = key;
                IsInitial = isInitial;
                _isDeserialized = !isInitial;
                _value = value;
            }

            /// <summary>
            /// Gets or sets the value.
            /// </summary>
            public object Value
            {
                get
                {
                    if (!_isDeserialized)
                    {
                        using (var stream = new MemoryStream((byte[])_value))
                        {
                            _value = new BinaryFormatter().Deserialize(stream);
                        }

                        _isDeserialized = true;
                    }

                    return _value;
                }
                set
                {
                    _value = value;
                    _isDeserialized = true;
                }
            }

            /// <summary>
            /// Copies contents to another entry.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
            public void CopyTo(Entry entry)
            {
                Debug.Assert(entry != null);

                entry._isDeserialized = _isDeserialized;
                entry._value = _value;
            }

            /// <summary>
            /// Gets the bytes.
            /// </summary>
            public byte[] GetBytes()
            {
                if (!_isDeserialized)
                    return (byte[]) _value;

                using (var stream = new MemoryStream())
                {
                    new BinaryFormatter().Serialize(stream, _value);

                    return stream.ToArray();
                }
            }
        }

        /// <summary>
        /// NameObjectCollectionBase.KeysCollection has internal constructor.
        /// The only way to implement ISessionStateItemCollection.Keys property 
        /// is to have a NameObjectCollectionBase in hand.
        /// </summary>
        private class NameObjectCollection : NameObjectCollectionBase
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="NameObjectCollection"/> class.
            /// </summary>
            public NameObjectCollection(IEnumerable keys)
            {
                foreach (string key in keys)
                    BaseAdd(key, null);
            }
        }
    }
}
