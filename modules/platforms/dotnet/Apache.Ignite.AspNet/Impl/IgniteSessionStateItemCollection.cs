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
    using System.Collections.Specialized;
    using System.Diagnostics;
    using System.Web.SessionState;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Wrapper for <see cref="KeyValueDirtyTrackedCollection" />.
    /// </summary>
    internal class IgniteSessionStateItemCollection : ISessionStateItemCollection
    {
        /** Wrapped collection */
        private readonly KeyValueDirtyTrackedCollection _collection;

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteSessionStateItemCollection"/> class.
        /// </summary>
        /// <param name="collection">The collection.</param>
        public IgniteSessionStateItemCollection(KeyValueDirtyTrackedCollection collection)
        {
            Debug.Assert(collection != null);

            _collection = collection;
        }

        /** <inheritdoc /> */
        public IEnumerator GetEnumerator()
        {
            // This should return only keys.
            return _collection.GetKeys().GetEnumerator();
        }

        /** <inheritdoc /> */
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", 
            "CA1062:Validate arguments of public methods", MessageId = "0", Justification = "Validated.")]
        public void CopyTo(Array array, int index)
        {
            IgniteArgumentCheck.NotNull(array, "array");
            IgniteArgumentCheck.Ensure(Count + index < array.Length, "array",
                "The number of elements in the source collection is greater than the available space " +
                "from specified index to the end of the array.");

            foreach (var key in _collection.GetKeys())
                array.SetValue(key, index++);
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return _collection.Count; }
        }

        /** <inheritdoc /> */
        public object SyncRoot
        {
            get { return _collection; }
        }

        /** <inheritdoc /> */
        public bool IsSynchronized
        {
            get { return false; }
        }

        /** <inheritdoc /> */
        public void Remove(string name)
        {
            _collection.Remove(name);
        }

        /** <inheritdoc /> */
        public void RemoveAt(int index)
        {
            _collection.RemoveAt(index);
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            _collection.Clear();
        }

        /** <inheritdoc /> */
        public object this[string name]
        {
            get { return _collection[name]; }
            set { _collection[name] = value; }
        }

        /** <inheritdoc /> */
        public object this[int index]
        {
            get { return _collection[index]; }
            set { _collection[index] = value; }
        }

        /** <inheritdoc /> */
        public NameObjectCollectionBase.KeysCollection Keys
        {
            get { return new NameObjectCollection(this).Keys; }
        }

        /** <inheritdoc /> */
        public bool Dirty
        {
            get { return _collection.IsDirty; }
            set { _collection.IsDirty = value; }
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