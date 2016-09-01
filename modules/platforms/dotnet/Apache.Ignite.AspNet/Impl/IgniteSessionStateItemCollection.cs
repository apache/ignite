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

    /// <summary>
    /// Wrapper for <see cref="KeyValueDirtyTrackedCollection" />.
    /// </summary>
    internal class IgniteSessionStateItemCollection : ISessionStateItemCollection
    {
        // TODO: Dedicated test!

            
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
        public void CopyTo(Array array, int index)
        {
            throw new NotImplementedException();
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
        object ISessionStateItemCollection.this[string name]
        {
            get { return _collection[name]; }
            set { _collection[name] = value; }
        }

        /** <inheritdoc /> */
        object ISessionStateItemCollection.this[int index]
        {
            get { return _collection[index]; }
            set { _collection[index] = value; }
        }

        /** <inheritdoc /> */
        public NameObjectCollectionBase.KeysCollection Keys
        {
            get
            {
                // KeysCollection ctor is internal, not possible to return it.
                throw new NotSupportedException("Use GetEnumerator instead to get all entries.");
            }
        }

        /** <inheritdoc /> */
        public bool Dirty
        {
            get { return _collection.IsDirty; }
            set { _collection.IsDirty = value; }
        }
    }
}