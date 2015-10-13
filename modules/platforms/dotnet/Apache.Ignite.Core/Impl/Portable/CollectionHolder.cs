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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Wrapper for .Net-specific collections.
    /// Can be interpreted as a Portable Object in Java, but not as a collection.
    /// </summary>
    internal class CollectionHolder : IPortableWriteAware
    {
        /** Collection. */
        private readonly object _collection;

        /** Write action. */
        private readonly Action<PortableWriterImpl, object> _writeAction;

        /// <summary>
        /// Initializes a new instance of the <see cref="CollectionHolder" /> class.
        /// </summary>
        public CollectionHolder(object collection, Action<PortableWriterImpl, object> writeAction)
        {
            Debug.Assert(collection != null);
            Debug.Assert(writeAction != null);

            _collection = collection;
            _writeAction = writeAction;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CollectionHolder"/> class.
        /// </summary>
        public CollectionHolder(IPortableReader reader)
        {
            _collection = PortableUtils.ReadGenericCollectionAsObject((PortableReaderImpl) reader.GetRawReader());
        }

        /// <summary>
        /// Gets the wrapped collection.
        /// </summary>
        public object Collection
        {
            get { return _collection; }
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            Debug.Assert(_writeAction != null);

            _writeAction((PortableWriterImpl) writer.GetRawWriter(), _collection);
        }
    }
}
