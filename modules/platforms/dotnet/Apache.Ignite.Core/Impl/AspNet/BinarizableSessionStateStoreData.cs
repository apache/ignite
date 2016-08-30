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

namespace Apache.Ignite.Core.Impl.AspNet
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Collections;

    /// <summary>
    /// Binarizable SessionStateStoreData. 
    /// Does not override System.Web.SessionState.SessionStateStoreData to avoid dependency on System.Web.
    /// </summary>
    public class BinarizableSessionStateStoreData : IBinaryWriteAware
    {
        private readonly KeyValueDirtyTrackedCollection _items;

        private readonly byte[] _staticObjects;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarizableSessionStateStoreData"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal BinarizableSessionStateStoreData(IBinaryRawReader reader)
        {
            _items = new KeyValueDirtyTrackedCollection(reader);
            _staticObjects = reader.ReadByteArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarizableSessionStateStoreData"/> class.
        /// </summary>
        public BinarizableSessionStateStoreData()
        {
            _items = new KeyValueDirtyTrackedCollection();
            _staticObjects = null;
        }

        /// <summary>
        /// Gets the items.
        /// </summary>
        public KeyValueDirtyTrackedCollection Items
        {
            get { return _items; }
        }

        /// <summary>
        /// Gets the static objects.
        /// </summary>
        public byte[] StaticObjects
        {
            get { return _staticObjects; }
        }

        /// <summary>
        /// Gets or sets the timeout.
        /// </summary>
        public int Timeout { get; set; }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WriteBinary(IBinaryWriter writer)
        {
            var raw = (IBinaryRawWriter) writer;

            raw.WriteInt(Timeout);
            raw.WriteObject(Items);
            raw.WriteByteArray(StaticObjects);
        }
    }
}
