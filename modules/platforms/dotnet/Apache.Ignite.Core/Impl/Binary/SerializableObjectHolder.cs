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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Diagnostics;
    using System.Runtime.Serialization.Formatters.Binary;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Wraps Serializable item in a binarizable.
    /// </summary>
    internal class SerializableObjectHolder : IBinaryWriteAware
    {
        /** */
        private readonly object _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="item">The item to wrap.</param>
        public SerializableObjectHolder(object item)
        {
            _item = item;
        }

        /// <summary>
        /// Gets the item to wrap.
        /// </summary>
        public object Item
        {
            get { return _item; }
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            Debug.Assert(writer != null);

            var writer0 = (BinaryWriter)writer.GetRawWriter();

            writer0.WithDetach(w =>
            {
                using (var streamAdapter = new BinaryStreamAdapter(w.Stream))
                {
                    new BinaryFormatter().Serialize(streamAdapter, Item);
                }
            });
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public SerializableObjectHolder(BinaryReader reader)
        {
            Debug.Assert(reader != null);

            using (var streamAdapter = new BinaryStreamAdapter(reader.Stream))
            {
                _item = new BinaryFormatter().Deserialize(streamAdapter, null);
            }
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;

            return Equals(_item, ((SerializableObjectHolder) obj)._item);
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return _item != null ? _item.GetHashCode() : 0;
        }
    }
}