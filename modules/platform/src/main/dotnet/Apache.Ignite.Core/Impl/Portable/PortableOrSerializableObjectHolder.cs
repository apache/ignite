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
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Wraps portable/serializable item in a portable.
    /// </summary>
    internal class PortableOrSerializableObjectHolder : IPortableWriteAware
    {
        /** */
        private readonly object _item;

        /// <summary>
        /// Initializes a new instance of the <see cref="SerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="item">The item to wrap.</param>
        public PortableOrSerializableObjectHolder(object item)
        {
            _item = item;
        }

        /// <summary>
        /// Gets or sets the item to wrap.
        /// </summary>
        public object Item
        {
            get { return _item; }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();

            PortableUtils.WritePortableOrSerializable(writer0, Item);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableOrSerializableObjectHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableOrSerializableObjectHolder(IPortableReader reader)
        {
            _item = PortableUtils.ReadPortableOrSerializable<object>((PortableReaderImpl)reader.RawReader());
        }
    }
}