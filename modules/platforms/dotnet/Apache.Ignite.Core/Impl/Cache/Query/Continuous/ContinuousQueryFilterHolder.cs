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

namespace Apache.Ignite.Core.Impl.Cache.Query.Continuous
{
    using System;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Continuous query remote filter holder. Wraps real filter into portable object,
    /// so that it can be passed over wire to another node.
    /// </summary>
    public class ContinuousQueryFilterHolder : IPortableWriteAware
    {
        /** Key type. */
        private readonly Type _keyTyp;

        /** Value type. */
        private readonly Type _valTyp;

        /** Filter object. */
        private readonly object _filter;

        /** Keep portable flag. */
        private readonly bool _keepPortable;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="keyTyp">Key type.</param>
        /// <param name="valTyp">Value type.</param>
        /// <param name="filter">Filter.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public ContinuousQueryFilterHolder(Type keyTyp, Type valTyp, object filter, bool keepPortable)
        {
            _keyTyp = keyTyp;
            _valTyp = valTyp;
            _filter = filter;
            _keepPortable = keepPortable;
        }

        /// <summary>
        /// Key type.
        /// </summary>
        internal Type KeyType
        {
            get { return _keyTyp; }
        }

        /// <summary>
        /// Value type.
        /// </summary>
        internal Type ValueType
        {
            get { return _valTyp; }
        }

        /// <summary>
        /// Filter.
        /// </summary>
        internal object Filter
        {
            get { return _filter; }
        }

        /// <summary>
        /// Keep portable flag.
        /// </summary>
        internal bool KeepPortable
        {
            get { return _keepPortable; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WritePortable(IPortableWriter writer)
        {
            PortableWriterImpl rawWriter = (PortableWriterImpl) writer.RawWriter();

            PortableUtils.WritePortableOrSerializable(rawWriter, _keyTyp);
            PortableUtils.WritePortableOrSerializable(rawWriter, _valTyp);
            PortableUtils.WritePortableOrSerializable(rawWriter, _filter);

            rawWriter.WriteBoolean(_keepPortable);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ContinuousQueryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ContinuousQueryFilterHolder(IPortableReader reader)
        {
            PortableReaderImpl rawReader = (PortableReaderImpl) reader.RawReader();

            _keyTyp = PortableUtils.ReadPortableOrSerializable<Type>(rawReader);
            _valTyp = PortableUtils.ReadPortableOrSerializable<Type>(rawReader);
            _filter = PortableUtils.ReadPortableOrSerializable<object>(rawReader);
            _keepPortable = rawReader.ReadBoolean();
        }
    }
}
