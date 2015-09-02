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

namespace Apache.Ignite.Core.Impl.Common
{
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Simple wrapper over result to handle marshalling properly.
    /// </summary>
    internal class PortableResultWrapper : IPortableWriteAware
    {
        /** */
        private readonly object _result;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableResultWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableResultWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            _result = PortableUtils.ReadPortableOrSerializable<object>(reader0);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="res">Result.</param>
        public PortableResultWrapper(object res)
        {
            _result = res;
        }

        /// <summary>
        /// Result.
        /// </summary>
        public object Result
        {
            get { return _result; }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, Result);
        }
    }
}
